# encoding: utf-8
# require "logstash/inputs/base"
require "logstash/inputs/threadable"
require "logstash/namespace"
require "logstash/timestamp"
require "logstash/plugin_mixins/aws_config"
require "logstash/errors"
require "time"
require "tmpdir"
require "stud/interval"
require "stud/temporary"


class LogStash::Inputs::S3File_Via_Sqs < LogStash::Inputs::Threadable
  include LogStash::PluginMixins::AwsConfig::V2
  MAX_TIME_BEFORE_GIVING_UP = 60
  MAX_MESSAGES_TO_FETCH = 10 # Between 1-10 in the AWS-SDK doc
  SENT_TIMESTAMP = "SentTimestamp"
  SQS_ATTRIBUTES = [SENT_TIMESTAMP]
  BACKOFF_SLEEP_TIME = 1
  BACKOFF_FACTOR = 2
  DEFAULT_POLLING_FREQUENCY = 20

  config_name "s3file_via_sqs"

  default :codec, "plain"

  # Name of the event field in which to store the SQS message Sent Timestamp
  config :sent_timestamp_field, :validate => :string

  # Polling frequency, default is 20 seconds
  config :polling_frequency, :validate => :number, :default => DEFAULT_POLLING_FREQUENCY

  # Assume Role arn (cross accounts access)
  config :assume_role_arn, :validate => :string

  # The AWS account number for the SQS queue (get queue url)
  config :queue, :validate => :string, :required => true

  # The AWS account number for the SQS queue (get queue url)
  config :aws_queue_owner_id, :validate => :string

  # The AWS Region
  config :region, :validate =>:string, :default => LogStash::PluginMixins::AwsConfig::US_EAST_1

  # This plugin uses the AWS SDK and supports several ways to get credentials, which will be tried in this order...
  # 1. Static configuration, using `access_key_id` and `secret_access_key` params in logstash plugin config
  # 2. External credentials file specified by `aws_credentials_file`
  # 3. Environment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
  # 4. Environment variables `AMAZON_ACCESS_KEY_ID` and `AMAZON_SECRET_ACCESS_KEY`
  # 5. IAM Instance Profile (available when running inside EC2)
  config :access_key_id, :validate => :string

  # The AWS Secret Access Key
  config :secret_access_key, :validate => :string

  # Path to YAML file containing a hash of AWS credentials.
  # This file will only be loaded if `access_key_id` and
  # `secret_access_key` aren't set. The contents of the
  # file should look like this:
  #
  #     :access_key_id: "12345"
  #     :secret_access_key: "54321"
  #
  config :aws_credentials_file, :validate => :string

  # Where to write the since database (keeps track of the date
  # the last handled file was added to S3). The default will write
  # sincedb files to some path matching "$HOME/.sincedb*"
  # Should be a path with filename not just a directory.
  config :sincedb_path, :validate => :string, :default => nil

  # Path of a local directory to backup processed files to.
  config :backup_to_dir, :validate => :string, :default => nil

  # Set the directory where logstash will store the tmp files before processing them.
  # default to the current OS temporary directory in linux /tmp/logstash
  config :temporary_directory, :validate => :string, :default => File.join(Dir.tmpdir, "logstash")

  attr_reader :poller

  def register
    require "fileutils"
    require "digest/md5"
    require "aws-sdk"
    require 'securerandom'

    @logger.info("Registering S3file-via-sqs input", :queue => @queue)

    setup_tmp_dir
    # aws config options
    # region, access_key_id, secret_access_key, use_ssl, aws_credentials_file

    if @assume_role_arn
      @sts = Aws::STS::Client.new(aws_options_hash)
      @sqs_owner_info = {queue_owner_aws_account_id: @aws_queue_owner_id}
      get_temp_credentials
    else
      @sqs = Aws::SQS::Client.new(aws_options_hash)
      @s3  = Aws::S3::Client.new(aws_options_hash)
    end

    queue_config = {queue_name:  @queue}
    queue_config.merge!(@sqs_owner_info) unless @sqs_owner_info.nil?

    queue_url = @sqs.get_queue_url(queue_config)[:queue_url]

    # Messages are automatically deleted from the queue at the end of the given block.
    @poller = Aws::SQS::QueuePoller.new(queue_url, :client => @sqs)
    #require 'pry'; binding.pry

    rescue Aws::SQS::Errors::ServiceError, Aws::S3::Errors::ServiceError, Aws::STS::Errors::ServiceError => e
      @logger.error("AWS init error: ", :error => e)
      raise LogStash::ConfigurationError, "Error Registering S3File_Via_Sqs: #{e}"
  end # def register

  def setup_tmp_dir
    unless @backup_to_dir.nil?
      Dir.mkdir(@backup_to_dir, 0700) unless File.exists?(@backup_to_dir)
    end
    FileUtils.mkdir_p(@temporary_directory) unless Dir.exist?(@temporary_directory)
  end

  def polling_options
    {
      :max_number_of_messages => MAX_MESSAGES_TO_FETCH,
      :attribute_names => SQS_ATTRIBUTES,
      :wait_time_seconds => @polling_frequency
    }
  end

  def run(queue)
    @logger.info("Polling SQS messages", :polling_options => polling_options)
    run_with_backoff do
        @poller.poll(polling_options) do |messages, stats|
          counter ||= messages.size
          break if stop?
          s3_locations = []
          if MAX_MESSAGES_TO_FETCH > 1
            @logger.info("Start Polling #{messages.size} messages from SQS - #{Time.now}")
            messages.each { |message|
              counter = (counter - 1)
              @logger.info("now processing message number #{counter}")
              break if stop?
              s3_locations = s3_locations + parse_sqs(message)
            }
          else
            s3_locations = parse_sqs(messages)
          end

          @logger.info("SQS Poller Stats:",
            :request_count => stats.request_count,
            :received_message_count => stats.received_message_count,
            :last_message_received_at => stats.last_message_received_at
          )

          process_log(queue, s3_locations)

          @logger.info("Finished processing #{messages.size} messages - #{Time.now}, batch deleting them now from sqs")
        end # @poller
    end # run_with_backoff
  end # def process_files

  def parse_sqs(message)
    @logger.info("started processing sqs message #{message}")
    s3_locations = []

    begin
      msg = JSON.parse(message.body)
      if msg.has_key?('Records')
        msg['Records'].each { |record|
          @logger.info("Message received at #{Time.now}")
          key =  record['s3']['object']['key']
          bucket = record['s3']['bucket']['name']
          s3_loc = {:bucket => bucket, :key => key}
          @logger.info("Processing S3 file from s3://#{bucket}/#{key} @ #{Time.now}", s3_loc)
          s3_locations << s3_loc
        }
      end
    rescue Exception => e
      @logger.error("Error Parsing JSON - The SQS message: #{message} is not a valid JSON string: #{e}")
    end

    s3_locations
  end

  def process_log(queue, s3_locations)
    unless s3_locations.empty?
      s3_locations.each do |s3_file_info|
        filename = File.join(temporary_directory, File.basename(s3_file_info[:key]))

        if download_remote_file(s3_file_info[:bucket], s3_file_info[:key], filename)
          if process_local_log(queue, filename, s3_file_info)
            lastmod = @s3.get_object(s3_file_info).last_modified # why do we even need this sincedb?
            @bucket = s3_file_info[:bucket]
            backup_to_dir(filename)

            @logger.debug("Deleting local donwloaded file ", :local_filename => filename)
            FileUtils.remove_entry_secure(filename, true)
            sincedb.write(lastmod)
          end
        else
          FileUtils.remove_entry_secure(filename, true)
        end
      end #s3_locations.each
    end # unless
  end

  def backup_to_dir(filename)
    unless @backup_to_dir.nil?
      FileUtils.cp(filename, @backup_to_dir)
    end
  end


  def get_temp_credentials
        require 'pry'; binding.pry

    @logger.info("Getting temp credentials using the role: #{@assume_role_arn}")
    role_credentials = Aws::AssumeRoleCredentials.new(
      client: @sts,
      role_arn: @assume_role_arn,
      role_session_name: "#{SecureRandom.hex}"
    )

    credentials_config = {credentials: role_credentials, region: @region}
    @logger.info("AWS AssumeRoleCredentials: #{credentials_config}")

    @s3 = Aws::S3::Client.new(credentials_config)
    @sqs = Aws::SQS::Client.new(credentials_config)

    rescue Aws::SQS::Errors::ServiceError, Aws::S3::Errors::ServiceError, Aws::STS::Errors::ServiceError, StandardError => e
      @logger.error("Error getting temp credentials:", :error => e)
      raise LogStash::ConfigurationError, "Error getting temp credentials for assume role - #{e}"
  end

  # Runs an AWS request inside a Ruby block with an exponential backoff in case
  # we experience a ServiceError.
  #
  # @param [Integer] max_time maximum amount of time to sleep before giving up.
  # @param [Integer] sleep_time the initial amount of time to sleep before retrying.
  # @param [Block] block Ruby code block to execute.
  def run_with_backoff(max_time = MAX_TIME_BEFORE_GIVING_UP, sleep_time = BACKOFF_SLEEP_TIME, &block)
    next_sleep = sleep_time

    begin
      block.call
      next_sleep = sleep_time
    rescue Aws::SQS::Errors::ServiceError => e
      @logger.warn("Aws::SQS::Errors::ServiceError ... retrying SQS request with exponential backoff", :queue => @queue, :sleep_time => sleep_time, :error => e)
      sleep(next_sleep)
      next_sleep =  next_sleep > max_time ? sleep_time : sleep_time * BACKOFF_FACTOR

      retry
    end
  end

  # Read the content of the local file
  #
  # @param [Queue] Where to push the event
  # @param [String] Which file to read from
  # @return [Boolean] True if the file was completely read, false otherwise.
  def process_local_log(queue, filename, s3_file_info)
    @logger.info("Processing downloaded file - Started at #{Time.now}" , :filename => filename)
    count = 0
    metadata = {}
    # Currently codecs operates on bytes instead of stream.
    # So all IO stuff: decompression, reading need to be done in the actual
    # input and send as bytes to the codecs.
    read_file(filename) do |line|
      if stop?
        @logger.error("Logstash S3 input, stop reading in the middle of the file, we will read it again when logstash is started")
        return false
      end
      count +=1
      @codec.decode(line) do |event|
        # We are making an assumption concerning cloudfront
        # log format, the user will use the plain or the line codec
        # and the message key will represent the actual line content.
        # If the event is only metadata the event will be drop.
        # This was the behavior of the pre 1.5 plugin.
        #
        # The line need to go through the codecs to replace
        # unknown bytes in the log stream before doing a regexp match or
        # you will get a `Error: invalid byte sequence in UTF-8'
        if event_is_metadata?(event)
          @logger.error('Event is metadata, updating the current cloudfront metadata', :event => event)
          update_metadata(metadata, event)
        else
          event.set('path', filename)
          event.set('s3_bucket', s3_file_info[:bucket])
          event.set('s3_key', s3_file_info[:key])
          if !metadata[:cloudfront_version].nil?
            event.set("cloudfront_version", metadata[:cloudfront_version])
          end
          if !metadata[:cloudfront_fields].nil?
            event.set("cloudfront_fields", metadata[:cloudfront_fields])
          end
          decorate(event)
          queue << event
        end
      end #codec
    end #read_file
    @logger.info("Processing downloaded file - Finished at #{Time.now}" , :filename => filename)

    return true
  end # def process_local_log


  def event_is_metadata?(event)
    return false if event.get("message").nil?
    line = event.get("message")
    version_metadata?(line) || fields_metadata?(line)
  end

  def version_metadata?(line)
    line.start_with?('#Version: ')
  end

  def fields_metadata?(line)
    line.start_with?('#Fields: ')
  end

  def update_metadata(metadata, event)
    line = event.get('message').strip

    if version_metadata?(line)
      metadata[:cloudfront_version] = line.split(/#Version: (.+)/).last
    end

    if fields_metadata?(line)
      metadata[:cloudfront_fields] = line.split(/#Fields: (.+)/).last
    end
  end

  def read_file(filename, &block)
    if gzip?(filename)
      read_gzip_file(filename, block)
    else
      read_plain_file(filename, block)
    end
  end

  def read_plain_file(filename, block)
    File.open(filename, 'rb') do |file|
      file.each(&block)
    end
    #remove the file from the pod
    system("rm #{filename}")
  end

  def read_gzip_file(filename, block)
    begin
      #unzip the file localy in etc/logstash folder
      command = "gunzip --force #{filename}"
      string_size = filename.length * -1
      log_file = filename[string_size..-4]
      success = system(command)
      #success && $?.exitstatus == 0
      read_plain_file(log_file, block)
    rescue Zlib::Error, Zlib::GzipFile::Error => e
      @logger.error("Gzip codec: We cannot uncompress the gzip file", :filename => filename)
      raise e
    end
  end

  def gzip?(filename)
    filename.end_with?('.gz') || filename.end_with?('.gzip')
  end

  def sincedb
    @sincedb ||= if @sincedb_path.nil?
                    @logger.info("Using default generated file for the sincedb", :filename => sincedb_file)
                    SinceDB::File.new(sincedb_file)
                  else
                    @logger.info("Using the provided sincedb_path",
                                 :sincedb_path => @sincedb_path)
                    SinceDB::File.new(@sincedb_path)
                  end
  end

  def sincedb_file
    File.join(ENV["HOME"], ".sincedb_" + Digest::MD5.hexdigest("#{@bucket}+#{@prefix}"))
  end

  def ignore_filename?(filename)
    if @prefix == filename
      return true
    elsif (@backup_add_prefix && @backup_to_bucket == @bucket && filename =~ /^#{backup_add_prefix}/)
      return true
    elsif @exclude_pattern.nil?
      return false
    elsif filename =~ Regexp.new(@exclude_pattern)
      return true
    else
      return false
    end
  end



  # Stream the remove file to the local disk
  # @return [Boolean] True if the file was completely downloaded
  def download_remote_file(bucket, key, local_filename)
    completed = false

    @logger.debug("S3 input: Download remote file ", :bucket => bucket, :remote_key => key, :local_filename => local_filename)

    # When using blocks to downloading objects,
    # the Ruby SDK will NOT retry failed requests after the first chunk of data has been yielded.
    # Doing so could cause file corruption on the client end by starting over mid-stream.
    # For this reason we use target file and not loading the file into memory
    begin
      ::File.open(local_filename, 'wb') do |file|
        reap = @s3.get_object({ bucket: bucket, key: key }, target: file)
        return completed if stop?
        completed = true
        @logger.debug("download completed: ", :completed => completed)
        return completed
      end
    rescue Aws::S3::Errors::ServiceError => e
      @logger.error("Could not download the file from S3: ", :error => e)
    end
  end

  module SinceDB
    class File
      def initialize(file)
        @sincedb_path = file
      end

      def newer?(date)
        date > read
      end

      def read
        if ::File.exists?(@sincedb_path)
          content = ::File.read(@sincedb_path).chomp.strip
          # If the file was created but we didn't have the time to write to it
          return content.empty? ? Time.new(0) : Time.parse(content)
        else
          return Time.new(0)
        end
      end

      def write(since = nil)
        since = Time.now() if since.nil?
        ::File.open(@sincedb_path, 'w') { |file| file.write(since.to_s) }
      end
    end
  end
end # class LogStash::Inputs::S3FileViaSqs
