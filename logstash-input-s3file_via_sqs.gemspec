Gem::Specification.new do |s|

  s.name            = 'logstash-input-s3file_via_sqs'
  s.version         = '1.0.4'
  s.licenses        = ['Apache License (2.0)']
  s.summary         = "Stream events from files using S3 events to send filenames to a Amazon Simple Queue Service (SQS)."
  s.description     = "This gem is a logstash plugin required to be installed on top of the Logstash core pipeline using $LS_HOME/bin/plugin install gemname. This gem is not a stand-alone program"
  s.authors         = ["Ami Mahloof"]
  s.email           = 'ami.mahloof@gmail.com'
  s.homepage        = "https://github.com/innovia/logstash-input-s3file_via_sqs"
  s.require_paths = ["lib"]

  # Files
  s.files = Dir['lib/**/*','spec/**/*','vendor/**/*','*.gemspec','*.md','CONTRIBUTORS','Gemfile','LICENSE','NOTICE.TXT']

  # Tests
  s.test_files = s.files.grep(%r{^(test|spec|features)/})

  # Special flag to let us know this is actually a logstash plugin
  s.metadata = { "logstash_plugin" => "true", "logstash_group" => "input" }

  # Gem dependencies
  s.add_runtime_dependency "logstash-core", ">= 2.0.0.beta2", "< 9.0.0"
  s.add_runtime_dependency 'logstash-mixin-aws'
  s.add_runtime_dependency 'stud', '~> 0.0.18'

  #s.add_development_dependency 'logstash-devutils'
  s.add_development_dependency 'simplecov'
  s.add_development_dependency 'coveralls'
  s.add_development_dependency "logstash-codec-json"
end

