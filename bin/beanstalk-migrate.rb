#!/usr/bin/env ruby

require 'rubygems'
require 'beanstalk-client'

def log(message)
  $stderr.puts message
end

def fail_with_error(message)
  log message
  exit 1
end

RESERVE_WAIT = 5
SYNTAX = "Syntax: beanstalk-migrate.rb source:port target:port"
ARGV.length == 2 or fail_with_error(SYNTAX)

log "Connecting to source: #{ARGV[0]}"
SOURCE = Beanstalk::Connection.new ARGV[0]
log "Connecting to target: #{ARGV[1]}"
TARGET = Beanstalk::Connection.new ARGV[1]

def kick(n=10000000)
  BS.kick n
rescue
  # Kick isn't implemented in the current released client.
  BS.send(:interact, "kick #{n}\r\n", %w(KICKED))[0].to_i
end

def watch_all_tubes
  SOURCE.list_tubes.each do |tube|
    SOURCE.watch tube
  end
end

def migrate_one_job
  source_job = SOURCE.reserve RESERVE_WAIT

  tube = source_job.stats["tube"]
  job_desc = "tube #{tube} pri #{source_job.pri} delay #{source_job.delay}"
  log "Migrating job #{source_job.id} (#{job_desc})"
                  
  TARGET.use tube
  TARGET.put source_job.body, source_job.pri, source_job.delay, source_job.ttr
  
  source_job.delete
  
  true
rescue Beanstalk::TimedOut
  false
end
  

def migrate_loop
  watch_all_tubes
  
  loop do
    something_migrated = migrate_one_job
    
    # Start watching any new tubes.
    watch_all_tubes unless something_migrated
  end
end

migrate_loop