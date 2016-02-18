# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "logstash/plugin_mixins/example"
require "yaml" # persistence

class LogStash::Inputs::Example < LogStash::Inputs::Base
  include LogStash::PluginMixins::Example
  config_name "example"

  # If undefined, Logstash will complain, even if codec is unused.
  default :codec, "plain"

  # Statement to execute
  #
  # To use parameters, use named parameter syntax.
  # For example:
  #
  # [source, ruby]
  # ----------------------------------
  # "SELECT * FROM MYTABLE WHERE id = :target_id"
  # ----------------------------------
  #
  # here, ":target_id" is a named parameter. You can configure named parameters
  # with the `parameters` setting.
  config :statement, :validate => :string

  # Path of file containing statement to execute
  config :statement_filepath, :validate => :path

  # Hash of query parameter, for example `{ "target_id" => "321" }`
  config :parameters, :validate => :hash, :default => {}

  # Schedule of when to periodically run statement, in Cron format
  # for example: "* * * * *" (execute query every minute, on the minute)
  #
  # There is no schedule by default. If no schedule is given, then the statement is run
  # exactly once.
  config :schedule, :validate => :string

  # Path to file with last run time
  config :last_run_metadata_path, :validate => :string, :default => "#{ENV['HOME']}/.logstash_jdbc_last_run"

  # Use an incremental column value rather than a timestamp
  config :use_column_value, :validate => :boolean, :default => false

  # If tracking column value rather than timestamp, the column whose value is to be tracked
  config :tracking_column, :validate => :string

  # Whether the previous run state should be preserved
  config :clean_run, :validate => :boolean, :default => false

  # Whether to save state or not in last_run_metadata_path
  config :record_last_run, :validate => :boolean, :default => true

  # Whether to force the lowercasing of identifier fields
  config :lowercase_column_names, :validate => :boolean, :default => true

  public

  def register
    require "rufus/scheduler"
    prepare_jdbc_connection

    # Raise an error if @use_column_value is true, but no @tracking_column is set
    if @use_column_value
      if @tracking_column.nil?
        raise(LogStash::ConfigurationError, "Must set :tracking_column if :use_column_value is true.")
      end
    end

    # load sql_last_value from file if exists
    if @clean_run && File.exist?(@last_run_metadata_path)
      File.delete(@last_run_metadata_path)
    elsif File.exist?(@last_run_metadata_path)
      @sql_last_value = YAML.load(File.read(@last_run_metadata_path))
    end

    unless @statement.nil? ^ @statement_filepath.nil?
      raise(LogStash::ConfigurationError, "Must set either :statement or :statement_filepath. Only one may be set at a time.")
    end

    @statement = File.read(@statement_filepath) if @statement_filepath
  end # def register

  def run(queue)
    @logger.error("run action #{queue}")
    @logger.info("run action:")
    @logger.info(queue)
    if @schedule
      @scheduler = Rufus::Scheduler.new(:max_work_threads => 1)
      @scheduler.cron @schedule do
        execute_query(queue)
        update_state_file
      end

      @scheduler.join
    else
      execute_query(queue)
      update_state_file
    end
  end # def run

  def stop
    @scheduler.stop if @scheduler

    close_jdbc_connection
  end

  private

  def execute_query(queue)
    @logger.error("execute_query action #{queue}")
    # update default parameters
    @parameters['sql_last_value'] = @sql_last_value
    execute_statement(@statement, @parameters) do |row|

      @logger.error("execute_query callback action #{row}")
      if(row["userid"] && row["userid"]!=0)
        is_login?(row["userid"], "2015-01-10T19:25:42.6063412+08:00", "2017-01-10T19:25:42.6063412+08:00")
        event = LogStash::Event.new(row)
        decorate(event)
        queue << event
      end
    end
  end

  def is_login?(user_id, time_begin, time_end)
    client = Elasticsearch::Client.new(:host => @jdbc_ecs_host)
    query = {
      "size"=> 0,
      "query"=> {
        "bool"=> {
          "must"=> [
            {
              "term"=> {
                "userLevel"=> {
                  "value"=> 4
                }
              }
            },
            {
              "range"=> {
                "visitDate"=> {
                  "gte"=> time_begin
                }
              }
            },
            {
              "range"=> {
                "visitDate"=> {
                  "lte"=> time_end
                }
              }
            },
            {
              "terms"=> {
                "userId"=> [ user_id ]
              }
            }
          ]
        }
      },
      "aggs"=> {
        "stat_user"=> {
          "terms"=> {
            "field"=> "userId",
            "size"=> 1000
          }
        }
      }
    }
    res = client.search({body: query, index: "esmapping"})
    @logger.error("ecs result #{res}")
    res
  end

  def update_state_file
    if @record_last_run
      File.write(@last_run_metadata_path, YAML.dump(@sql_last_value))
    end
  end

end # class LogStash::Inputs::Jdbc