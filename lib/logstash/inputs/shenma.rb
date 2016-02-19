# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "logstash/plugin_mixins/shenma"
require "logstash/plugin_mixins/shenma_sql"
require "yaml" # persistence
require "mongo"

include Mongo

class LogStash::Inputs::Shenma < LogStash::Inputs::Base
  include LogStash::PluginMixins::Shenma
  include LogStash::PluginMixins::ShenmaSql
  config_name "shenma"

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
    @parameters['sql_last_value'] = @sql_last_value
    time_begin = (Date.today()-1).to_time.strftime("%Y-%m-%dT%H:%M:%S")
    time_end = Date.today().to_time.strftime("%Y-%m-%dT%H:%M:%S")

    execute_statement(buyer_everyday_data_sql(time_begin, time_end), @parameters) do |row|
      if(row["userid"] && row["userid"]!=0)
        row["isLogin"] = is_login?(row["userid"], time_begin, time_end)
        row["sendMessage"] = buyer_send_message_number(row["userid"], time_begin.to_time.utc,  time_end.to_time.utc)
        row["receivedMessage"] = buyer_received_message_number(row["userid"], time_begin.to_time.utc,  time_end.to_time.utc)
        row["statisticaldate"] = Date.parse(time_begin)
        event = LogStash::Event.new(row)
        decorate(event)
        queue << event
      end
    end
  end

  def buyer_received_message_number(user_id, time_begin, time_end)
    # conn = Mongo::Client.new("mongodb://Mhdev:Mhdev_123@182.92.7.70:27017/chatserver")
    @mongo_conn[:messages].find( "creationDate" => {'$gt'=> time_begin, '$lt' => time_end}, "toUserId"=> user_id.to_i ).to_a.size
  end

  def buyer_send_message_number(user_id, time_begin, time_end)
    # "username": "Mhdev",
    #   "password": "Mhdev_123",
    #   "host":"10.165.68.116",
    #   "port":"27017",
    #   "dbname":"chatserver"
    # conn = Mongo::Client.new("mongodb://Mhdev:Mhdev_123@182.92.7.70:27017/chatserver")
    @mongo_conn[:messages].find( "creationDate" => {'$gt'=> time_begin, '$lt' => time_end}, "fromUserId"=> user_id.to_i ).to_a.size
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

    if(res["hits"] && res["hits"]["total"] && res["hits"]["total"]> 0)
      return "是"
    else
      return "否"
    end
  end

  def update_state_file
    if @record_last_run
      File.write(@last_run_metadata_path, YAML.dump(@sql_last_value))
    end
  end

end # class LogStash::Inputs::Jdbc