# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "logstash/plugin_mixins/shenma"
require "logstash/plugin_mixins/shenma_sql"
require "yaml" # persistence
require "mongo"

include Mongo
include YAML

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
    @logger.error("execute_query action #{@jdbc_task_name}")
    @logger.error("execute_query action #{@jdbc_task_name == 'store_everyweek_data'}")
    
    if @jdbc_task_name == "buyer_everyday_data"
      execute_query_buyer_everyday_data(queue)
    elsif @jdbc_task_name == "buyer_everyweek_data" || @jdbc_task_name == "buyer_everymonth_data"
      execute_query_buyer_everyweek_data(queue)
    elsif @jdbc_task_name == "store_everyweek_data"
      execute_query_store_data(queue)  
    end
  end

  def execute_query_store_data(queue)
    @parameters['sql_last_value'] = @sql_last_value
    time_end =@time_end || Date.today().to_time.strftime("%Y-%m-%dT%H:%M:%S")
    if @date_interval == 'week'
      time_begin =  (Date.parse(time_end)-7).to_time.strftime("%Y-%m-%dT%H:%M:%S")
    elsif @date_interval == 'month'
      time_begin =  (Date.parse(time_end) << 1).to_time.strftime("%Y-%m-%dT%H:%M:%S")
    else
      time_begin =  (Date.parse(time_end)-1).to_time.strftime("%Y-%m-%dT%H:%M:%S")
    end
    execute_statement(store_sql(Date.parse(time_begin).to_s, Date.parse(time_end).to_s), @parameters) do |row|
      if(row["store_id"] && row["store_id"]!=0)
        event = LogStash::Event.new(translate_name(row, "store_data"))
        decorate(event)
        queue << event
      end
    end

  end

  #每周买手数据
  def execute_query_buyer_everyweek_data(queue)
    @parameters['sql_last_value'] = @sql_last_value
    time_end =@time_end || Date.today().to_time.strftime("%Y-%m-%dT%H:%M:%S")
    if @date_interval == 'week'
      time_begin =  (Date.parse(time_end)-7).to_time.strftime("%Y-%m-%dT%H:%M:%S")
    elsif @date_interval == 'month'
      time_begin =  (Date.parse(time_end) << 1).to_time.strftime("%Y-%m-%dT%H:%M:%S")
    else
      time_begin =  (Date.parse(time_end)-1).to_time.strftime("%Y-%m-%dT%H:%M:%S")
    end

    all_new_customer = @database[new_add_favorite_sql(time_begin,time_end), {}]
    @logger.error("all_new_customer action *******************\r\n #{all_new_customer.to_a}")

    all_new_orders = @database[new_add_orders_sql(time_begin,time_end), {}].to_a.map{|x| extract_values_from(x)}
    @logger.error("all_new_customer action *******************\r\n #{all_new_orders.to_a}")

    all_new_orders_group_buyeruserid = all_new_orders.to_a.group_by{|o| o["buyer_userid"]}
    @logger.error("all_new_orders_group_buyeruserid action *******************\r\n #{all_new_orders_group_buyeruserid}")

    all_orders = @database["select income.AssociateUserId, o.CustomerId, count(1) as order_number from `order` o join ims_associateincomehistory income on o.OrderNo = income.SourceNo where o.`Status` > 0 and o.CreateDate < '#{time_end}' group by o.CustomerId, income.AssociateUserId", {}].to_a


    execute_statement(buyer_everyweek_data_sql(Date.parse(time_begin).to_s, Date.parse(time_end).to_s), @parameters) do |row|
      
      if(row["userid"] && row["userid"]!=0)
        row["login_days"] = login_days(row["userid"], time_begin, time_end)
        row["sendMessage"] = buyer_send_message_number(row["userid"], Time.parse(time_begin).utc,  Time.parse(time_end).utc)
        row["receivedMessage"] = buyer_received_message_number(row["userid"], Time.parse(time_begin).utc,  Time.parse(time_end).utc)
        row["private_message_number"] = buyer_private_message_number(row["userid"], Time.parse(time_begin).utc,  Time.parse(time_end).utc)
        row["group_message_number"] = buyer_group_message_number(row["userid"], Time.parse(time_begin).utc,  Time.parse(time_end).utc)
        row["private_message_customer_number"] = buyer_private_message_customer_number(row["userid"], Time.parse(time_begin).utc,  Time.parse(time_end).utc)
        row["time_begin"] = Date.parse(time_begin).to_s
        row["time_end"] = Date.parse(time_end).to_s
        row["orderamount"] = row["orderamount"].to_f
        row["orderrecivedamount"] = row["orderrecivedamount"].to_f
        row["userLevel"] = (row["userlevel"].to_i == 4 ? "专柜买手" : (row["userlevel"].to_i == 8 ? "认证买手" : (row["userlevel"].to_i == 16 ? "品牌买手" : "未知类型")  ))

        row["mulit_buy_number"] = 0
        row["mulit_buy_total_amount"] = 0
        row["customer_total_amount"] = 0
        row["new_customer_total_amount"] = 0
        if all_new_orders_group_buyeruserid[row["userid"]]
          all_mulit_buy_customerids = all_orders.select{|x| x[:associateuserid] == row["userid"] && x[:order_number] > 1 }.map{|x| x[:customerid]}

           @logger.error("all_mulit_buy_customerids action #{row['userid']} *******************\r\n #{all_mulit_buy_customerids}")
          mulit_buy_data = @database[mulit_buy_sql(row["userid"],all_new_orders_group_buyeruserid[row["userid"]].map{|x| x["customerid"]}.join(","),time_end, time_begin), {}].to_a.select{|x| all_mulit_buy_customerids.include?(x[:customerid]) }

          row["mulit_buy_number"] =  mulit_buy_data.size
          row["mulit_buy_total_amount"] =  mulit_buy_data.inject(0){|sum, x| sum+x[:total_amount].to_f}
          row["customer_total_amount"] = all_new_orders_group_buyeruserid[row["userid"]].group_by{|x| x["customerid"]}.size
          row["new_customer_total_amount"] = (all_new_orders_group_buyeruserid[row["userid"]].group_by{|x| x["customerid"]}.keys & all_new_orders_group_buyeruserid[row["userid"]].map{|x| x["customerid"]}).size
        end

        event = LogStash::Event.new(translate_name(row, "buyer_everyweek_data"))
        decorate(event)
        queue << event
      end
    end
  end

  #每日买手数据导出
  def execute_query_buyer_everyday_data(queue)
    @parameters['sql_last_value'] = @sql_last_value
    time_end =@time_end || Date.today().to_time.strftime("%Y-%m-%dT%H:%M:%S")
    if @date_interval == 'week'
      time_begin =  (Date.parse(time_end)-7).to_time.strftime("%Y-%m-%dT%H:%M:%S")
    elsif @date_interval == 'month'
      time_begin =  (Date.parse(time_end) << 1).to_time.strftime("%Y-%m-%dT%H:%M:%S")
    else
      time_begin =  (Date.parse(time_end)-1).to_time.strftime("%Y-%m-%dT%H:%M:%S")
    end
    execute_statement(buyer_everyday_data_sql(Date.parse(time_begin).to_s, Date.parse(time_end).to_s), @parameters) do |row|
      
      if(row["userid"] && row["userid"]!=0)
        row["isLogin"] = is_login?(row["userid"], time_begin, time_end)
        row["sendMessage"] = buyer_send_message_number(row["userid"], Time.parse(time_begin).utc,  Time.parse(time_end).utc)
        row["receivedMessage"] = buyer_received_message_number(row["userid"], Time.parse(time_begin).utc,  Time.parse(time_end).utc)
        row["time_begin"] = Date.parse(time_begin).to_s
        row["time_end"] = Date.parse(time_end).to_s
        row["orderamount"] = row["orderamount"].to_f
        row["orderrecivedamount"] = row["orderrecivedamount"].to_f
        row["userLevel"] = (row["userlevel"].to_i == 4 ? "专柜买手" : (row["userlevel"].to_i == 8 ? "认证买手" : (row["userlevel"].to_i == 16 ? "品牌买手" : "未知类型")  ))
        event = LogStash::Event.new(translate_name(row, "buyer_everyday_data"))
        decorate(event)
        queue << event
      end
    end
  end

  def translate_name(hash, namespase)
    yaml = YAML::load(File.read("#{File.dirname(File.dirname(File.dirname(File.dirname(File.expand_path(__FILE__)))))}/locales/zh.yml"))
    res = {}
    hash.each do |k, v|
      if yaml["zh"] && yaml["zh"][namespase.to_s] &&  yaml["zh"][namespase.to_s][k.to_s]
        res[yaml["zh"][namespase.to_s][k.to_s]] = v
      else
        res[k] = v
      end
    end
    return res
  end

  def buyer_private_message_customer_number(user_id, time_begin, time_end)
    @mongo_conn[:messages].find( "creationDate" => {'$gt'=> time_begin, '$lt' => time_end}, "toUserId"=> user_id.to_i, "messageType" => 0 ).group_by{|m| m["fromUserId"]}.size
  end

  def buyer_private_message_number(user_id, time_begin, time_end)
    @mongo_conn[:messages].find( "creationDate" => {'$gt'=> time_begin, '$lt' => time_end}, "toUserId"=> user_id.to_i, "messageType" => 0 ).to_a.size + @mongo_conn[:messages].find( "creationDate" => {'$gt'=> time_begin, '$lt' => time_end}, "fromUserId"=> user_id.to_i, "messageType" => 0 ).to_a.size
  end

  def buyer_group_message_number(user_id, time_begin, time_end)
    @mongo_conn[:messages].find( "creationDate" => {'$gt'=> time_begin, '$lt' => time_end}, "toUserId"=> user_id.to_i, "messageType" => 1 ).to_a.size + @mongo_conn[:messages].find( "creationDate" => {'$gt'=> time_begin, '$lt' => time_end}, "fromUserId"=> user_id.to_i, "messageType" => 1 ).to_a.size
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

  def login_days(user_id, time_begin, time_end)
    query={
      "size"=> 0, 
      "query"=> {
        "bool"=> {
          "must"=> [
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
        "stat_date"=> {
          "date_histogram"=> {
            "field"=> "visitDate",
            "interval"=> "day"
          }
        }
      }
    }
    client = Elasticsearch::Client.new(:host => @jdbc_ecs_host)
    res = client.search({body: query, index: "esmapping", type:"ESUserVisitPage" })
    @logger.error("ecs result #{res}")

    if res && res["aggregations"] && res["aggregations"]["stat_date"] && res["aggregations"]["stat_date"]["buckets"] && res["aggregations"]["stat_date"]["buckets"].class == Array
      res["aggregations"]["stat_date"]["buckets"]
      return res["aggregations"]["stat_date"]["buckets"].inject(0){|sum, e| (e["doc_count"] && e["doc_count"] > 0) ? sum+1 : sum}
    else
      return 0
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
            "size"=> 10000
          }
        }
      }
    }
    res = client.search({body: query, index: "esmapping", type:"ESUserVisitPage"})
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