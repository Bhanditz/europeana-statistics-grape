require "rubygems"
require "grape"
require "json"
require 'logging'
require_relative "lib/authentication"
require_relative "models/cell"
require_relative "models/grid"
require_relative "models/row"
require_relative "models/column"
require_relative "models/filter"

class Base < Grape::API
  
  format :json
  helpers do
    def authenticate!(username, projectname, filename, token)
      auth_resp = Authentication.sudo_project_member!(username, projectname, filename, token)
      if auth_resp[:table_name]
        return auth_resp[:table_name]
      else
        error! ( {error: auth_resp[:error_msg]}), 401
      end
    end
  end


  logger = Logging.logger['s']
  logger.add_appenders(
    Logging.appenders.stdout,
    Logging.appenders.file('logs.log')
  )
  logger.level = :info
  logger.level = :debug
  logger.level = :error
  
  before do
    @girish_start = Time.now
  end
  
  #Time, HTTP METHOD, URL, QUERY STRING, TIME TAKEN, IP, REFERRER, PARAMS-CONFIG
  after do
    time_to_run = Time.now - @girish_start
    logger.info "#{@girish_start}|#{env['REQUEST_METHOD']}|#{env['PATH_INFO']}|#{env['QUERY_STRING']}|#{time_to_run}|#{env['REMOTE_ADDR']}|#{env['HTTP_REFERER']}|_|200|#{params[:config]}"
  end
  
  resource :v1 do
    route_param :account_slug do
      route_param :project_slug do
        route_param :datastore_slug do
          
          # CELL -----------------------
          resource :cell do
            
            params do
              requires :row_id, type: Integer
              requires :column_name, type: String
              requires :value, type: String
            end
            
            post :update do
              table_name = authenticate!(params[:account_slug], params[:project_slug], params[:datastore_slug], params[:token])
              response_data = Cell.update("api_rumi", table_name, params[:row_id], params[:column_name], params[:value])
              if response_data.class == Hash and response_data.has_key?(:error_type)
                time_to_run = Time.now - @girish_start
                err = ["[rumi-api] cell.update > Invalid data.", 422]
                logger.error "#{@girish_start}|#{env['REQUEST_METHOD']}|#{env['PATH_INFO']}|#{env['QUERY_STRING']}|#{time_to_run}|#{env['REMOTE_ADDR']}|#{env['HTTP_REFERER']}|#{err[0]}|#{err[1]}|#{params[:config]}"
                error!({error: err[0]}, err[1])
              end
            end
          end
          
          # CELL -----------------------
          # PYKQUERY -----------------------
          
          resource :filter do
            post :show do
              table_name = authenticate!(params[:account_slug], params[:project_slug], params[:datastore_slug], params[:token])
              if not params[:format] 
                params[:format] = params[:config][:dataformat]
              end
              params[:format] = 'array' if not params[:format]
              if not ['json', 'csv', 'array'].include? params[:format]
                time_to_run = Time.now - @girish_start
                err = ["[rumi-api] filter.show > Unsupported Response Type.", 415]
                logger.error "#{@girish_start}|#{env['REQUEST_METHOD']}|#{env['PATH_INFO']}|#{env['QUERY_STRING']}|#{time_to_run}|#{env['REMOTE_ADDR']}|#{env['HTTP_REFERER']}|#{err[0]}|#{err[1]}|#{params[:config]}"
                error!({error: err[0]}, err[1])
              end
              data = Filter.get_data("api_rumi", table_name, params[:config], params[:format])
              if data.class == Array or data.class == String
                {"data" => data}
              else
                time_to_run = Time.now - @girish_start
                if data.class == Hash and data.has_key? "error"
                  err = ["[rumi-api] " + data["error"], 422]
                else
                  err = ["[rumi-api] filter.show > Query Failed.", 400]
                end
                  
                logger.error "#{@girish_start}|#{env['REQUEST_METHOD']}|#{env['PATH_INFO']}|#{env['QUERY_STRING']}|#{time_to_run}|#{env['REMOTE_ADDR']}|#{env['HTTP_REFERER']}|#{err[0]}|#{err[1]}|#{params[:config]}"
                error!({error: err[0]}, err[1])
              end
            end
          end
          
          # PYKQUERY -----------------------
          # GRID -----------------------
          resource :grid do
            
            params do
              requires :first_row_header, type: Boolean
              requires :data, type: Array
            end
            
            post :create do
              table_name = authenticate!(params[:account_slug], params[:project_slug], params[:datastore_slug], params[:token])
              if !Grid.create("api_rumi", table_name, params[:data], params[:first_row_header])
                error!({error: "[rumi-api] grid.create > Failed."}, 422)
              end
            end
    
            post :clone do
              table_name = authenticate!(params[:account_slug], params[:project_slug], params[:datastore_slug], params[:token])
              new_table_name = Grid.clone("api_rumi", table_name)
              if new_table_name
                {"table_name" => new_table_name}
              else
                time_to_run = Time.now - @girish_start
                err = ["[rumi-api] grid.clone > Failed.", 422]
                logger.error "#{@girish_start}|#{env['REQUEST_METHOD']}|#{env['PATH_INFO']}|#{env['QUERY_STRING']}|#{time_to_run}|#{env['REMOTE_ADDR']}|#{env['HTTP_REFERER']}|#{err[0]}|#{err[1]}|#{params[:config]}"
                error!({error: err[0]}, err[1])
              end
            end

            post :delete do
              table_name = authenticate!(params[:account_slug], params[:project_slug], params[:datastore_slug], params[:token])
              if !Grid.delete("api_rumi", table_name)
                time_to_run = Time.now - @girish_start
                err = ["[rumi-api] grid.delete > Failed.", 422]
                logger.error "#{@girish_start}|#{env['REQUEST_METHOD']}|#{env['PATH_INFO']}|#{env['QUERY_STRING']}|#{time_to_run}|#{env['REMOTE_ADDR']}|#{env['HTTP_REFERER']}|#{err[0]}|#{err[1]}|#{params[:config]}"
                error!({error: err[0]}, err[1])
              end
            end

            get :columns do
              table_name = authenticate!(params[:account_slug], params[:project_slug], params[:datastore_slug], params[:token])
              data = Grid.get_columns("api_rumi", table_name)
              if data
                {"data" => data}
              else
                time_to_run = Time.now - @girish_start
                err = ["[rumi-api] grid.columns > Failed.", 422]
                logger.error "#{@girish_start}|#{env['REQUEST_METHOD']}|#{env['PATH_INFO']}|#{env['QUERY_STRING']}|#{time_to_run}|#{env['REMOTE_ADDR']}|#{env['HTTP_REFERER']}|#{err[0]}|#{err[1]}|#{params[:config]}"
                error!({error: err[0]}, err[1])
              end
            end

            get :analyse_datatypes do
              table_name = authenticate!(params[:account_slug], params[:project_slug], params[:datastore_slug], params[:token])
              Grid.analyse_datatypes("api_rumi", table_name)
            end

            params do
              requires :source_column , type: String
              requires :target_column, type: String
              requires :depth, type: Fixnum
            end
            get :graph_traversal do            
              table_name = authenticate!(params[:account_slug], params[:project_slug], params[:datastore_slug], params[:token])
              node_relations = Grid.get_graph_traversal("api_rumi", table_name , params[:source_column], params[:target_column], params[:depth])
              return { "data" => node_relations } if node_relations
              return { "error" => "Graph Traversal failed."}
            end
            params do
              requires :join_config
            end
            post :join do
              table_name = authenticate!(params[:account_slug], params[:project_slug], params[:datastore_slug], params[:token])
              Grid.join("api_rumi", table_name, params[:join_config])
            end

            params do
              requires :source_table_name
            end
            post :append_dataset do
              table_name = authenticate!(params[:account_slug], params[:project_slug], params[:datastore_slug], params[:token])
              query_resp = Grid.append_dataset("api_rumi", table_name, params[:source_table_name])
              if query_resp.class == Hash and query_resp.has_key? ("error")
                time_to_run = Time.now - @girish_start
                err = [query_resp, 422]
                logger.error "#{@girish_start}|#{env['REQUEST_METHOD']}|#{env['PATH_INFO']}|#{env['QUERY_STRING']}|#{time_to_run}|#{env['REMOTE_ADDR']}|#{env['HTTP_REFERER']}|#{err[0]}|#{err[1]}"
                error!(err[0], err[1])
              end
            end
             
          end
          # GRID -----------------------
          # ROW -----------------------
          resource :row do
            
            params do
              requires :data, type: Array
            end
            
            post :add do
              table_name = authenticate!(params[:account_slug], params[:project_slug], params[:datastore_slug], params[:token])
              grid_data = []
              params[:data].each { |v| grid_data << v }
              query_response = Row.add("api_rumi", table_name, grid_data)
              if query_response.class == Hash and query_response.has_key?(:error_type) and query_response[:error_type] == "InvalidTextRepresentation"
                time_to_run = Time.now - @girish_start
                err = ["[rumi-api] row.add > Failed.", 422]
                logger.error "#{@girish_start}|#{env['REQUEST_METHOD']}|#{env['PATH_INFO']}|#{env['QUERY_STRING']}|#{time_to_run}|#{env['REMOTE_ADDR']}|#{env['HTTP_REFERER']}|#{err[0]}|#{err[1]}|#{params[:config]}"
                error!({error: err[0]}, err[1])
              end
              {"id" => query_response}
            end
            
            params do
              requires :data
            end

            post :batch_add do
              table_name = authenticate!(params[:account_slug], params[:project_slug], params[:datastore_slug], params[:token])
              if !Row.batch_add("api_rumi", table_name, params[:data])
                time_to_run = Time.now - @girish_start
                err = ["[rumi-api] row.batch_add > Failed.", 422]
                logger.error "#{@girish_start}|#{env['REQUEST_METHOD']}|#{env['PATH_INFO']}|#{env['QUERY_STRING']}|#{time_to_run}|#{env['REMOTE_ADDR']}|#{env['HTTP_REFERER']}|#{err[0]}|#{err[1]}|#{params[:config]}"
                error!({error: err[0]}, err[1])
              end
            end
            
            params do
              requires :row_id, type: Array
            end
    
            post :delete do
              table_name = authenticate!(params[:account_slug], params[:project_slug], params[:datastore_slug], params[:token])
              rows_to_delete = []
              params[:row_id].each { |v| rows_to_delete << v }
              if !Row.delete("api_rumi", table_name, rows_to_delete)
                time_to_run = Time.now - @girish_start
                err = ["[rumi-api] row.delete > Failed.", 422]
                logger.error "#{@girish_start}|#{env['REQUEST_METHOD']}|#{env['PATH_INFO']}|#{env['QUERY_STRING']}|#{time_to_run}|#{env['REMOTE_ADDR']}|#{env['HTTP_REFERER']}|#{err[0]}|#{err[1]}|#{params[:config]}"
                error!({error: err[0]}, err[1])
              end
            end

          end  
          # ROW -----------------------
          # COLUMN -----------------------
          resource :column do
    
            params do
              requires :column_name, type: String
            end
    
            post :add do
              table_name = authenticate!(params[:account_slug], params[:project_slug], params[:datastore_slug], params[:token])
              params[:after] = false if !params.has_key? "after"
              actual_column_name = Column.add("api_rumi", table_name, params[:column_name], params[:after])
              if actual_column_name.class == String
                {"column_name" => actual_column_name}
              else
                time_to_run = Time.now - @girish_start
                err = ["[rumi-api] #{actual_column_name["error"]}", 422]
                logger.error "#{@girish_start}|#{env['REQUEST_METHOD']}|#{env['PATH_INFO']}|#{env['QUERY_STRING']}|#{time_to_run}|#{env['REMOTE_ADDR']}|#{env['HTTP_REFERER']}|#{err[0]}|#{err[1]}|#{params[:config]}"
                error!({error: err[0]}, err[1])
              end
            end

            params do
              requires :column_name, type: String
              requires :new_original_column_name, type: String
            end
    
            post :change_name do
              table_name = authenticate!(params[:account_slug], params[:project_slug], params[:datastore_slug], params[:token])
              actual_column_name = Column.change_name("api_rumi", table_name, params[:column_name], params[:new_original_column_name])
              if actual_column_name.class == String
                {"column_name" => actual_column_name}
              else
                time_to_run = Time.now - @girish_start
                err = ["[rumi-api] #{actual_column_name["error"]}", 422]
                logger.error "#{@girish_start}|#{env['REQUEST_METHOD']}|#{env['PATH_INFO']}|#{env['QUERY_STRING']}|#{time_to_run}|#{env['REMOTE_ADDR']}|#{env['HTTP_REFERER']}|#{err[0]}|#{err[1]}|#{params[:config]}"
                error!({error: err[0]}, err[1])
              end
            end

            params do
              requires :column_name, type: String
              requires :new_type, type: String
            end
            
            post :change_type do
              table_name = authenticate!(params[:account_slug], params[:project_slug], params[:datastore_slug], params[:token])
              if !Column.change_type("api_rumi", table_name, params[:column_name], params[:new_type])
                time_to_run = Time.now - @girish_start
                err = ["[rumi-api] Failed to change " + params[:column_name] + " data type to " + params[:new_type] + ".", 422]
                logger.error "#{@girish_start}|#{env['REQUEST_METHOD']}|#{env['PATH_INFO']}|#{env['QUERY_STRING']}|#{time_to_run}|#{env['REMOTE_ADDR']}|#{env['HTTP_REFERER']}|#{err[0]}|#{err[1]}|#{params[:config]}"
                error!({error: err[0]}, err[1])
              end
            end


            params do
              requires :column_name, type: String
              requires :new_type, type: String
            end

            post :change_sub_type do
              table_name = authenticate!(params[:account_slug], params[:project_slug], params[:datastore_slug], params[:token])
              if !Column.change_sub_type("api_rumi", table_name, params[:column_name], params[:new_type])
                time_to_run = Time.now - @girish_start
                err = ["[rumi-api] Failed to change " + params[:column_name] + " sub type to " + params[:new_type] + ".", 422]
                logger.error "#{@girish_start}|#{env['REQUEST_METHOD']}|#{env['PATH_INFO']}|#{env['QUERY_STRING']}|#{time_to_run}|#{env['REMOTE_ADDR']}|#{env['HTTP_REFERER']}|#{err[0]}|#{err[1]}|#{params[:config]}"
                error!({error: err[0]}, err[1])
              end
            end

            params do
              requires :column_name, type: String
              requires :after, type: String
            end
            
            post :move do
              table_name = authenticate!(params[:account_slug], params[:project_slug], params[:datastore_slug], params[:token])
              if !Column.move("api_rumi", table_name, params[:column_name], params[:after])
                time_to_run = Time.now - @girish_start
                err = ["[rumi-api] column.move > Failed.", 422]
                logger.error "#{@girish_start}|#{env['REQUEST_METHOD']}|#{env['PATH_INFO']}|#{env['QUERY_STRING']}|#{time_to_run}|#{env['REMOTE_ADDR']}|#{env['HTTP_REFERER']}|#{err[0]}|#{err[1]}|#{params[:config]}"
                error!({error: err[0]}, err[1])
              end
            end
    
            params do
              requires :column_name, type: String
            end
            
            post :delete do
              table_name = authenticate!(params[:account_slug], params[:project_slug], params[:datastore_slug], params[:token])
              if !Column.delete("api_rumi", table_name, params[:column_name])
                time_to_run = Time.now - @girish_start
                err = ["[rumi-api] column.delete > Failed.", 422]
                logger.error "#{@girish_start}|#{env['REQUEST_METHOD']}|#{env['PATH_INFO']}|#{env['QUERY_STRING']}|#{time_to_run}|#{env['REMOTE_ADDR']}|#{env['HTTP_REFERER']}|#{err[0]}|#{err[1]}|#{params[:config]}"
                error!({error: err[0]}, err[1])
              end
            end

            params do
              requires :column_name, type: String
            end
            
            post :uppercase do
              table_name = authenticate!(params[:account_slug], params[:project_slug], params[:datastore_slug], params[:token]) 
              if !Column.uppercase("api_rumi", table_name, params[:column_name])
                time_to_run = Time.now - @girish_start
                err = ["[rumi-api] column.uppercase > Failed.", 422]
                logger.error "#{@girish_start}|#{env['REQUEST_METHOD']}|#{env['PATH_INFO']}|#{env['QUERY_STRING']}|#{time_to_run}|#{env['REMOTE_ADDR']}|#{env['HTTP_REFERER']}|#{err[0]}|#{err[1]}|#{params[:config]}"
                error!({error: err[0]}, err[1])
              end
            end

            params do
              requires :column_name, type: String
            end
            
            post :lowercase do
              table_name = authenticate!(params[:account_slug], params[:project_slug], params[:datastore_slug], params[:token]) 
              if !Column.lowercase("api_rumi", table_name, params[:column_name])
                time_to_run = Time.now - @girish_start
                err = ["[rumi-api] column.lowercase > Failed.", 422]
                logger.error "#{@girish_start}|#{env['REQUEST_METHOD']}|#{env['PATH_INFO']}|#{env['QUERY_STRING']}|#{time_to_run}|#{env['REMOTE_ADDR']}|#{env['HTTP_REFERER']}|#{err[0]}|#{err[1]}|#{params[:config]}"
                error!({error: err[0]}, err[1])
              end
            end
    
            params do
              requires :column_name, type: String
            end
            
            post :titleize do
              table_name = authenticate!(params[:account_slug], params[:project_slug], params[:datastore_slug], params[:token]) 
              if !Column.titleize("api_rumi", table_name, params[:column_name])
                time_to_run = Time.now - @girish_start
                err = ["[rumi-api] column.titleize > Failed.", 422]
                logger.error "#{@girish_start}|#{env['REQUEST_METHOD']}|#{env['PATH_INFO']}|#{env['QUERY_STRING']}|#{time_to_run}|#{env['REMOTE_ADDR']}|#{env['HTTP_REFERER']}|#{err[0]}|#{err[1]}|#{params[:config]}"
                error!({error: err[0]}, err[1])
              end
            end

            params do
              requires :column_name, type: String
            end
            
            get :datatype_distribution do
              table_name = authenticate!(params[:account_slug], params[:project_slug], params[:datastore_slug], params[:token])
              datatype_distribution = Column.datatype_distribution("api_rumi", table_name, params[:column_name])
              if datatype_distribution
                {"dtd" => datatype_distribution}
              else
                time_to_run = Time.now - @girish_start
                err = ["[rumi-api] column.datatype_distribution > Failed.", 422]
                logger.error "#{@girish_start}|#{env['REQUEST_METHOD']}|#{env['PATH_INFO']}|#{env['QUERY_STRING']}|#{time_to_run}|#{env['REMOTE_ADDR']}|#{env['HTTP_REFERER']}|#{err[0]}|#{err[1]}|#{params[:config]}"
                error!({error: err[0]}, err[1])
              end
            end

            params do
              requires :column_name, type: String
            end
            
            post :trim do
              table_name = authenticate!(params[:account_slug], params[:project_slug], params[:datastore_slug], params[:token])
              if !Column.trim("api_rumi", table_name, params[:column_name])
                time_to_run = Time.now - @girish_start
                err = ["[rumi-api] column.trim > Failed.", 422]
                logger.error "#{@girish_start}|#{env['REQUEST_METHOD']}|#{env['PATH_INFO']}|#{env['QUERY_STRING']}|#{time_to_run}|#{env['REMOTE_ADDR']}|#{env['HTTP_REFERER']}|#{err[0]}|#{err[1]}|#{params[:config]}"
                error!({error: err[0]}, err[1])
              end
            end

            params do
              requires :column_name, type: String
            end
            
            post :trim_inside do
              table_name = authenticate!(params[:account_slug], params[:project_slug], params[:datastore_slug], params[:token])
              if !Column.trim_inside("api_rumi", table_name, params[:column_name])
                time_to_run = Time.now - @girish_start
                err = ["[rumi-api] column.trim_inside > Failed.", 422]
                logger.error "#{@girish_start}|#{env['REQUEST_METHOD']}|#{env['PATH_INFO']}|#{env['QUERY_STRING']}|#{time_to_run}|#{env['REMOTE_ADDR']}|#{env['HTTP_REFERER']}|#{err[0]}|#{err[1]}|#{params[:config]}"
                error!({error: err[0]}, err[1])
              end
            end

            params do
              requires :column_name, type: String
              requires :values, type: Array
              requires :new_value, type: String
            end
            
            post :merge_to_clean do
              table_name = authenticate!(params[:account_slug], params[:project_slug], params[:datastore_slug], params[:token])
              if !Column.replace("api_rumi", table_name, params[:column_name], params[:values], params[:new_value])
                time_to_run = Time.now - @girish_start
                err = ["[rumi-api] column.replace > Failed.", 422]
                logger.error "#{@girish_start}|#{env['REQUEST_METHOD']}|#{env['PATH_INFO']}|#{env['QUERY_STRING']}|#{time_to_run}|#{env['REMOTE_ADDR']}|#{env['HTTP_REFERER']}|#{err[0]}|#{err[1]}|#{params[:config]}"
                error!({error: err[0]}, err[1])
              end
            end

            params do
              requires :column_name, type: String
              requires :value, type: String
            end
            
            post :remove_character do
              table_name = authenticate!(params[:account_slug], params[:project_slug], params[:datastore_slug], params[:token])
              query_resp = Column.remove_character("api_rumi", table_name, params[:column_name], params[:value])
              if query_resp.class == Hash and query_resp.has_key? ("error")
                time_to_run = Time.now - @girish_start
                err = ["[rumi-api] " + query_resp["error"], 422]
                logger.error "#{@girish_start}|#{env['REQUEST_METHOD']}|#{env['PATH_INFO']}|#{env['QUERY_STRING']}|#{time_to_run}|#{env['REMOTE_ADDR']}|#{env['HTTP_REFERER']}|#{err[0]}|#{err[1]}|#{params[:config]}"
                error!({error: err[0]}, err[1])
              end
            end
            
            get :all_columns do
              table_name = authenticate!(params[:account_slug], params[:project_slug], params[:datastore_slug], params[:token])
              params[:data_types] = false if not params.has_key? :data_types
              params[:original_names] = false if not params.has_key? :original_names
              params[:sub_types] = false if not params.has_key? :sub_types
              columns = Column.get_columns_data("api_rumi", table_name, params[:data_types], params[:original_names], params[:sub_types])
              if columns
                {"columns" => columns}
              else
                time_to_run = Time.now - @girish_start
                err = ["[rumi-api] column.all_columns > Failed.", 422]
                logger.error "#{@girish_start}|#{env['REQUEST_METHOD']}|#{env['PATH_INFO']}|#{env['QUERY_STRING']}|#{time_to_run}|#{env['REMOTE_ADDR']}|#{env['HTTP_REFERER']}|#{err[0]}|#{err[1]}|#{params[:config]}"
                error!({error: err[0]}, err[1])
              end
            end

            get :dimensions_and_metrics do
              table_name = authenticate!(params[:account_slug], params[:project_slug], params[:datastore_slug], params[:token])
              results = Column.get_dimension_and_metrics("api_rumi", table_name)
              results
            end
            
            post :set_dimensions_and_metrics do
              table_name = authenticate!(params[:account_slug], params[:project_slug], params[:datastore_slug], params[:token])
              config = {}
              config["dimensions"] = params[:dimensions]
              config["metrics"] = params[:metrics]
              Column.set_dimension_and_metrics("api_rumi", table_name, config)
            end

            params do
              requires :column_name, type: String
              requires :value, type: String
            end

            post :set_column_dimension_and_metrics do 
              table_name = authenticate!(params[:account_slug], params[:project_slug], params[:datastore_slug], params[:token])
              if Column.set_column_dimension_or_metric("api_rumi", table_name, params[:column_name], params[:value])
                return true
              else
                time_to_run = Time.now - @girish_start
                err = ["[rumi-api] Set Column Dimension Metric > Failed.", 422]
                logger.error "#{@girish_start}|#{env['REQUEST_METHOD']}|#{env['PATH_INFO']}|#{env['QUERY_STRING']}|#{time_to_run}|#{env['REMOTE_ADDR']}|#{env['HTTP_REFERER']}|#{err[0]}|#{err[1]}|#{params[:config]}"
                error!({error: err[0]}, err[1])
              end
            end

          end
        end
      end
    end

    resource :data do
      route_param :identifier do

        params do
          requires :token, type: String
          requires :account_slug, type: String
        end

        post :q do
          vizs_object = CQ.get_vizs_pykquery_object(params[:identifier])
          if !vizs_object
            time_to_run = Time.now - @girish_start
            err = ["[rumi-api] data.q > query identifier not found.", 404]
            logger.error "#{@girish_start}|#{env['REQUEST_METHOD']}|#{env['PATH_INFO']}|#{env['QUERY_STRING']}|#{time_to_run}|#{env['REMOTE_ADDR']}|#{env['HTTP_REFERER']}|#{err[0]}|#{err[1]}|#{identifier}"
            error!({error: err[0]}, err[1])
          end
          table_name = authenticate!(params[:account_slug], vizs_object[0][0], vizs_object[0][1], params[:token])
          pykquery_object = vizs_object[0][2]
          pykquery_object = JSON.parse(pykquery_object)
          if pykquery_object.has_key? "dataformat"
            format = pykquery_object["dataformat"]
            if not ['json', 'csv', 'array'].include? format
              time_to_run = Time.now - @girish_start
              err = ["[rumi-api] data.q > Unsupported Response Type.", 415]
              logger.error "#{@girish_start}|#{env['REQUEST_METHOD']}|#{env['PATH_INFO']}|#{env['QUERY_STRING']}|#{time_to_run}|#{env['REMOTE_ADDR']}|#{env['HTTP_REFERER']}|#{err[0]}|#{err[1]}|#{pykquery_object}"
              error!({error: err[0]}, err[1])
            end
          else
            format = "csv"
          end

          data = Filter.get_data("api_rumi", table_name, pykquery_object, format)

          if data.class == Array or data.class == String
            {"data" => data}
          else
            time_to_run = Time.now - @girish_start
            if data.class == Hash and data.has_key? "error"
              err = ["[rumi-api] " + data["error"], 422]
            else
              err = ["[rumi-api] data.q > Query Failed.", 400]
            end

            logger.error "#{@girish_start}|#{env['REQUEST_METHOD']}|#{env['PATH_INFO']}|#{env['QUERY_STRING']}|#{time_to_run}|#{env['REMOTE_ADDR']}|#{env['HTTP_REFERER']}|#{err[0]}|#{err[1]}|#{pykquery_object}"
            error!({error: err[0]}, err[1])
          end

        end
      end
    end
  end


end