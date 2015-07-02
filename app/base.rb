require "rubygems"
require "grape"
require "json"
require 'logging'
require_relative "lib/authentication"
require_relative "models/grid"
require_relative "models/row"

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
        route_param :datacast_slug do
          
          # GRID -----------------------
          resource :grid do
            
            params do
              requires :first_row_header, type: Boolean
              requires :data, type: Array
            end
            
            post :create do
              table_name = authenticate!(params[:account_slug], params[:project_slug], params[:datacast_id], params[:token])
              if !Grid.create("api_rumi", table_name, params[:data], params[:first_row_header])
                error!({error: "[rumi-api] grid.create > Failed."}, 422)
              end
            end
    
            post :delete do
              table_name = authenticate!(params[:account_slug], params[:project_slug], params[:datacast_id], params[:token])
              if !Grid.delete("api_rumi", table_name)
                time_to_run = Time.now - @girish_start
                err = ["[rumi-api] grid.delete > Failed.", 422]
                logger.error "#{@girish_start}|#{env['REQUEST_METHOD']}|#{env['PATH_INFO']}|#{env['QUERY_STRING']}|#{time_to_run}|#{env['REMOTE_ADDR']}|#{env['HTTP_REFERER']}|#{err[0]}|#{err[1]}|#{params[:config]}"
                error!({error: err[0]}, err[1])
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