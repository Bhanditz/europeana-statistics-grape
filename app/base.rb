require "rubygems"
require "grape"
require "json"
require 'logging'
require_relative "lib/authentication"
require_relative "models/grid"
require_relative "models/row"
require_relative "models/column"
require_relative "models/datacast"
class Base < Grape::API
  
  format :json
  helpers do
    def authenticate!(username, projectname, filename, token)
      auth_resp = Authentication.sudo_project_member!(username, projectname, filename, token)
      if auth_resp[:core_db_connection_id] and auth_resp[:table_name]
        return auth_resp[:core_db_connection_id],auth_resp[:table_name]
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
              core_db_connection_id, table_name = authenticate!(params[:account_slug], params[:project_slug], params[:datacast_slug], params[:token])
              if !Grid.create(core_db_connection_id, table_name, params[:data], params[:first_row_header])
                error!({error: "[rumi-api] grid.create > Failed."}, 422)
              end
            end
            
            post :delete do
              core_db_connection_id, table_name = authenticate!(params[:account_slug], params[:project_slug], params[:datacast_slug], params[:token])
              if !Grid.delete(core_db_connection_id, table_name)
                time_to_run = Time.now - @girish_start
                err = ["[rumi-api] grid.delete > Failed.", 422]
                logger.error "#{@girish_start}|#{env['REQUEST_METHOD']}|#{env['PATH_INFO']}|#{env['QUERY_STRING']}|#{time_to_run}|#{env['REMOTE_ADDR']}|#{env['HTTP_REFERER']}|#{err[0]}|#{err[1]}|#{params[:config]}"
                error!({error: err[0]}, err[1])
              end
            end
          end

          resource :row do
            
            params do
              requires :data
            end

            post :batch_add do
              core_db_connection_id, table_name = authenticate!(params[:account_slug], params[:project_slug], params[:datacast_slug], params[:token])
              if !Row.batch_add(core_db_connection_id, table_name, params[:data])
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

    resource :datacast do
      route_param :identifier do
        get do
          datacast_output = Datacast.get_datacast_output(params[:identifier])
          if datacast_output
            datacast_output
          else
            time_to_run = Time.now - @girish_start
            err = ["[rumi-api] datacast.identifier > Not found.", 404]
            logger.error "#{@girish_start}|#{env['REQUEST_METHOD']}|#{env['PATH_INFO']}|#{env['QUERY_STRING']}|#{time_to_run}|#{env['REMOTE_ADDR']}|#{env['HTTP_REFERER']}|#{err[0]}|#{err[1]}|#{params[:config]}"
            error!({error: err[0]}, err[1])
          end
        end
      end
    end
  end


end