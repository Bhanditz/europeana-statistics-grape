require 'connection_pool'
require 'uri'

module ConnPool
	def self.get_db_connection_pool(db_identifier, pool_size=3)
		if available_dbs.has_key? db_identifier
			config = available_dbs[db_identifier]
		else
			return false
		end	
		conn_pool = ConnectionPool.new(size: pool_size) { PG.connect(:dbname => config['database'], 
																																	:user => config['username'], 
																																	:password => config['password'], 
																																	:port => config['port'], 
																																	:host => config['host'] 
																																	) }
		conn_pool
	end

	def self.get_pg_connection(config)
		PG.connect(:dbname => config['db_name'], 
								:user => config['username'],
								:password => config['password'],
								:port => config['port'],
								:host => config['host'])
	end

	def self.available_dbs
		@@available_dbs ||= begin
			db_uri = URI.parse(ENV['DATABASE_URL'])
			{
				'datahub' => {
					'database' => db_uri.path.nil? ? nil : db_uri.path[1..-1],
					'username' => db_uri.user,
					'password' => db_uri.password,
					'port' => db_uri.port,
					'host' => db_uri.host
				}
			}
		end
	end
end
