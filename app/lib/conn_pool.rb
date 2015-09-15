require 'connection_pool'

module ConnPool
	
	env = JSON.parse(File.read('./environment.json'))
	env = env["current_environment"]
	
	case env
	when 'development'
		db_config_file = './development.json'
	when 'production'
		db_config_file = './production.json'
	when 'staging'
		db_config_file = './staging.json'
	end

	@@available_dbs = JSON.parse(File.read(db_config_file))
	
	def ConnPool.get_db_connection_pool(db_identifier, pool_size=3)
		if @@available_dbs.has_key? db_identifier
			config = @@available_dbs[db_identifier]
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

end