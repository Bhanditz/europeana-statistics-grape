require_relative '../lib/common_queries'

class DataStore

	def self.update_size(db_identifier , table_name, table_size)  
		update_size_query = "UPDATE core_data_stores SET properties = properties || '\"size\"=>\"#{table_size}\"'::hstore WHERE table_name=#{table_name};"
		CQ.execute_query(db_identifier, update_size_query)
	end

end