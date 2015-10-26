require_relative 'conn_pool'
require 'csv'

module CQ

  @@db_pools = Hash.new #maintains db pools
  @@rumi_io_db_identifier = "datahub" #default rumi.io dbs
	
  def self.get_account_id_from_slug(account_slug)
    account_id_query = "SELECT id from accounts WHERE slug=$1 LIMIT 1;"
    query_params = [account_slug]
    begin
      account_id = self.execute_query(@@rumi_io_db_identifier, account_id_query, query_params)
      return account_id.values[0][0]
    rescue
      return false
    end
  end

	def self.get_project_id_from_slug(project_slug, account_id) 
    project_id_query = "SELECT id from core_projects WHERE slug=$1 and account_id=$2 LIMIT 1;"
    query_params = [project_slug,account_id]
    begin
      project_id = self.execute_query(@@rumi_io_db_identifier, project_id_query, query_params)
      return project_id.values[0][0]
    rescue
      return false
    end
  end

  def self.get_table_name_from_slug(project_id, file_slug)
    table_name_query = "SELECT table_name from core_data_stores WHERE core_project_id=$1 AND slug=$2 LIMIT 1;"
    query_params = [project_id,file_slug]
    begin
      table_name = self.execute_query(@@rumi_io_db_identifier, table_name_query, query_params)
      return table_name.values[0][0]
    rescue
      return false 
    end 
  end

  def self.get_db_connection_id(project_id, filename)
    db_connection_query = "Select core_db_connection_id,table_name from core_datacasts WHERE core_project_id=$1 AND slug=$2"
    query_params = [project_id,filename]
    begin
      db_connection = self.execute_query(@@rumi_io_db_identifier, db_connection_query,query_params)
      return db_connection.values[0][0], db_connection.values[0][1]
    rescue => e
      return false 
    end 
  end

  def self.get_grid_table_name(username, projectname, filename)
    account_id = self.get_account_id_from_slug(username)
    if account_id
      project_id = self.get_project_id_from_slug(projectname, account_id)
    else 
      return false
    end
    if project_id
      table_name = self.get_table_name_from_slug(project_id, filename)
    else
      return false
    end
    return table_name
  end

  def self.get_human_from_token(project_id, token)
    query = "SELECT account_id from core_tokens WHERE core_project_id=$1 AND api_token=$2 LIMIT 1;"
    query_params = [project_id,token]
    begin
      token_validation_resp = self.execute_query(@@rumi_io_db_identifier, query, query_params)
      return token_validation_resp.values[0][0]
    rescue
      return false
    end
  end

  def self.authenticate_token(account_id, project_id, user_id)
    authenticate_token_query = "SELECT p.id FROM core_permissions p WHERE p.core_project_id = $1 AND p.status = 'Accepted' AND p.account_id = $2 LIMIT 1;"
    query_params = [project_id,user_id]
    begin
      token_validation_resp = self.execute_query(@@rumi_io_db_identifier, authenticate_token_query,query_params)
      return token_validation_resp.values[0][0]
    rescue
      return false
    end 
  end

  def self.execute_query(db_identifier, query, params)
    if !@@db_pools.has_key? db_identifier
      @@db_pools[db_identifier] = ConnPool.get_db_connection_pool(db_identifier, 5)
      return false if !@@db_pools[db_identifier]
    end

    begin
      conn = @@db_pools[db_identifier].checkout
      name = SecureRandom.hex(5)
      conn.prepare(name, query)
      data = conn.exec_prepared(name, params)
    rescue PG::UndefinedColumn => ee 

    rescue PG::UniqueViolation => ee

    rescue PG::InvalidTextRepresentation, PG::ObjectNotInPrerequisiteState => ee
      error = {error_type: "InvalidTextRepresentation"}
      return error
    ensure
      @@db_pools[db_identifier].checkin
    end
    data
  end

  def self.execute_custom_transaction(core_db_connection_id, query, query_params)
    core_db_connection_query = "Select adapter, (properties->'host') as host,(properties->'port') as port,(properties->'db_name') as db_name, (properties->'username') as username,(properties->'password') as password from core_db_connections where id=$1 LIMIT 1"
    data = self.execute_query(@@rumi_io_db_identifier, core_db_connection_query, [core_db_connection_id])
    data = data.first
    begin
      case data["adapter"]
      when "postgresql"
        conn = ConnPool.get_pg_connection(data)
      else
        raise "Not a valid adapter"
      end
      name = SecureRandom.hex(5)
      conn.prepare(name,query)
      if conn.exec_prepared(name, query_params)
        is_success = true
      else
        is_success = false
      end
    rescue Exception => e
      is_success = false
    ensure
      conn.close if conn.present?
    end
    is_success
  end

  def self.execute_custom_query(core_db_connection_id, query,query_params)
    core_db_connection_query = "Select adapter, (properties->'host') as host,(properties->'port') as port,(properties->'db_name') as db_name, (properties->'username') as username,(properties->'password') as password   from core_db_connections where id=$1 LIMIT 1"
    params = [core_db_connection_id]
    data = self.execute_query(@@rumi_io_db_identifier, core_db_connection_query,params)
    data = data.first
    begin
      case data["adapter"]
      when "postgresql"
        conn = ConnPool.get_pg_connection(data)
      else
        raise "Not a valid adapter"
      end
      begin
        name = SecureRandom.hex(6)
        conn.transaction do |c|
          c.prepare(name,query)
          data = c.exec_prepared(name,query_params)
        end
      rescue => e
        puts "rollback;"
      end
    rescue => ee
      error = {error_type: ee}
      return error
    ensure
      conn.close if conn.present?
    end
    data
  end


  def self.to_2d_array(object)
    # Convert PG::Result object to 2d array
    final_data = []
    begin
      final_data << object[0].keys 
    rescue IndexError => e
      return []
    end
    object.each do |row|
      final_data << row.values
    end
    return final_data
  end

  def self.to_json(object)
    # Convert PG::Result object to JSON
    final_data = []
    object.each do |row|
      final_data << row
    end
    return final_data.to_json
  end

  def self.to_csv(object)
    # Convert PG::Result object to CSV
    final_data = ""
    begin
      headers = object[0].keys
      final_data += headers.to_csv
      object.each do |row|
        row = row.values
        final_data += row.to_csv
      end
      final_data = final_data[0...-1] #removing the last extra \n
    rescue IndexError => e
      return ""
    end
    
    return final_data
  end

end