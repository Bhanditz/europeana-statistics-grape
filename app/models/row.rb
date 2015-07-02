require 'pg'
require 'json'
require_relative '../lib/common_queries'

class Row

	def self.add(db_identifier, table_name, grid_data)
    
    return false if grid_data.empty?

    column_names = Column.get_columns(db_identifier, table_name)
    insert_rows_query = "INSERT INTO #{table_name}"
    column_names.shift #remove id 
    if column_names
      insert_rows_query += " ( "
      column_names.each do |col|
        insert_rows_query += " #{col},"
      end
      insert_rows_query = insert_rows_query[0..-2] + ") VALUES "
    end
    grid_data.shift
    if grid_data
      insert_rows_query += "(" 
      grid_data.each do |cell_value|
        cell_value = cell_value[0...254] if cell_value and cell_value.length > 254
        cell_value.gsub! "'", "''" if cell_value and cell_value.include?"'"
        if not cell_value or cell_value.empty?
          insert_rows_query += " NULL," 
        else
          insert_rows_query += " '#{cell_value}'," 
        end
      end
      insert_rows_query = insert_rows_query[0..-2] + ");"
    end
    query_success = CQ.execute_query(db_identifier ,insert_rows_query)
    
    if query_success.class == Hash and query_success.has_key?(:error_type)
      return query_success
    end
    
    last_id = CQ.execute_query(db_identifier, "SELECT currval('#{table_name}_id_seq');")
    last_id.to_a[0]["currval"]  
  end

  def self.delete(db_identifier, table_name, row_ids)
    #to-do -- bulk delete
    row_ids.each do |id|
      delete_query = "DELETE FROM #{table_name} WHERE id=#{id};"
      CQ.execute_query(db_identifier, delete_query)
    end
  end


  def self.batch_add(db_identifier, table_name, grid_data)
    return false if grid_data.empty?
    column_names = Column.get_columns(db_identifier, table_name)
    
    insert_rows_query = "INSERT INTO #{table_name}"
    column_names.shift #remove id 
    if column_names
      insert_rows_query += " ( "
      column_names.each do |col|
        insert_rows_query += " #{col},"
      end
      insert_rows_query = insert_rows_query[0..-2] + ") VALUES "
    end

    grid_data.each do |row|
      insert_rows_query += " (" 
      row.each do |cell_value|
        cell_value = cell_value[0...254] if cell_value and cell_value.length > 254
        cell_value.gsub! "'", "''" if cell_value and cell_value.include?"'"
        if not cell_value or cell_value.empty?
          insert_rows_query += " NULL," 
        else
          insert_rows_query += " '#{cell_value}'," 
        end
      end
      insert_rows_query = insert_rows_query[0..-2] + "),"
    end
    insert_rows_query = insert_rows_query[0..-2] + ";"
    CQ.execute_query(db_identifier, insert_rows_query)
  end

end

