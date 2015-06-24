require 'pg'
require 'json'
require_relative '../lib/common_queries'

class Cell

  def self.update(db_identifier ,table_name, id, column_name, new_value)

    new_value.gsub! "'", "\\'" # escape apostrophe 
    if new_value and new_value.empty?
      update_cell_query = "UPDATE #{table_name} SET #{column_name} = NULL WHERE id = #{id};"
    else
      new_value = new_value[0...254] if new_value.length > 255
      update_cell_query = "UPDATE #{table_name} SET #{column_name} = '#{new_value}' WHERE id = #{id};"
    end
    query_result = CQ.execute_query(db_identifier, update_cell_query)
    if query_result.class == Hash and query_result.has_key?(:error_type)
      return query_result
    else
      return true
    end
  end

end

