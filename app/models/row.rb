# frozen_string_literal: true
require 'pg'
require 'json'
require_relative '../lib/common_queries'

class Row
  def self.batch_add(core_db_connection_id, table_name, grid_data)
    return false if grid_data.empty?
    column_names = Column.get_columns(core_db_connection_id, table_name)

    insert_rows_query = "INSERT INTO #{table_name} "
    count = 1
    query_params = []
    column_names.shift # remove id
    if column_names
      insert_rows_query += ' ( '
      column_names.each do |col|
        insert_rows_query += " #{col},"
      end
      insert_rows_query = insert_rows_query[0..-2] + ') VALUES '
    end

    grid_data.each do |row|
      insert_rows_query += ' ('
      row.each do |cell_value|
        cell_value = cell_value[0...254] if cell_value && cell_value.length > 254
        cell_value.gsub! "'", "''" if cell_value && cell_value.include?("'")
        insert_rows_query += " $#{count},"
        query_params << if (!cell_value) || cell_value.empty?
          " NULL,"
        else
          " '#{cell_value}',"
                        end
        count += 1
      end
      insert_rows_query = insert_rows_query[0..-2] + '),'
    end
    insert_rows_query = insert_rows_query[0..-2] + ';'
    CQ.execute_custom_query(core_db_connection_id, insert_rows_query, query_params)
  end
end
