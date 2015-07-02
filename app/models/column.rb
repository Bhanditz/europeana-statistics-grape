require 'pg'
require 'json'
require_relative '../lib/common_queries'
require_relative 'grid'

class Column

  private
  
  def self.get_columns(db_identifier, table_name)
    column_names_query = "SELECT column_name from column_meta where table_name = '#{table_name}' ORDER BY pos ASC;"
    column_names = CQ.execute_query("api_rumi", column_names_query)
    columns = []
    column_names.each do |x|
      columns << x["column_name"]
    end
    if columns.empty?
      column_names_query = "SELECT column_name from information_schema.columns where table_name ='#{table_name}';"
      column_names = CQ.execute_query(db_identifier, column_names_query)
      columns = []
      column_names.each do |x|
        columns << x["column_name"]
      end
    end
    columns
  end
end

