# frozen_string_literal: true
require 'pg'
require 'json'
require_relative '../lib/common_queries'
require_relative 'grid'

class Column
  private

  def self.get_columns(core_db_connection_id, table_name)
    column_names_query = query_get_all_columns
    column_names = CQ.execute_custom_query(core_db_connection_id, column_names_query, [table_name])
    return false if column_names.class == Hash && column_names.key?('error_type')
    columns = []
    column_names.each do |x|
      columns << x['column_name']
    end
    columns
  end

  def self.query_get_all_columns
    'SELECT column_name from information_schema.columns where table_name=$1;'
  end
end
