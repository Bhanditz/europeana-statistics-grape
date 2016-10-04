# frozen_string_literal: true
require 'pg'
require 'json'
require_relative '../lib/common_queries'
require_relative 'grid'

class Datacast
  private

  def self.get_datacast_output(datacast_identifier)
    if !datacast_identifier
      return nil
    else
      core_db_output_query = 'SELECT output from core_datacast_outputs where datacast_identifier=$1 LIMIT 1'
      params = [datacast_identifier]
      response = CQ.execute_query('datahub', core_db_output_query, params)
      if response.first.present?
        data = response.first
        return JSON.parse(data['output'])
      else
        return nil
      end
    end
  end

  def self.get_column_meta(datacast_identifier)
    if !datacast_identifier
      return nil
    else
      core_db_column_meta_query = 'SELECT column_properties from core_datacasts where identifier=$1 LIMIT 1'
      params = [datacast_identifier]
      response = CQ.execute_query('datahub', core_db_column_meta_query, params)
      data = {}
      if response.first.present?
        col_properties = JSON.parse(response.first['column_properties'])
        data[:column_names] = Datacast.get_column_names_and_datatypes(col_properties)
        data[:dimensions] = Datacast.get_column_properties(col_properties, 'dimensions')
        data[:metrics] = Datacast.get_column_properties(col_properties, 'metrics')
        return data
      else
        return nil
      end
    end
  end

  def self.get_column_names_and_datatypes(col_properties)
    column_properties = {}
    col_properties.each do |k, v|
      column_properties[k] = v['data_type']
    end
    column_properties
  end

  def self.get_column_properties(col_properties, property)
    condition = property == 'dimensions' ? 'd' : 'm'
    p = []
    col_properties.each { |k, v| p << k if v['d_or_m'] == condition }
    p
  end
end
