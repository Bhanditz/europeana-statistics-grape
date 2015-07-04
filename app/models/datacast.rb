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
      core_db_output_query = "SELECT output from core_datacast_outputs where datacast_identifier='#{datacast_identifier}' LIMIT 1"
      response = CQ.execute_query("datahub", core_db_output_query)
      if response.first.present?
        data = response.first
        return data["output"]
      else
        return nil
      end
    end
  end

end

