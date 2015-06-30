require 'pg'
require 'json'
require_relative '../lib/common_queries'
require_relative 'grid'

class Column

  def self.add(db_identifier, table_name, column_name, prev_col_name)
    columns = self.get_columns(db_identifier ,table_name)
    original_column_name = column_name.dup
    column_name = Grid.get_sql_compatible_column_name(column_name)
    
    return {"error" => "Column '#{column_name}' already exists."} if columns.include? column_name
    return {"error" => "No such column '#{prev_col_name}' as mentioned in after."} if prev_col_name and !columns.include? prev_col_name

    column_add_query = "ALTER TABLE #{table_name} ADD COLUMN #{column_name} character varying(255) DEFAULT NULL;"
    if !CQ.execute_query(db_identifier ,column_add_query)
      return {"error" => "Unable to add column '#{original_col_name}'"}
    end

    if prev_col_name
      prev_col_pos_query = "SELECT pos FROM column_meta WHERE table_name='#{table_name}' AND column_name='#{prev_col_name}';"
      prev_col_pos = CQ.execute_query(db_identifier, prev_col_pos_query)
      prev_col_pos = prev_col_pos[0]["pos"].to_i
      new_col_pos = prev_col_pos + 1
      update_col_pos_transaction = "UPDATE column_meta SET pos=t1_inc(pos) WHERE table_name='#{table_name}' AND pos > #{prev_col_pos};" 
      update_col_pos_transaction += "INSERT INTO column_meta (table_name, column_name, original_column_name, pos) VALUES ('#{table_name}', '#{column_name}', '#{original_column_name}', #{new_col_pos});"
    else
      last_col_pos_query = "SELECT max(pos) FROM column_meta WHERE table_name='#{table_name}';"
      last_col_pos = CQ.execute_query(db_identifier, last_col_pos_query)
      last_col_pos = last_col_pos.values[0][0].to_i
      new_col_pos = last_col_pos + 1
      update_col_pos_transaction = "INSERT INTO column_meta (table_name, column_name, original_column_name, pos) VALUES ('#{table_name}', '#{column_name}', '#{original_column_name}', #{new_col_pos});"
    end
    if CQ.execute_transaction("api_rumi", update_col_pos_transaction)
      return column_name
    else
      return {"error" => "Unable to add column '#{original_col_name}'"}
    end
  end

  def self.delete(db_identifier, table_name, column_name) 
    column_delete_query = "ALTER TABLE #{table_name} DROP COLUMN #{column_name};"
    if !CQ.execute_query(db_identifier, column_delete_query)
      puts "Unable to delete #{column_name}"
      return false
    end

    col_pos_query = "SELECT pos FROM column_meta WHERE table_name='#{table_name}' AND column_name='#{column_name}';"
    col_pos = CQ.execute_query(db_identifier, col_pos_query)
    col_pos = col_pos[0]["pos"]
    update_col_pos_transaction = "DELETE from column_meta WHERE table_name='#{table_name}' AND column_name='#{column_name}';"
    update_col_pos_transaction += "UPDATE column_meta SET pos=t1_dec(pos) WHERE table_name='#{table_name}' AND pos > #{col_pos};"
    CQ.execute_transaction("api_rumi", update_col_pos_transaction)
  end

  def self.move(db_identifier, table_name, column_name, prev_col_name)
    prev_col_pos_query = "SELECT pos FROM column_meta WHERE table_name='#{table_name}' AND column_name='#{prev_col_name}';"
    prev_col_pos = CQ.execute_query("api_rumi", prev_col_pos_query)

    if !prev_col_pos
      puts "No Column named #{column_name}"
      return false
    end
    prev_col_pos = prev_col_pos[0]["pos"].to_i
    new_col_pos = prev_col_pos + 1
    update_col_pos_transaction = "UPDATE column_meta SET pos=t1_inc(pos) WHERE table_name='#{table_name}' AND pos > #{prev_col_pos};" 
    update_col_pos_transaction += "UPDATE column_meta SET pos=#{new_col_pos} WHERE table_name='#{table_name}' AND column_name='#{column_name}';"
    CQ.execute_transaction("api_rumi", update_col_pos_transaction)
  end

  def self.change_name(db_identifier, table_name, column_name, new_original_column_name)
    columns = self.get_columns(db_identifier, table_name)
    if columns and !columns.include?column_name
      return {"error" => "No column named '#{column_name}'."}
    end

    new_original_column_name = new_original_column_name[0..255] if new_original_column_name.length > 255

    change_column_transaction = "UPDATE column_meta SET original_column_name='#{new_original_column_name}' WHERE table_name='#{table_name}' AND column_name='#{column_name}';"
    if CQ.execute_transaction("api_rumi", change_column_transaction)
      return new_original_column_name
    else
      return {"error" => "Failed to update original column name."}
    end
  end


  def self.change_type(db_identifier, table_name, column_name, new_type)
    original_column_type = Column.get_column_type(db_identifier, table_name, column_name)
    if original_column_type == new_type
      return false
    end
    change_type_query = ""
    if new_type != "string"
      change_type_query += "ALTER TABLE #{table_name} ALTER COLUMN #{column_name} DROP DEFAULT;" #drop default condition
    end

    change_type_query += "ALTER TABLE #{table_name} ALTER COLUMN #{column_name} TYPE "
    case new_type
    when "string"
      change_type_query += "character varying USING #{column_name}::character varying;"
    when "integer"
      change_type_query += "integer USING " 
      change_type_query += Column.to_integer(column_name, original_column_type)
    when "double"
      change_type_query += "double precision USING " 
      change_type_query += Column.to_float(column_name, original_column_type)
    when "boolean"
      change_type_query += "boolean USING " 
      change_type_query += Column.to_boolean(column_name, original_column_type)
    when "date"  
      date_format = "DD-MM-YYYY" #for now. We need to take date format later.
      change_type_query += "date USING to_date(#{column_name}, '#{date_format}');"
    else 
      return false
    end
    if CQ.execute_transaction(db_identifier, change_type_query) 
      if new_type == "integer" or new_type == "double"
        self.set_column_dimension_or_metric(db_identifier, table_name, column_name, 'm')
      else
        self.set_column_dimension_or_metric(db_identifier, table_name, column_name, 'd')
      end
    else
      return false
    end

  end

  def self.change_sub_type(db_identifier, table_name, column_name, new_sub_type)
    if not ['latitude', 'longitude', 'none'].include? new_sub_type
      return false
    end
    if new_sub_type == 'latitude' or new_sub_type == 'longitude'
      column_type = self.get_column_type(db_identifier, table_name, column_name)
      if column_type == "character varying"
        dtd = self.datatype_distribution(db_identifier, table_name, column_name)
        new_type = Column.detect_type(dtd)
        if new_type == "double" or new_type == "integer"
          column_type = "double precision" if self.change_type(db_identifier, table_name, column_name, new_type)
        else
          return false
        end
      end

      if column_type == "double precision" or column_type == "integer"
        sub_type = self.detect_lat_long_sub_type(db_identifier, table_name, column_name)
        return false if sub_type.length < 1
        if sub_type == 'longitude' and new_sub_type == 'latitude'
          #column has values in the range -180 to 180 not in range -90 to 90.
          return false
        end
      end 
    end
    new_sub_type = 'lng' if new_sub_type == "longitude"
    new_sub_type = 'lat' if new_sub_type == "latitude"
    new_sub_type = '' if new_sub_type == "none"
    if new_sub_type.empty?
      change_sub_type_query = "UPDATE column_meta SET sub_type = NULL WHERE table_name = '#{table_name}' AND column_name = '#{column_name}';"
    else
      change_sub_type_query = "UPDATE column_meta SET sub_type = '#{new_sub_type}' WHERE table_name = '#{table_name}' AND column_name = '#{column_name}';"
    end
    
    if CQ.execute_query("api_rumi", change_sub_type_query)
      if new_sub_type == "lat" or new_sub_type == "lng"
        self.set_column_dimension_or_metric(db_identifier, table_name, column_name, 'dimension')
      end
      return true
    else
      return false
    end

  end

  def self.uppercase(db_identifier, table_name, column_name)
    return false if self.get_column_type(db_identifier, table_name, column_name) != "character varying"
    uppercase_query = "UPDATE #{table_name} SET #{column_name}=upper(#{column_name});"
    CQ.execute_query(db_identifier, uppercase_query)
  end

  def self.lowercase(db_identifier, table_name, column_name)
    return false if self.get_column_type(db_identifier, table_name, column_name) != "character varying"
    lowercase_query = "UPDATE #{table_name} SET #{column_name}=lower(#{column_name});"
    CQ.execute_query(db_identifier, lowercase_query)
  end

  def self.titleize(db_identifier, table_name, column_name)
    return false if self.get_column_type(db_identifier, table_name, column_name) != "character varying"
    titleize_query = "UPDATE #{table_name} SET #{column_name}=initcap(#{column_name});"
    CQ.execute_query(db_identifier ,titleize_query)
  end

  def self.datatype_distribution(db_identifier, table_name, column_name)
    datatype_query = "SELECT data_type from information_schema.columns where table_name ='#{table_name}' and column_name='#{column_name}';"
    datatype = CQ.execute_query(db_identifier, datatype_query)
    
    datatype = datatype[0]["data_type"]
    total_count_query = "SELECT count(*) from #{table_name};"
    total_count = CQ.execute_query(db_identifier, total_count_query)
    total_count = total_count[0]["count"].to_i
    
    blank_query = "SELECT count(*) FROM #{table_name} WHERE #{column_name} IS NULL;"
    blank_count = CQ.execute_query(db_identifier, blank_query)
    blank_count = blank_count[0]["count"].to_i
    
    datatype_distribution = {}
    datatype_distribution["blank"] = blank_count if blank_count > 0

    case datatype
    when "boolean"
      boolean_count = total_count - blank_count
      datatype_distribution["boolean"] = boolean_count if boolean_count > 0
    when "double precision"
      float_count = total_count -  blank_count
      datatype_distribution["double"] = float_count if float_count > 0
    when "integer"
      integer_count = total_count - blank_count
      datatype_distribution["integer"] = integer_count if integer_count > 0
    when "date"
      date_count = total_count - blank_count
      datatype_distribution["date"] = date_count if date_count > 0-9
    when "character varying"
      boolean_query = "SELECT count(*) FROM #{table_name} WHERE lower(trim(#{column_name})) IN ('t', 'true', 'f', 'false', 'yes', 'no', 'y', 'n');"
      boolean_count = CQ.execute_query(db_identifier, boolean_query)
      boolean_count = boolean_count[0]["count"].to_i.to_i
      datatype_distribution["boolean"] = boolean_count if boolean_count > 0

      ##to-do -- check if boolean count + blank count == total count, so we can skip rest of the checks.

      float_query = "SELECT count(*) FROM #{table_name} WHERE #{column_name} SIMILAR TO '[-+]?[0-9]*\.[0-9]+';"
      float_count = CQ.execute_query(db_identifier, float_query)
      float_count = float_count[0]["count"].to_i
      datatype_distribution["double"] = float_count if float_count > 0

      integer_query = "SELECT count(*) FROM #{table_name} WHERE #{column_name} SIMILAR TO '[-+]?[0-9]+';"
      integer_count = CQ.execute_query(db_identifier, integer_query)
      integer_count = integer_count[0]["count"].to_i
      datatype_distribution["integer"] = integer_count if integer_count > 0

      date_query = "SELECT count(*) FROM #{table_name} WHERE " 
      date_query += "#{column_name} SIMILAR TO '[0-3]?[0-9](-|/)[0-1]?[0-9](-|/)[0-9]{4}'" #DD-MM-YYYY
      date_query += " OR #{column_name} SIMILAR TO '[0-1]?[0-9](-|/)[0-3]?[0-9](-|/)[0-9]{4}' ;" #MM-DD-YYYY
      date_count = CQ.execute_query(db_identifier, date_query)
      date_count = date_count[0]["count"].to_i
      datatype_distribution["date"] = date_count if date_count > 0

      string_count = total_count - (boolean_count + float_count + integer_count + blank_count + date_count)
      datatype_distribution["string"] = string_count if string_count > 0
    end
    datatype_distribution
  end

  def self.replace(db_identifier, table_name, column_name, values, new_value)
    column_name.downcase!
    replace_query = "UPDATE #{table_name} SET #{column_name} = '#{new_value}' WHERE #{column_name} IN ("
    values.each do |val| 
      replace_query += " '#{val}',"
    end
    replace_query = replace_query[0..-2] + ");"
    CQ.execute_query(db_identifier, replace_query)
  end

  def self.remove_character(db_identifier, table_name, column_name, value)
    column_name.downcase!
    column_type = self.get_column_type(db_identifier, table_name, column_name)
    if column_type != "character varying"
      return {"error" => "Remove character is supported only on textual columns."}
    end
    escape_chars = [".", "\\", "(", ")"]
    value = "\\" + value  if escape_chars.include? (value)
    remove_character_query = "UPDATE #{table_name} SET #{column_name} = regexp_replace(#{column_name}, '#{value}', '', 'g');"
    p remove_character_query
    if CQ.execute_query(db_identifier, remove_character_query)
      return true
    else
      return {"error" => "Remove character #{value} from #{column_name} failed."}
    end
  end

  def self.trim(db_identifier, table_name, column_name)
    trim_query = "UPDATE #{table_name} SET #{column_name}=trim(#{column_name});"
    CQ.execute_query(db_identifier, trim_query)
  end

  def self.trim_inside(db_identifier, table_name, column_name)
    trim_inside_query = "UPDATE #{table_name} SET #{column_name}=regexp_replace(#{column_name}, '[[:space:]]{2,}', ' ', 'g');"
    CQ.execute_query(db_identifier, trim_inside_query)
    # UPDATE table SET col = regexp_replace(col, '[[:space:]]{2,}', '', 'g')
  end

  def self.get_columns_data(db_identifier, table_name, data_types=false, original_names=false, sub_types=false)
    columns = {}
    if data_types
      column_types = self.get_column_types(db_identifier, table_name)
      columns["column_types"] = column_types
    end
    if original_names
      original_column_names = self.get_original_column_names(db_identifier, table_name)
      columns["original_column_names"] = original_column_names
    end
    if sub_types
      sub_types = self.get_column_sub_types(db_identifier, table_name)
      columns["sub_types"] = sub_types
    end
    columns
  end

  def self.get_dimension_and_metrics(db_identifier, table_name)
    d_and_m_query = "SELECT column_name, d_or_m FROM column_meta WHERE table_name = '#{table_name}';"
    query_result = CQ.execute_query("api_rumi", d_and_m_query)
    result = {dimensions: [], metrics: []}
    query_result.each do |col|
      if col["d_or_m"] == "d"
        result[:dimensions] << col["column_name"]
      elsif col["d_or_m"] == "m"
        result[:metrics] << col["column_name"]
      end
    end
    result
  end

  def self.set_column_dimension_or_metric(db_identifier, table_name, column_name, d_or_m)
    d_or_m = 'd' if d_or_m == "dimension"
    d_or_m = 'm' if d_or_m == "metric"
    if ['d', 'm'].include? d_or_m
      set_d_or_m_query = "UPDATE column_meta SET d_or_m = '#{d_or_m}' WHERE table_name = '#{table_name}' AND column_name = '#{column_name}';"
      if CQ.execute_query("api_rumi", set_d_or_m_query)
        return true
      else
        return false
      end
    else
      return false
    end 
  end

  def self.set_dimension_and_metrics(db_identifier, table_name, config)
    set_dim_and_met_query = ""
    if config.has_key? ("dimensions") and config["dimensions"]
      cols = config["dimensions"]
      cols.each do |col_name|
        set_dim_and_met_query += "UPDATE column_meta SET d_or_m = 'd' WHERE table_name = '#{table_name}' AND column_name = '#{col_name}';"
      end
    end

    if config.has_key? ("metrics") and config["metrics"]
      cols = config["metrics"]
      cols.each do |col_name|
        set_dim_and_met_query += "UPDATE column_meta SET d_or_m = 'm' WHERE table_name = '#{table_name}' AND column_name = '#{col_name}';"
      end
    end
    CQ.execute_transaction("api_rumi", set_dim_and_met_query)
  end

  private
  def self.get_column_type(db_identifier, table_name, column_name)
    get_column_type_query = "SELECT data_type from information_schema.columns where table_name ='#{table_name}' and column_name='#{column_name}';"
    column_type = CQ.execute_query(db_identifier, get_column_type_query)
    column_type = column_type[0]["data_type"]
    column_type
  end

  def self.detect_type(datatype_distribution)
    return false if not datatype_distribution
    return "string" if datatype_distribution.has_key?("string") and datatype_distribution["string"] > 0
    possible_types = datatype_distribution.keys
    return "double" if datatype_distribution.has_key?("double") and (possible_types & ["date", "boolean"]).length < 1 and datatype_distribution["double"] > 0
    return "integer" if datatype_distribution.has_key?("integer") and (possible_types & ["date", "boolean", "double"]).length < 1 and datatype_distribution["integer"] > 0
    return "boolean" if datatype_distribution.has_key?("boolean") and (possible_types & ["date", "double"]).length < 1 and datatype_distribution["boolean"] > 0
    return "date" if datatype_distribution.has_key?("date") and (possible_types & ["boolean", "double", "integer"]).length < 1 and datatype_distribution["date"] > 0
    return "string" #else - worst case scenario
  end

  def self.detect_lat_long_sub_type(db_identifier, table_name, column_name)
    count_query = "SELECT count(*) from #{table_name};"
    count = CQ.execute_query(db_identifier, count_query)
    count = count[0]["count"].to_i
    blank_query = "SELECT count(*) from #{table_name} WHERE #{column_name} IS NULL;"
    blank_count = CQ.execute_query(db_identifier, blank_query)
    blank_count = blank_count[0]["count"].to_i
    lat_query = "SELECT count(#{column_name}) from #{table_name} WHERE #{column_name} BETWEEN -90 and 90;"
    lat_count = CQ.execute_query(db_identifier, lat_query)
    lat_count = lat_count[0]["count"].to_i
    return "latitude" if lat_count + blank_count == count
    long_query = "SELECT count(#{column_name}) from #{table_name} WHERE #{column_name} BETWEEN -180 AND 180;"
    long_count = CQ.execute_query(db_identifier, long_query)
    long_count = long_count[0]["count"].to_i
    return "longitude" if long_count + blank_count == count
    return '' #else return nil
  end

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

  def self.get_column_types(db_identifier, table_name)
    #To-Do -- research whether a hash is order on insertion or not

    columns = self.get_columns(db_identifier ,table_name)
    columns_type_query = "SELECT column_name, data_type from information_schema.columns where table_name ='#{table_name}';"
    column_types_result = CQ.execute_query(db_identifier ,columns_type_query)
    column_types_result_hash = {}
    column_types_result.each do |col|
      col_name, col_data_type = col["column_name"], col["data_type"]
      case col_data_type
      when "character varying"
        col_data_type = "string"
      when "integer"
        col_data_type = "integer"
      when "double precision"
        col_data_type = "double"
      when "boolean"
        col_data_type = "boolean"
      when "date"
        col_data_type = "date"
      end
      column_types_result_hash["#{col_name}"] = col_data_type
    end

    column_types = {}
    columns.each do |col_name|
      ## Assumption - Ruby Hashes are ordered by insertion order since ruby 1.9.1. Need to verify
      column_types[col_name] = column_types_result_hash[col_name]
    end
    column_types
  end

  def self.get_original_column_names(db_identifier, table_name)
    column_original_names_query = "SELECT column_name, original_column_name from column_meta where table_name = '#{table_name}' ORDER BY pos ASC;"
    query_result = CQ.execute_query("api_rumi", column_original_names_query)
    original_column_names = {}
    query_result.each do |col|
      col_name, original_col_name = col["column_name"], col["original_column_name"]
      original_column_names["#{col_name}"] = original_col_name
    end
    original_column_names
  end

  def self.get_column_sub_types(db_identifier, table_name)
    column_sub_types_query = "SELECT column_name, sub_type from column_meta where table_name = '#{table_name}' ORDER BY pos ASC;"
    query_result = CQ.execute_query("api_rumi", column_sub_types_query)
    column_sub_types = {}
    query_result.each do |col|
      col_name, sub_type = col["column_name"], col["sub_type"]
      sub_type = "latitude" if sub_type == "lat"
      sub_type = "longitude" if sub_type == "lng"
      column_sub_types["#{col_name}"] = sub_type
    end
    column_sub_types
  end

  def self.to_integer(column_name, old_type)
    case old_type
    when "character varying"
      return "str_to_int(#{column_name});"
    when "double precision"
      return "#{column_name}::integer;"
    when "boolean"

    when "date"
    
    else 
      return false
    end
  end

  def self.to_float(column_name, old_type)
    case old_type
    when "character varying"
      return "str_to_float(#{column_name});"
    when "integer"
      return "#{column_name}::double precision;"
    when "boolean"

    when "date"
    
    else
      return false
    end
  end

  def self.to_boolean(column_name, old_type)
    case old_type
    when "character varying"
      return "str_to_boolean(#{column_name});"
    when "double precision"
      
    when "integer"

    when "date"
      
    end
  end
end

