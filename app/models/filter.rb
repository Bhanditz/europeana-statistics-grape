require_relative '../lib/common_queries'
require_relative 'column'

class Filter

  def self.get_data(db_identifier, table_name, filter_config, response_format)

    if !filter_config.has_key? "mode"
      return {"error" => "No mode specified."}
    end
    mode = filter_config["mode"].strip

    column_types = Column.get_column_types(db_identifier, table_name)
    columns = column_types.keys

    # START -- SELECT COLUMN NAMES clause
    required_columns = []

    case mode
    when "aggregation"
      # Dimensions Part
      if filter_config.has_key?("dimensions")
        dimensions = filter_config["dimensions"]
        if dimensions and not dimensions.empty?
          if ((columns & dimensions).sort) != dimensions.sort # Dimension not found
            return {"error" => "dimension not found."}
          end
          # In Group By clauses - only columns used in group by clause and aggregation functions
          # on remaining columns is allowed. So, we don't concat dimensions to required_columns.
          required_columns = dimensions.dup
        end
      else
        dimensions = false
      end

      # Metrics Part
      if filter_config.has_key?("metrics")
        metrics = filter_config["metrics"]
        if metrics and not metrics.empty?
          if ((columns & metrics.keys).sort) != metrics.keys.sort # Metric not found
            return {"error" => "Metric not found."}
          end
          metrics.each do |column_name, aggregate_functions|
            if aggregate_functions and not aggregate_functions.empty?
              aggregate_functions.each do |f|
                if column_types[column_name] != "integer" and column_types[column_name] != "double"
                  return {"error" => "#{f} not allowed on #{column_name}(#{column_types[column_name]})."} if f != "count"
                end
                required_columns << "#{f}(#{column_name})"
              end
            end
          end
        end
      end

    when "select"
      if filter_config.has_key?("cols")
        select_cols = filter_config["cols"]
        if select_cols and not select_cols.empty?
          if columns & select_cols != select_cols # Column not found
            return {"error" => "Column not found."}
          end
          required_columns += select_cols
        end
      end

    when "unique"
      if filter_config.has_key?("cols")
        unique_cols = filter_config["cols"]
        if unique_cols and not unique_cols.empty?
          if columns & unique_cols != unique_cols # Column not found
            return {"error" => "Column not found."}
          end
          unique_cols.each do |column_name|
            required_columns << "DISTINCT(#{column_name})"
          end
        end
      end
    else
      return {"error" => "Mode '#{mode}' not supported."}
    end

    column_alias = {}

    if filter_config.has_key?("alias")
      config_alias = filter_config["alias"]
      config_alias.each do |col_name, col_alias|
        if col_alias.class == String
          column_alias["#{col_name}"] = col_alias
        else
          col_alias.each do |metric_name, metric_alias|
            if col_name.include? "("
              col_name.gsub! ")", ""
              col_name = col_name.split("(")
              col_name = col_name[1]
            end
            if metric_name == col_name
              column_alias["#{col_name}"] = metric_alias
            else
              col_name = metric_name + "(#{col_name})"
              column_alias["#{col_name}"] = metric_alias
            end
          end
        end
      end
    end

    required_columns = columns if !column_alias.empty? and required_columns.empty? and not dimensions
    if required_columns.empty? and not dimensions
      q = "SELECT "
      columns.each do |col_name|
        if column_types[col_name] == "date"
          q += " to_char(#{col_name}, 'YYYY-MM-DD') AS #{col_name},"
        else
          q += " #{col_name},"
        end
      end
      q = q[0..-2] + " "
    else
      q = "SELECT "
      required_columns.each_with_index do |col_name, index|
        if column_types[col_name] == "date"
          q += " to_char(#{col_name}, 'YYYY-MM-DD')"
        else
          q += col_name
        end
          q += " AS \"#{column_alias[col_name]}\" " if column_alias.include? col_name
          q += " AS \"#{col_name}\" " if column_types[col_name] == "date" and !column_alias.include? col_name
        q += index < required_columns.length - 1 ? ", " : " "
      end
    end

    # END -- SELECT COLUMN NAMES clause

    # START -- WHERE clause
    filter = filter_config.has_key?("filters") ? filter_config["filters"] : nil
    if filter and !filter.empty?
      where_clause = " WHERE " if filter.length > 0
      next_op = false
      filter.each do |f|
        f = f[1] if f[1].present?
        f = f[0] if f[0].present?
        where_clause += " #{next_op} " if next_op
        where_clause += " ( " if f.has_key?("group") and f["group"] == "true"
        f["condition_type"].downcase!

        case f["condition_type"]
        when "range"
          range_clause = self.get_range_where_query(f["column_name"], f["condition"])
          return {"error" => "Range condition for '#{f["column_name"]}' not in right format."} if !range_clause
          where_clause += range_clause
        when "values"
          where_clause += self.get_values_where_query(f["column_name"], f)
        when "datatype"
          col_name = f["column_name"]
          types = f["in"]
          where_clause += self.get_filter_by_type_clause(col_name, column_types[col_name], types)
        else
          return {"error" => "Wrong condition_type for filter."}
        end

        next_op = f.has_key?("next") ? f["next"] : false
        next_op = false if next_op == "false"
        where_clause += " ) " if f.has_key?("group") and  f["group"] == "false"

      end


      where_clause = where_clause.squeeze(" ") #remove extra whitespaces
      where_clause = where_clause.gsub "()", "" #remove empty where clauses
      where_clause = where_clause.gsub "( )", ""
      # TO-DO === sanitize sql query - security + bad illegal queries
    else
      where_clause = ""
    end

    # END -- WHERE clause

    # START -- GROUP BY clause
    if dimensions and not dimensions.empty?
      groupby_clause = "GROUP BY " + dimensions.join(", ")
    else
      groupby_clause = ""
    end
    # END -- GROUP BY clause

    # START -- ORDER BY clause
    if filter_config.has_key?("sort")
      sort_order = filter_config["sort"]
      if not sort_order.empty?
        orderby_clause = "ORDER BY "
        sort_order.each do |col_order|
          col_order = col_order[1] if col_order.length > 1 and col_order[1].class == Hashie::Mash
          col_order.each do |col_name, order|
            order.upcase!
            return {"error" => "Sort order can either be asc or desc."} if not ["ASC", "DESC"].include? order
            possible_sortable_columns = columns + column_alias.values
            return {"error" => "No such column '#{col_name}' as specified in sort condition"} if !possible_sortable_columns.include? col_name
            orderby_clause += "\"#{col_name}\" #{order}, "
          end
        end
        orderby_clause = orderby_clause[0...-2] + " "
      end
    elsif mode == "select"
      orderby_clause = "ORDER BY id ASC" if columns.include? "id"
    else
      orderby_clause = " "
    end
    # END -- ORDER clause
    if filter_config.has_key?("limit")
      limit = filter_config["limit"]
      return {"error" => "Limit should be in the range 1 to 2000."} if not limit.to_i.between?(1, 10000)
    else
      limit = 200
    end


    q += "FROM #{table_name} #{where_clause} #{groupby_clause} #{orderby_clause} "
    q += "LIMIT  #{limit} "
    q += "OFFSET #{filter_config["offset"]} " if filter_config.has_key?("offset")
    q += ";"
    q = q.squeeze(" ")
    p q

    final_data = CQ.execute_query(db_identifier ,q)

    case response_format
    when "array"
      final_data = CQ.to_2d_array(final_data)
    when "json"
      final_data = CQ.to_json(final_data)
    when "csv"
     final_data = CQ.to_csv(final_data)
    else
      return {"error" => "#{response_format} not supported."}
    end

    return final_data
  end


  private

  def self.get_range_where_query(column_name, conditions)
    conditions = [conditions] if conditions.class == Hash
    range_clause = column_name
    num_of_conditions = conditions.length
    return false if num_of_conditions < 1
    conditions.each_with_index do |cond, i|
      cond = cond[1] if cond[1].present?
      return false if !cond["min"] or !cond["max"]
      cond['min'] = cond['min'].to_f
      cond['max'] = cond['max'].to_f
      return false if cond['min'] >= cond['max']

      range_clause += " NOT " if cond["not"] == "true"
      range_clause += " BETWEEN #{cond["min"]} AND #{cond["max"]} "
      range_clause += " OR #{column_name} " if i < num_of_conditions - 1 #multiple ranges
    end
    return " ( " + range_clause + " ) "
  end

  def self.get_values_where_query(column_name, conditions)

    values_clause = column_name
    if conditions.has_key? "in" and !conditions["in"].empty?
      values_clause += " IN "
      conditions["in"].each_with_index { |x, i| conditions["in"][i] = "'#{x}'" }
      values_clause += "(" + conditions["in"].join(", ") + ")"
      in_flag = true
    end

    if conditions.has_key? "not in" and !conditions["not in"].empty?
      values_clause += "  AND " if in_flag
      values_clause += " NOT IN "
      conditions["not in"].each_with_index { |x, i| conditions["not in"][i] = "'#{x}'" }
      values_clause += "(" + f["not in"].join(", ") + ")"
    end

    return " ( " + values_clause + " ) "
  end

  def self.get_filter_by_type_clause(column_name, column_type, types)

    return " " if types.length < 1
    filter_by_type_clause = " "

    if ["integer", "double", "date", "boolean"].include? column_type
      column_type = "double" if column_type == "double precision"
      type = types & [column_type, 'blank'] #find whether the request is for numbers or blanks
      #to-do we need to add no where query if type.length == 2
      return " " if type.length != 1
      type = type[0]
      case type
      when column_type
        filter_by_type_clause += " #{column_name} IS NOT NULL"
      when "blank"
        filter_by_type_clause += " #{column_name} IS NULL"
      end

    elsif column_type == "string"
      types.each_with_index do |type, index|
        case type
          when "string"
            filter_by_type_clause += " #{column_name}  ~* '[a-zA-Z-]' AND #{column_name} NOT IN ('t', 'true', 'f', 'false', 'yes', 'no', 'y', 'n')"
          when "integer"
            filter_by_type_clause += " #{column_name} SIMILAR TO '[0-9]+'"
          when "double"
            filter_by_type_clause += " #{column_name} SIMILAR TO '[-+]?[0-9]*\.[0-9]+'"
          when "boolean"
            filter_by_type_clause += " #{column_name} IN ('t', 'true', 'f', 'false', 'yes', 'no', 'y', 'n')"
          when "blank"
            filter_by_type_clause += " #{column_name} IS NULL"
          when "date"
            filter_by_type_clause += " #{column_name} SIMILAR TO '[0-3]?[0-9](-|/)[0-1]?[0-9](-|/)[0-9]{4}' OR #{column_name} SIMILAR TO '[0-1]?[0-9](-|/)[0-3]?[0-9](-|/)[0-9]{4}' "
        end

        filter_by_type_clause += " OR " if index < types.length - 1
      end

    end
    return " ( " + filter_by_type_clause + " ) "

  end

end
