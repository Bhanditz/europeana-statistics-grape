require 'pg'
require 'json'
require 'set'
require_relative "data_store"
require_relative '../lib/common_queries'

class Grid

  # 77 reserved keywords not allowed for postgresql column names
  # Ref - http://www.postgresql.org/docs/current/static/sql-keywords-appendix.html
  @@reserved_keywords = ["all", "analyse", "analyze", "and", "any", "array", "as", "asc", "asymmetric", "both", "case", "cast", "check", "collate", "column", "constraint", "create", "current_catalog", "current_date", "current_role", "current_time", "current_timestamp", "current_user", "default", "deferrable", "desc", "distinct", "do", "else", "end", "except", "false", "fetch", "for", "foreign", "from", "grant", "group", "having", "in", "initially", "intersect", "into", "lateral", "leading", "limit", "localtime", "localtimestamp", "not", "null", "offset", "on", "only", "or", "order", "placing", "primary", "references", "returning", "select", "session_user", "some", "symmetric", "table", "then", "to", "trailing", "true", "union", "unique", "user", "using", "variadic", "when", "where", "window", "with"]
  
  # some more additional
  @@reserved_keywords += ["alter", "drop", "copy", "delete", "insert", "id", "date"]

	def self.create(db_identifier, table_name, grid_data, first_row_header=true)
   if not first_row_header
      headers = []
      grid_data[0].length.times { |i| headers << "column_#{i}" } 
      grid_data.unshift(headers)
      provided_headers = headers
    else
      headers = grid_data[0]
      
      provided_headers = []
      headers.each_with_index do |column_name, pos|
        if column_name
          provided_headers << column_name.dup #dup and clone doesn't do a deep copy.
        else
          provided_headers << "" #dup fails on nil value for column name
        end

        column_name = self.get_sql_compatible_column_name(column_name)
        headers[pos] = column_name
        #check for duplicate column names
        if headers.index(column_name) != headers.rindex(column_name)
          headers[pos] += "_" + rand(100).to_s
        end
      end
    end

    Grid.create_table(db_identifier, table_name, grid_data[0], provided_headers) unless grid_data.length < 1 and table_name.is_empty?
    Grid.insert_values(db_identifier, table_name, grid_data)
    # return false if not DataStore.set_table_name(username, projectname, filename, table_name)
    return true
  end

  def self.get_columns(db_identifier, table_name)
    column_order_query = "SELECT column_name from column_meta WHERE table_name='#{table_name}' order by pos;"
    column_order = CQ.execute_query(db_identifier ,column_order_query)
    column_names = []
    column_order.each do |x|
      column_names << x["column_name"].strip
    end

    column_type_query = "SELECT column_name, data_type from information_schema.columns WHERE table_name = '#{table_name}';"
    column_type = CQ.execute_query(db_identifier, column_type_query)
    column_types = []
    
    column_type.each do |a|
      a = a.values
      col_name, col_type = a[0], a[1]
      index = column_names.index(col_name)
    
      case col_type
      when "character varying"
        column_types[index] = "string"
      when "integer"
        column_types[index] = "integer"
      when "double precision"
        column_types[index] = "double"
      when "boolean"
        column_types[index] = "boolean"
      when "date"
        column_types[index] = "date"
      end
      
    end
    return column_names, column_types
  end

  def self.get_sql_compatible_column_name(column_name)
    # http://www.postgresql.org/docs/current/static/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
    #ordinal range for allowed characters:
    # a-z -- 97-122
    # A-Z -- 65-90
    # _ -- 95

    if not column_name or column_name == ""
      column_name = "undefined_column_" + rand(100).to_s
      return column_name
    end

    column_name.strip!
    column_name.squeeze!(" ") #remove consecutive whitespaces.
    column_name.downcase!
    column_name.gsub! /"/, ''

    if @@reserved_keywords.include?column_name
      column_name = "_" + column_name
      return column_name
    end

    allowed_ordinal_range = [95]
    allowed_ordinal_range += Array(97..122) #a-z
    allowed_ordinal_range += Array(65..90)  #A-Z
    
    #Make sure first character is an _ or letter
    column_name[0] = "_" + column_name[0] if not allowed_ordinal_range.include?column_name[0].ord

    #Rest of the characters have to be letters, digits or _
    allowed_ordinal_range += Array(48..57)  #0-9
    column_name.each_char.with_index do |char, i|
      if not allowed_ordinal_range.include?char.ord
        column_name[i] = "_"
      end
    end
    column_name
  end

  def self.delete(db_identifier, table_name)
    is_table_query = "SELECT count(*) FROM pg_tables where tablename='#{table_name}';"
    is_table = CQ.execute_query(db_identifier, is_table_query)
    is_table = is_table.values
    is_table = is_table[0][0].to_i
    if is_table == 1
      drop_table_query = "DROP TABLE #{table_name};"
      p drop_table_query
      CQ.execute_query(db_identifier, drop_table_query)
      delete_columns_meta_query = "DELETE FROM column_meta WHERE table_name='#{table_name}';"
      p delete_columns_meta_query
      CQ.execute_transaction(db_identifier, delete_columns_meta_query)
      return true;
    end

    is_view_query = "SELECT count(*) FROM pg_views where viewname='#{table_name}';"
    is_view = CQ.execute_query(db_identifier, is_view_query)
    is_view = is_view.values
    is_view = is_view[0][0].to_i
    if is_view == 1
      drop_view_query = "DROP VIEW #{table_ename};"
      CQ.execute_query(db_identifier, drop_view_query)
    end
  
  end

  def self.analyse_datatypes(db_identifier, table_name)
    columns_with_type = Column.get_column_types(db_identifier, table_name)
    columns = columns_with_type.keys
    columns.delete('id')
    columns.each do |col_name|
      dtd = Column.datatype_distribution(db_identifier, table_name, col_name)
      col_type = Column.detect_type(dtd)
      Column.change_type(db_identifier, table_name, col_name, col_type) if col_type and col_type != "string" and columns_with_type[col_name] != col_type
      if ['lat', 'latitude', 'long', 'lng', 'longitude'].include? col_name.downcase
        sub_type = Column.detect_lat_long_sub_type(db_identifier, table_name, col_name)
        sub_type = 'longitude' if sub_type == 'latitude' and ['long', 'lng', 'longitude'].include? col_name.downcase
        Column.change_sub_type(db_identifier, table_name, col_name, sub_type) if sub_type.length > 1
      end
    end
    table_size = Grid.calculate_size(db_identifier, table_name)
    DataStore.update_size("datahub", table_name, table_size)
  end

  def self.get_graph_traversal(db_identifier, table_name ,source_column, target_column, depth=1)
    node_ids = Set.new
    node_ids.add('214328887')
    (0...depth).each do |i|
      node_ids_list = "("
      node_ids.each do |node|
        node_ids_list += "#{node}, "
      end
      node_ids_list = node_ids_list[0..-3] +  ")"
      q = "SELECT source, target FROM #{table_name} WHERE #{source_column} IN #{node_ids_list} OR #{target_column} IN #{node_ids_list} LIMIT 1000;"
      more_node_ids = CQ.execute_query(db_identifier, q)
      more_node_ids = more_node_ids.values
      more_node_ids.each do |s, t|
        node_ids.merge([s, t])
      end
    end
    node_ids_list = "("
    node_ids.each do |node|
      node_ids_list += "#{node}, "
    end
    node_ids_list = node_ids_list[0..-3] +  ")"
    q = "SELECT source, target FROM #{table_name} WHERE #{source_column} IN #{node_ids_list} OR #{target_column} IN #{node_ids_list} LIMIT 1000;"
    final_relations = CQ.execute_query(db_identifier, q)
    final_relations = final_relations.values
  end

  private

  def self.create_table(db_identifier, table_name, headers, provided_headers)

    create_table_query = "CREATE TABLE " + table_name + "("
    create_table_query += " id SERIAL PRIMARY KEY,"
    column_order_query = "INSERT into column_meta (table_name, column_name, original_column_name, pos) VALUES ('#{table_name}', 'id', 'id', 0)," 
    
    headers.each_with_index do |column_name, pos|     
      create_table_query += " #{column_name} character varying(255) DEFAULT NULL,"
      column_order_query += " ('#{table_name}', '#{column_name}', '#{provided_headers[pos]}', #{pos + 1}),"
    end
    create_table_query = create_table_query[0..-2] + ");"
    column_order_query = column_order_query[0..-2] + ";"
    create_table_and_column_order_transaction = create_table_query + column_order_query
    CQ.execute_transaction(db_identifier, create_table_and_column_order_transaction)
  end

  def self.insert_values(db_identifier, table_name, grid_data)
    insert_rows_query = "INSERT INTO " + table_name + "("
    headers = grid_data[0]
    headers.each do |column_name|
      insert_rows_query += " #{column_name},"
    end
    insert_rows_query = insert_rows_query[0..-2] + ")" + " VALUES "
    grid_data.shift
    grid_data.each do |row|
      row = row[0...headers.length]
      if row.length == headers.length
        insert_rows_query += "("
        row.each do |value|
          value = value[0...254] if value and value.length > 255
          value.gsub! "'", "''" if value and value.include?"'"
          if not value or value.empty?
            insert_rows_query += " NULL," 
          else
            insert_rows_query += " '#{value}'," 
          end
        end
        insert_rows_query = insert_rows_query[0..-2] + "),"
      end
      
    end
    insert_rows_query = insert_rows_query[0..-2] + ";"
    if insert_rows_query[-2] == ","
      insert_rows_query = insert_rows_query[0..-3] + ";"
    end
    CQ.execute_query(db_identifier, insert_rows_query)
  end

  def self.convert_sqlresults_hash_to_array(sql_results_hash)
    sql_results_array = []
    begin
      sql_results_array << sql_results_hash[0].keys 
    rescue IndexError => e
      return []
    end
    sql_results_hash.each do |row|
      sql_results_array << row.values
    end
    sql_results_array
  end

  def self.calculate_size(db_identifier, table_name)
    table_size_query = "SELECT pg_size_pretty(pg_total_relation_size('#{table_name}'));"
    table_size = CQ.execute_query(db_identifier, table_size_query)
    table_size = table_size[0]["pg_size_pretty"]
    table_size
  end
end