require 'pg'
require 'json'
require 'set'
require_relative '../lib/common_queries'

class Grid

  # 77 reserved keywords not allowed for postgresql column names
  # Ref - http://www.postgresql.org/docs/current/static/sql-keywords-appendix.html
  @@reserved_keywords = ["all", "analyse", "analyze", "and", "any", "array", "as", "asc", "asymmetric", "both", "case", "cast", "check", "collate", "column", "constraint", "create", "current_catalog", "current_date", "current_role", "current_time", "current_timestamp", "current_user", "default", "deferrable", "desc", "distinct", "do", "else", "end", "except", "false", "fetch", "for", "foreign", "from", "grant", "group", "having", "in", "initially", "intersect", "into", "lateral", "leading", "limit", "localtime", "localtimestamp", "not", "null", "offset", "on", "only", "or", "order", "placing", "primary", "references", "returning", "select", "session_user", "some", "symmetric", "table", "then", "to", "trailing", "true", "union", "unique", "user", "using", "variadic", "when", "where", "window", "with"]
  
  # some more additional
  @@reserved_keywords += ["alter", "drop", "copy", "delete", "insert", "id", "date"]

	def self.create(core_db_connection_id, table_name, grid_data, first_row_header=true)
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

    Grid.create_table(core_db_connection_id, table_name, grid_data[0], provided_headers) unless grid_data.length < 1 and table_name.is_empty?
    Grid.insert_values(core_db_connection_id, table_name, grid_data)
    return true
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

  def self.delete(core_db_connection_id, table_name)
    is_table_query = "SELECT count(*) FROM pg_tables where tablename='#{table_name}';"
    is_table = CQ.execute_custom_query(core_db_connection_id, is_table_query)
    is_table = is_table.values
    is_table = is_table[0][0].to_i
    if is_table == 1
      drop_table_query = "DROP TABLE #{table_name};"
      p drop_table_query
      CQ.execute_custom_transaction(core_db_connection_id, drop_table_query)
      return true;
    end

    is_view_query = "SELECT count(*) FROM pg_views where viewname='#{table_name}';"
    is_view = CQ.execute_custom_query(core_db_connection_id, is_view_query)
    is_view = is_view.values
    is_view = is_view[0][0].to_i
    if is_view == 1
      drop_view_query = "DROP VIEW #{table_ename};"
      CQ.execute_custom_transaction(core_db_connection_id, drop_view_query)
    end
  
  end

  private

  def self.create_table(core_db_connection_id, table_name, headers, provided_headers)

    create_table_query = "CREATE TABLE " + table_name + "("
    create_table_query += " id SERIAL PRIMARY KEY,"
    
    headers.each_with_index do |column_name, pos|     
      create_table_query += " #{column_name} character varying(255) DEFAULT NULL,"
    end
    create_table_query = create_table_query[0..-2] + ");"
    CQ.execute_custom_transaction(core_db_connection_id, create_table_query)
  end

  def self.insert_values(core_db_connection_id, table_name, grid_data)
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
    CQ.execute_custom_transaction(core_db_connection_id, insert_rows_query)
  end
end