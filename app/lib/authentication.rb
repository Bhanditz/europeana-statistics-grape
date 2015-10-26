require_relative 'common_queries'

class Authentication

  def self.sudo_project_member!(username, projectname, filename, token)
    if token.blank?
      return {error_msg: "[rumi-api] Token cannot be blank."}
    end
    account_id = CQ.get_account_id_from_slug(username)
    if !account_id
      return {error_msg: "[rumi-api] AccountID not found."}
    end
    project_id = CQ.get_project_id_from_slug(projectname, account_id)
    if !project_id
      return {error_msg: "[rumi-api] ProjectID not found."}
    end
    user_id = CQ.get_human_from_token(project_id, token)
    if !user_id
      return {error_msg: "[rumi-api] Invalid token."}
    end
    token_validation = CQ.authenticate_token(account_id, project_id, user_id)
    if !token_validation
      return {error_msg: "[rumi-api] Unauthorized."}
    end
    core_db_connection_id, table_name = CQ.get_db_connection_id(project_id,filename)
    if !core_db_connection_id and !table_name
      return {error_msg: "[rumi-api] Datacast not found"}
    end
    return {core_db_connection_id: core_db_connection_id,table_name: table_name}
  end

end