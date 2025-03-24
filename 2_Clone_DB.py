import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session
from modules.connector import create_session
import time
import re

# PAGE CONFIG
st.set_page_config(layout="wide", page_title="Clone DB")

# CREATE A SESSION VARIABLE
try:
    session = get_active_session()
except Exception as err:
    session = create_session()

st.header("Clone DB", divider="orange")

def get_current_role(session):
    result = session.sql("SELECT CURRENT_ROLE()").collect()
    return result[0][0]

def update_role_display():
    current_role = get_current_role(session)
    st.session_state.current_role = current_role

@st.cache_data
def get_databases():
    sql_get_db = """
    select DATABASE_NAME 
    from BALANCING_TOOL.INFORMATION_SCHEMA.DATABASES 
    where DATABASE_NAME like '____%\_DB\_DH'
    order by DATABASE_NAME
    """
    result_db_df = pd.DataFrame(session.sql(sql_get_db).collect())
    return result_db_df

#@st.cache_data
def check_db_exists_fun(db):
    sql_check_db_exists = """
    select DATABASE_NAME 
    from BALANCING_TOOL.INFORMATION_SCHEMA.DATABASES 
    where DATABASE_NAME = '{database}'
    """
    sql_check_db_exists = sql_check_db_exists.format(database=db)
    result_check_db = pd.DataFrame(session.sql(sql_check_db_exists).collect())
    return result_check_db

def get_cur_db_env(db_name):
    env = db_name[int(3)]
    if env == 'D':
        cur_env = 'Dev'
    elif env == 'Q':
        cur_env = 'QA'
    elif env == 'C':
        cur_env = 'CAT'
    elif env == 'P':
        cur_env = 'Prod'
    return cur_env

def new_db_name(cur_env, db_name):
    if cur_env == 'Dev':
        new_db_name = db_name[:3] + 'D' + db_name[4:]
    elif cur_env == 'QA':
        new_db_name = db_name[:3] + 'Q' + db_name[4:]
    elif cur_env == 'CAT':
        new_db_name = db_name[:3] + 'C' + db_name[4:]
    return new_db_name

def new_db_ver(db_name):
    cur_version = db_name[int(6)]
    new_version = int(cur_version) + 1 
    new_db_name = db_name[:6] + str(new_version) + db_name[7:]
    return new_db_name

def new_db_env_id(db_name):
    cur_env = db_name[int(4)]
    new_env = int(cur_env) + 1 
    new_db_name = db_name[:4] + str(new_env) + db_name[5:]
    return new_db_name

def set_db_target_name(clone_type, db_name, clone_new_env):
    if clone_new_env == 'Y':
        db_name=new_db_env_id(db_name)
    if clone_type.startswith('New'):
        return new_db_ver(db_name)
    else:
        return new_db_name(clone_type, db_name)

def switch_role(role_name):
    session.use_role(role_name)
    update_role_display()

def switch_database(target_db):
    session.use_database(target_db)

def clone_database(source_db, target_db):
    switch_role("SYSADMIN")
    sql_clone_db = f"CREATE OR REPLACE DATABASE {target_db} CLONE {source_db}"
    session.sql(sql_clone_db).collect()

def create_stage(target_db):
    switch_role("SYSADMIN")
    switch_database(target_db)
    session.sql("CREATE STAGE DATA_INGRESS.SNOWFLAKE_INTERNAL DIRECTORY = ( ENABLE = true )").collect()

def setup_roles(env_code, site_abbr, version):
    switch_role("SYSADMIN")
    if env_code == 'P' or env_code == 'C':
        sql_statements = [
            f"CREATE ROLE IF NOT EXISTS {site_abbr}{env_code}X{version}_RE_SYSADMIN",
            f"GRANT ROLE {site_abbr}{env_code}X{version}_RE_SYSADMIN TO ROLE SYSADMIN",
            f"GRANT ROLE LPC{env_code}1V1_RA_ENVSETUP TO ROLE {site_abbr}{env_code}X{version}_RE_SYSADMIN",
            f"GRANT CREATE DATABASE ON ACCOUNT TO ROLE {site_abbr}{env_code}X{version}_RE_SYSADMIN",
            f"GRANT USAGE, MONITOR ON WAREHOUSE LPCPXV1_WH_ETL TO ROLE {site_abbr}{env_code}X{version}_RE_SYSADMIN WITH GRANT OPTION",
            f"GRANT USAGE, MONITOR ON WAREHOUSE LPCPXV1_WH_LDI TO ROLE {site_abbr}{env_code}X{version}_RE_SYSADMIN WITH GRANT OPTION",
            f"GRANT USAGE, MONITOR ON WAREHOUSE LPCPXV1_WH_SUPPORT TO ROLE {site_abbr}{env_code}X{version}_RE_SYSADMIN WITH GRANT OPTION"
        ]
    elif env_code == 'D' or env_code == 'Q':
        sql_statements = [
            f"CREATE ROLE IF NOT EXISTS {site_abbr}{env_code}X{version}_RE_SYSADMIN",
            f"GRANT ROLE {site_abbr}{env_code}X{version}_RE_SYSADMIN TO ROLE SYSADMIN",
            f"GRANT ROLE LPC{env_code}1V1_RA_ENVSETUP TO ROLE {site_abbr}{env_code}X{version}_RE_SYSADMIN",
            f"GRANT CREATE DATABASE ON ACCOUNT TO ROLE {site_abbr}{env_code}X{version}_RE_SYSADMIN",
            f"GRANT USAGE, MONITOR ON WAREHOUSE LPCDXV1_WH_LDI TO ROLE {site_abbr}{env_code}X{version}_RE_SYSADMIN WITH GRANT OPTION"
        ]

    # Switch to USERADMIN role
    switch_role("USERADMIN")
    for sql in sql_statements[:3]:
        session.sql(sql).collect()

    # Switch to SYSADMIN role
    switch_role("SYSADMIN")
    for sql in sql_statements[3:]:
        session.sql(sql).collect()

def setup_privileges(target_db, site_abbr, env_code, env_id, version, category):
    sql_statements = [
        f"CALL LPC{env_code}{env_id}V1_DB_LDI.SETUP.ROLES('{site_abbr}','{env_code}{env_id}','{version}','CREATE','{category}')",
        f"CALL LPC{env_code}{env_id}V1_DB_LDI.SETUP.ROLES_TO_ROLE('{site_abbr}','{env_code}{env_id}','{version}','GRANT','{category}')",
        f"CALL LPC{env_code}{env_id}V1_DB_LDI.SETUP.CHANGE_OWNERSHIP('{target_db}', '{site_abbr}{env_code}{env_id}{version}_RU_DBADMIN', 'REVOKE')",
        f"CALL LPC{env_code}{env_id}V1_DB_LDI.SETUP.PRIVILEGES_TO_ROLE('{target_db}','S','F','GRANT','{category}')",
        f"CALL LPC{env_code}{env_id}V1_DB_LDI.SETUP.PRIVILEGES_TO_ROLE('{target_db}','A','F','GRANT','{category}')",
        f"CALL LPC{env_code}{env_id}V1_DB_LDI.SETUP.PRIVILEGES_TO_ROLE('{target_db}','U','F','GRANT','{category}')",
        f"CALL LPC{env_code}{env_id}V1_DB_LDI.SETUP.FUTURE_PRIVILEGES_TO_ROLE('{target_db}','S','GRANT','{category}');"
    ]

    # Switch to USERADMIN role
    switch_role("USERADMIN")
    switch_database(f"LPC{env_code}{env_id}V1_DB_LDI")
    for sql in sql_statements[:2]:
        session.sql(sql).collect()

    # Switch to SYSADMIN role
    switch_role("SYSADMIN")
    session.sql(sql_statements[2]).collect()

    # Switch to the specific role
    switch_role(f"{site_abbr}{env_code}X{version}_RE_SYSADMIN")
    switch_database(f"LPC{env_code}{env_id}V1_DB_LDI")
    for sql in sql_statements[3:]:
        session.sql(sql).collect()

def get_schemas(target_db):
    query = f"SHOW SCHEMAS IN DATABASE {target_db}"
    result = session.sql(query).collect()
    return [row[1] for row in result if row[1] not in ['INFORMATION_SCHEMA', 'PUBLIC']]

def replace_database_name(source_db, target_db, view_ddl):
    return re.sub(rf'\b{source_db}\b', target_db, view_ddl)

def replace_lpc(env_code, env_id, v_env_code_source, env_id_source, view_ddl):
    lpc_source = f"LPC{v_env_code_source}{env_id_source}V1_DB_LDI"
    lpc_target = f"LPC{env_code}{env_id}V1_DB_LDI"
    return re.sub(rf'\b{lpc_source}\b', lpc_target, view_ddl)

def execute_ddl(ddl_statements):
    for ddl in ddl_statements:
        session.sql(ddl).collect()

def recreate_views(source_db, target_db, env_code, env_id, v_env_code_source, env_id_source, site_abbr, version):
    # Switch to RU_ADMIN role and db
    switch_role(f"{site_abbr}{env_code}{env_id}{version}_RU_DBADMIN")
    session.sql(f"USE DATABASE {target_db}").collect()

    # Fetch all schemas
    schemas = get_schemas(target_db)
    schemas_df = pd.DataFrame(schemas, columns=['name'])

    # Initialize an empty list to store views data
    views_data = []

    # Loop through each schema
    for v_schema in schemas_df['name']:
        # Fetch views for the current schema
        views_query = f"SHOW VIEWS IN SCHEMA {target_db}.{v_schema}"
        views = session.sql(views_query).collect()

        # Convert the list of Row objects to a DataFrame
        views_df = pd.DataFrame(views)

        # Add schema name to the views DataFrame
        views_df['schema'] = v_schema

        # Append the views DataFrame to the list
        views_data.append(views_df)

    # Concatenate all views DataFrames
    all_views_df = pd.concat(views_data, ignore_index=True)

    # Initialize an empty list to store DDL statements
    ddl_statements = []

    # Loop through each schema and view
    for index, row in all_views_df.iterrows():
        schema = row['schema']
        view = row['name']
        # Fetch DDL for the current view
        ddl_query = f"SELECT GET_DDL('VIEW', '{target_db}.{schema}.{view}') AS ddl"
        ddl_result = session.sql(ddl_query).collect()
        ddl_statement = ddl_result[0]['DDL']

        # Replace old database name with new database name in the DDL statement
        ddl_statement = replace_database_name(source_db, target_db, ddl_statement)
        # Replace old database name with new database name in the DDL statement
        ddl_statement = replace_lpc(env_code, env_id, v_env_code_source, env_id_source, ddl_statement)

        # Store the updated DDL statement in the list
        ddl_statements.append({
            'schema': schema,
            'ddl': ddl_statement
        })

    # Return the DDL statements for further processing
    return ddl_statements

def get_user_info():
    user_info = session.sql("SELECT CURRENT_USER(), CURRENT_ROLE()").collect()
    return user_info[0][0], user_info[0][1]

def store_log(user, role, source_db, target_db, completed, failed_stage, overwrite):
    switch_role("BALANCE_RU_DBADMIN")
    sql_store_info = f"""
    INSERT INTO BALANCING_TOOL.PUBLIC.HISTORIC_CLONE_DB (username, role, source_db, target_db, overwrite, completed, failed_stage, exec_time)
    VALUES ('{user}', '{role}', '{source_db}', '{target_db}', '{overwrite}', '{completed}', '{failed_stage}', CURRENT_TIMESTAMP)
    """
    session.sql(sql_store_info).collect()

def event_log(user, role, source_db, target_db, completed, failed_stage, overwrite):
    switch_role("SYSADMIN")
    store_log(user, role, source_db, target_db, completed, failed_stage, overwrite)
    st.session_state.validate_clicked = False
    st.session_state.run_clicked = False
    if completed == 'N':
        session.sql(f"DROP DATABASE {target_db}").collect()
        switch_database("BALANCING_TOOL")
        switch_role("BALANCE_RU_DBADMIN")
        st.spinner(None)
        st.stop()

def update_comment(target_db, site_abbr, env_code, env_id, version):
    switch_role(f"{site_abbr}{env_code}{env_id}{version}_RU_DBADMIN")
    sql_comment_db = f"alter DATABASE  {target_db} set COMMENT = '{target_db} database for {site_abbr}'"
    session.sql(sql_comment_db).collect()

switch_role("SYSADMIN")

# Initialize session state variables
if 'current_role' not in st.session_state:
    st.session_state.current_role = 'Not Set'
if 'database' not in st.session_state:
    st.session_state.database = None
if 'env' not in st.session_state:
    st.session_state.env = None
if 'clone_type' not in st.session_state:
    st.session_state.clone_type = None
if 'clone_new_env' not in st.session_state:
    st.session_state.clone_new_env = None
if 'db_target_name' not in st.session_state:
    st.session_state.db_target_name = None
if 'check_db_exists' not in st.session_state:
    st.session_state.check_db_exists = None
if 'target_env' not in st.session_state:
    st.session_state.target_env = None
if 'overwrite' not in st.session_state:
    st.session_state.overwrite = None
if 'run' not in st.session_state:
    st.session_state.run = None
if 'validate_clicked' not in st.session_state:
    st.session_state.validate_clicked = False
if 'run_clicked' not in st.session_state:
    st.session_state.run_clicked = False

# Display the current role
st.text_input("Current Role", value=st.session_state.current_role, disabled=True)

# Reset validate_clicked and run_clicked if the user navigates back to the tab
if 'last_database' not in st.session_state:
    st.session_state.last_database = None
if 'last_clone_type' not in st.session_state:
    st.session_state.last_clone_type = None
if 'last_clone_new_env' not in st.session_state:
    st.session_state.last_clone_new_env = None

# Check if the user has navigated back to the tab
if (st.session_state.database != st.session_state.last_database or
    st.session_state.clone_type != st.session_state.last_clone_type or
    st.session_state.clone_new_env != st.session_state.last_clone_new_env):
    st.session_state.validate_clicked = False
    st.session_state.run_clicked = False

# Update the last known state
st.session_state.last_database = st.session_state.database
st.session_state.last_clone_type = st.session_state.clone_type
st.session_state.last_clone_new_env = st.session_state.clone_new_env

# Directly embed the logic to hide elements based on run_clicked state
if not st.session_state.run_clicked:
    result_db_df = get_databases()
    if not result_db_df.empty:
        st.session_state.database = st.selectbox(label="Select Database:", options=result_db_df, index=None, placeholder="Select database...")
        
        if st.session_state.database is not None:
            # GET ENV FOR SELECTED DATABASE
            st.session_state.env = get_cur_db_env(st.session_state.database)
                
            # CREATE NEW DB NAMES ACCORDING TO SELECTION - CAT JUST SUPPORT TO A NEW CAT
            if st.session_state.env == 'Dev':
                v_clone_type = st.radio('What kind of clone do you want to run from this Dev to?', ['QA', 'New Dev version'], index=0)
            elif st.session_state.env == 'QA':
                v_clone_type = st.radio('What kind of clone do you want to run from this QA to?', ['Dev', 'New QA version'], index=0)
            elif st.session_state.env == 'CAT':
                v_clone_type = st.radio('What kind of clone do you want to run from this CAT to?', ['New CAT version'], index=0)
            elif st.session_state.env == 'Prod':
                v_clone_type = st.radio('What kind of clone do you want to run from this Prod to?', ['CAT', 'New Prod version'], index=0)

            v_clone_new_env = st.radio('Do you want to clone to new envorionment ID (it usually occurrs in CAT, where we have C1 and C2)?', ['Y', 'N'], index=1)

            st.session_state.db_target_name = set_db_target_name(v_clone_type, st.session_state.database, v_clone_new_env)

            # Reset validate_clicked if the radio button selection changes
            if st.session_state.clone_type != v_clone_type or st.session_state.clone_new_env != v_clone_new_env:
                st.session_state.validate_clicked = False
                st.session_state.run_clicked = False
            # Store the selection in session state
            st.session_state.clone_type = v_clone_type
            st.session_state.clone_new_env = v_clone_new_env

            # Capture user input and update st.session_state.db_target_name
            st.session_state.db_target_name = st.text_input("Target DB name", value=st.session_state.db_target_name)

            # Your existing code for handling the Validate button
            if st.session_state.db_target_name:
                if st.button('Validate'):
                    st.session_state.validate_clicked = True
                if st.session_state.validate_clicked:
                    result_check_db_df = check_db_exists_fun(st.session_state.db_target_name)
                    if not result_check_db_df.empty:
                        for row in result_check_db_df.itertuples():
                            st.session_state.check_db_exists = row.DATABASE_NAME
                            break
                    else:
                        st.session_state.check_db_exists = 'NONE'
                    
                    # Ensure the radio button remains visible
                    if st.session_state.validate_clicked:
                        if st.session_state.db_target_name == st.session_state.check_db_exists:
                            v_run = st.radio(f"Database {st.session_state.db_target_name} already exists, do you want to overwrite?:", ['Y', 'N'], index=1)
                            st.session_state.overwrite = 'Y'
                        elif st.session_state.check_db_exists == 'NONE':
                            v_run = 'Y'
                            st.session_state.overwrite = 'N'
                        
                        # Evaluate target env
                        st.session_state.target_env = get_cur_db_env(st.session_state.db_target_name)
                        v_valid = 'F'

                        if st.session_state.env == 'Dev' and (st.session_state.target_env == 'QA' or st.session_state.target_env == 'Dev'):
                            v_valid = 'Y'
                        elif st.session_state.env == 'QA' and (st.session_state.target_env == 'Dev' or st.session_state.target_env == 'QA'):
                            v_valid = 'Y'
                        elif st.session_state.env == 'CAT' and st.session_state.target_env == 'CAT':
                            v_valid = 'Y'
                        elif st.session_state.env == 'Prod' and (st.session_state.target_env == 'CAT' or st.session_state.target_env == 'Prod'):
                            if st.session_state.db_target_name == st.session_state.check_db_exists:
                                v_valid = 'N'
                            else:
                                v_valid = 'Y'
                        else:
                            st.error('The target database name is not valid, maybe it will replace a current Production database or creating in a wrong environment what is not allowed')

                        if v_run == 'Y' and v_valid == 'Y':
                            st.success('The target database name is valid, you can proceed to clone')
                            if st.button('Run Clone'):
                                st.session_state.run_clicked = True

                        # Reset validate_clicked if the radio button selection changes
                        if st.session_state.run != v_run:
                            st.session_state.run_clicked = False

                        # Store the selection in session state
                        st.session_state.run = v_run

                        if st.session_state.run_clicked:
                            v_user, v_role = get_user_info()
                            with st.spinner('Cloning database...'):
                                try:
                                    clone_database(st.session_state.database, st.session_state.db_target_name)
                                except Exception as e:
                                    st.error(f"Not able to clone database: {e}")
                                    event_log(v_user, v_role, st.session_state.database, st.session_state.db_target_name, 'N', 'CLONE_DB', st.session_state.overwrite)

                            st.success(f"Database {st.session_state.db_target_name} cloned successfully from {st.session_state.database}")

                            # Extract and display the variables
                            v_site_abbr = st.session_state.db_target_name[:3]
                            v_env_code = st.session_state.db_target_name[3]
                            v_env_id = st.session_state.db_target_name[4]
                            v_version = st.session_state.db_target_name[5:7]
                            v_category = 'SITE'

                            v_env_code_source = st.session_state.database[3]
                            v_env_id_source = st.session_state.db_target_name[4]

                            with st.spinner('Creating new STAGE...'):
                                try:
                                    create_stage(st.session_state.db_target_name)
                                except Exception as e:
                                    st.error(f"Not able to setup roles: {e}")
                                    event_log(v_user, v_role, st.session_state.database, st.session_state.db_target_name, 'N', 'CREATE_STAGE', st.session_state.overwrite)
                                    
                            st.success(f"Stage DATA_INGRESS.SNOWFLAKE_INTERNAL on {st.session_state.db_target_name} created successfully")

                            with st.spinner('Setting up roles...'):
                                try:
                                    setup_roles(v_env_code, v_site_abbr, v_version)
                                except Exception as e:
                                    st.error(f"Not able to setup roles: {e}")
                                    event_log(v_user, v_role, st.session_state.database, st.session_state.db_target_name, 'N', 'SETUP_ROLES', st.session_state.overwrite)
                                    
                            st.success(f"Roles to {st.session_state.db_target_name} created successfully")

                            with st.spinner('Setting up privileges...'):
                                try:
                                    # setup_privileges(target_db, site_abbr, env_code, env_id, version, category)
                                    setup_privileges(st.session_state.db_target_name, v_site_abbr, v_env_code, v_env_id, v_version, v_category)
                                except Exception as e:
                                    st.error(f"Not able to setup privileges: {e}")
                                    event_log(v_user, v_role, st.session_state.database, st.session_state.db_target_name, 'N', 'GRANT_PRIVILEGES', st.session_state.overwrite)

                            st.success(f"Privileges to {st.session_state.db_target_name} granted successfully")     

                            # Update views to point to the new cloned database
                            with st.spinner('Re-building views...'):
                                try:
                                    ddl_statements = recreate_views(st.session_state.database, st.session_state.db_target_name, v_env_code, v_env_id, v_env_code_source, v_env_id_source, v_site_abbr, v_version)
                                    
                                    # Retry mechanism
                                    max_retries = 8
                                    remaining_ddls = ddl_statements
                                    success_count = 0

                                    for attempt in range(max_retries):
                                        remaining_ddls = []
                                        success_count = 0

                                        for ddl_info in ddl_statements:
                                            ddl = ddl_info['ddl']
                                            schema = ddl_info['schema']
                                            try:
                                                # Ensure the view is created in the correct schema
                                                session.sql(f"USE SCHEMA {schema}").collect()
                                                session.sql(ddl).collect()
                                                success_count += 1
                                            except Exception as e:
                                                # Extract the relevant part of the DDL statement
                                                ddl_summary = ddl.split('\n')[0]  # Assuming the first line contains the CREATE OR REPLACE VIEW command
                                                if attempt == max_retries - 1:
                                                    print(f"Error executing DDL: {ddl_summary}")
                                                remaining_ddls.append(ddl_info)

                                        if success_count == len(ddl_statements):
                                            st.success(f"All views in {st.session_state.db_target_name} have been re-created successfully")
                                            break
                                        else:
                                            ddl_statements = remaining_ddls

                                    if success_count != len(ddl_statements):
                                        st.warning(f"Views re-created successfully, but {len(remaining_ddls)} view(s) have columns reference issues. Please validate DDL in the Snowflake Query History.")
                                    
                                except Exception as e:
                                    st.error(f"Not able to recreate views: {e}")
                                    event_log(v_user, v_role, st.session_state.database, st.session_state.db_target_name, 'N', 'RECREATE_VIEWS', st.session_state.overwrite)
                                else:
                                    event_log(v_user, v_role, st.session_state.database, st.session_state.db_target_name, 'Y', None, st.session_state.overwrite)
                                    update_comment(st.session_state.db_target_name, v_site_abbr, v_env_code, v_env_id, v_version)

    else:
        st.warning("No databases available to select")
        v_database = None  # or set a default value if needed

# Display the elements that should remain visible
st.text_input("Current Role", value=st.session_state.current_role, disabled=True)
st.selectbox(label="Select Database:", options=result_db_df, index=None, placeholder="Select database...")
st.text_input("Target DB name", value=st.session_state.db_target_name)
