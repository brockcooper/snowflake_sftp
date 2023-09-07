USE ROLE accountadmin;

CREATE SECRET sftp_pw
    TYPE = password
    USERNAME = 'YOUR_USERNAME'
    PASSWORD = 'YOUR_PASSWORD';

CREATE NETWORK RULE sftp_external_access_rule
  TYPE = HOST_PORT
  VALUE_LIST = ('YOUR-SFTP-SERVER.com', 'YOUR-SFTP-SERVER:22') -- Port 22 is the default for SFTP, change if your port is different
  MODE= EGRESS;

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION sftp_access_integration
  ALLOWED_NETWORK_RULES = (sftp_external_access_rule)
  ALLOWED_AUTHENTICATION_SECRETS = (sftp_pw)
  ENABLED = true;

-- This procedure takes a table or view from Snowflake, outputs the results as a CSV, then sends it to an SFTP server
CREATE OR REPLACE PROCEDURE table_to_sftp(database_name string
                                         ,schema_name string
                                         ,table_name string
                                         ,output_file_name string
                                         ,remote_dir_path string
                                         ,sftp_server string
                                         ,port INT
                                         ,append_timestamp BOOLEAN)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.8
HANDLER = 'upload_file_to_sftp'
EXTERNAL_ACCESS_INTEGRATIONS = (sftp_access_integration)
PACKAGES = ('snowflake-snowpark-python','pysftp', 'pandas')
SECRETS = ('cred' = sftp_pw)
AS
$$
import _snowflake
import pysftp
from snowflake.snowpark.files import SnowflakeFile
from datetime import datetime

def upload_file_to_sftp(session, database_name, schema_name, table_name, output_file_name, remote_dir_path, sftp_server, port, append_timestamp):

    # Read the origin table into a Snowflake dataframe
    df_sp = session.table([database_name, schema_name, table_name])

    # Convert the Snowflake dataframe into a Pandas dataframe
    df_pd = df_sp.to_pandas()

    # Make the temp file
    if append_timestamp:
        now = datetime.now()
        epoch_time = str(int(now.timestamp() * 1000))  # Multiplied by 1000 to include milliseconds
        local_file_path = '/tmp/' + output_file_name + '_' + epoch_time + '.csv'
    else:
        local_file_path = '/tmp/' + output_file_name + '.csv'

    df_pd.to_csv(local_file_path)

    username_password_object = _snowflake.get_username_password('cred');

    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None  # Disable host key checking. Use carefully.

    # Your SFTP credentials
    sftp_host = sftp_server
    sftp_port = port  # default port for SFTP
    sftp_username = username_password_object.username
    sftp_password = username_password_object.password

    # Remote directory to upload the file
    remote_directory = remote_dir_path

    # Establish SFTP connection
    try:
        with pysftp.Connection(host=sftp_host, username=sftp_username, password=sftp_password, port=sftp_port, cnopts=cnopts) as sftp:
            try:
                # Change to the remote directory where the file will be uploaded
                sftp.chdir(remote_directory)
            except IOError:
                print(f"Remote directory {remote_directory} does not exist.")
                return

            # Upload the file
            sftp.put(local_file_path)

            message = f'Successfully uploaded {local_file_path} to {remote_directory}'
    except Exception as e:
        message = f"An error occurred: {e}"

    return message
$$;

-- Example to call the procedure
CALL table_to_sftp('DATABASE_NAME'
                 , 'SCHEMA_NAME'
                 , 'TABLE_NAME'
                 , 'OUTPUT_FILE_NAME'
                 ,'/example_folder/' -- Example remote dir path
                 , 'YOUR-SFTP-SERVER.com'
                 , 22 -- Port 22 is the default for SFTP, change if your port is different
                 , TRUE -- Appends an EPOCH timestamp to the file name
);