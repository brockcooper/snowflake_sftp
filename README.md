# Snowflake SFTP

## Overview

Although Snowflake is built for Secure Data Sharing between Snowflake accounts, there is often still the need to pull and push data to an SFTP Server. Using Python Stored Procedures and External Access, Snowflake can completely automate and manage that process.

## Setup

This is going to be a simple setup. All of this will be found in the `setup.sql` file. You will need the following:
* **Secret**: This will contain your Username and Password to log into the SFTP Server [Docs](https://docs.snowflake.com/sql-reference/sql/create-secret)
* **Network Rule**: This will create a network rule that represents the external network’s location and restrictions for accessing the SFTP server. This will mainly contain the Server Name and Port [Docs](https://docs.snowflake.com/sql-reference/sql/create-network-rule)
* **External Access Integration**: Aggregates Network Rule and Secret we created before [Docs](https://docs.snowflake.com/sql-reference/sql/create-external-access-integration)
* **Three Stored Procedures**:
    * `table_to_sftp`: Moves data from a Snowflake table to an SFTP Server.
    * `sftp_to_table`: Picks up the file from SFTP and write it to a specified Snowflake Table. This Procedure accepts the [mode](https://docs.snowflake.com/developer-guide/snowpark/reference/python/latest/api/snowflake.snowpark.DataFrameWriter.mode#snowflake.snowpark.DataFrameWriter.mode) parameter, which will control how the data is written to the table. Examples of `mode` would be append, overwrite, errorifexists, and ignore.
    * `sftp_to_internal_stage`: Moves data from a SFTP to an Internal Stage.


## Considerations

To put this into production, there are a handful of considerations:
* For each new SFTP server you interact with, you will need a new Secret, Network Rule, and External Access Integration which will also require a new Stored Procedure. You will need to update the header of the new Stored Procedure to update the `SECRETS` and `EXTERNAL_ACCESS_INTEGRATIONS` parameters for your new Secret and External Access Integrations
* This can easily be scheduled in Snowflake by calling the Stored Procedure within a [Task](https://docs.snowflake.com/en/user-guide/tasks-intro)