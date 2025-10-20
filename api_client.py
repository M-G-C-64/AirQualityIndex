import requests
import logging
import pytz
from datetime import datetime
import json
import os
from snowflake.snowpark import Session


snowflake_accuont = os.getenv("SNOWFLAKE_ACCOUNT")
snowflake_password = os.getenv("SNOWFLAKE_PASSWORD")
aqi_api_key = os.getenv("AQI_API_KEY")

# checking all env variables are fetched properly
print(f"""snowflake_account: {len(snowflake_accuont)},
      snowflake_password: {len(snowflake_password)},
      aqi_api_key: {len(aqi_api_key)}""")

try:
    connection_parameters = {
        "account"   : snowflake_accuont,
        "user"      : "RENERA",
        "password"  : snowflake_password,
        "role"      : "SYSADMIN",
        "DATABASE"  : "dev_db",
        "SCHEMA"    : "stage_sch",
        "WAREHOUSE" : "load_wh"
    }

    session = Session.builder.configs(connection_parameters).create()

    # logging config
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)-5s %(message)s",
        datefmt='%Y-%m-%d %H:%M:%S'

    )

    # Set the IST time zone
    ist_timezone = pytz.timezone('Asia/Kolkata')

    # Get the current time in IST
    current_time_ist = datetime.now(ist_timezone)

    # Format the timestamp
    timestamp = current_time_ist.strftime('%Y_%m_%d_%H_%M_%S')

    # Create the file name
    file_name = f'air_quality_data_{timestamp}.json'

    # testing the server
    logging.info("Sending request to the server")
    data = requests.get("https://api.data.gov.in/resource/3b01bcb8-0b14-4abf-b6f2-c1bfd384ba69",
                        headers={"Accept":"application/json"},
                        params={"api-key":aqi_api_key,
                                "format":"json",
                                "limit":"4000"})
    status_Code = data.status_code
    json_data = data.json()

    lst_files_sql = f"list @dev_db.stage_sch.raw_stg PATTERN = '.*{timestamp}.*'"
    list_rows_before = session.sql(lst_files_sql).collect()

    logging.info(f'BEFORE File is placed in snowflake stage location= {list_rows_before}')

    if status_Code == 200:
        logging.info(f"Status code: {status_Code}")

        logging.info(f"Saving the json data in local file")
        with open(file_name, "w") as file:
            json.dump(json_data, file, indent=2)
        logging.info(f'File Written to local disk with name: {file_name}')

        stg_location = '@dev_db.stage_sch.raw_stg'
        session.file.put(file_name, stg_location)
        logging.info('JSON File placed successfully in stage location in snowflake')

        lst_files_sql = f"list @dev_db.stage_sch.raw_stg PATTERN = '.*{timestamp}.*'"
        list_rows = session.sql(lst_files_sql).collect()

        logging.info(f'AFTER File is placed in snowflake stage location= {list_rows}')
        logging.info('The job over successfully...')

    else:
        logging.error(f"Unexpected status code: {status_Code}")
except Exception as e:
    print(e)
    logging.error("Job Failed")