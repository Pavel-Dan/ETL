import os, sys, logging
import json
import uuid
import snowflake.connector
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import tempfile

from dotenv import load_dotenv
from snowflake.ingest import SimpleIngestManager
from snowflake.ingest import StagedFile
import time

load_dotenv()
from cryptography.hazmat.primitives import serialization

logging.basicConfig(level=logging.WARN)


def connect_snow():
    private_key = "-----BEGIN PRIVATE KEY-----\n" + os.getenv("PRIVATE_KEY") + "\n-----END PRIVATE KEY-----\n"
    p_key = serialization.load_pem_private_key(
        bytes(private_key, 'utf-8'),
        password=None
    )
    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption())

    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        private_key=pkb,
        role="INGEST",
        database="INGEST",
        schema="INGEST",
        warehouse="INGEST",
        session_parameters={'QUERY_TAG': 'py-snowpipe'}, 
    )


def save_to_snowflake(snow, batch, temp_dir, ingest_manager):
    start = time.time()
    logging.debug('inserting batch to db')
    pandas_df = pd.DataFrame(batch, columns=["TXID","RFID","ITEM","PURCHASE_TIME", "EXPIRATION_TIME","DAYS","NAME","ADDRESS","PHONE","EMAIL", "EMERGENCY_CONTACT"])
    arrow_table = pa.Table.from_pandas(pandas_df)
    file_name = f"{str(uuid.uuid1())}.parquet"
    out_path =  f"{temp_dir.name}/{file_name}"
    pq.write_table(arrow_table, out_path, use_dictionary=False,compression='SNAPPY')
    snow.cursor().execute("PUT 'file://{0}' @%CLIENT_SUPPORT_ORDERS_PY_SNOWPIPE".format(out_path))
    os.unlink(out_path)
    # send the new file to snowpipe to ingest (serverless)
    resp = ingest_manager.ingest_files([StagedFile(file_name, None),])
    logging.info(f"response from snowflake for file {file_name}: {resp['responseCode']}")
    response_code = resp['responseCode']

    try:
        insert_sql = """
            INSERT INTO PIPE_LOAD_LOGS 
            (PIPE_NAME, FILE_NAME, ROUTINE_TIME_EXEC, ROW_COUNT, STATUS, ERROR_MESSAGE)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        status = "REQUESTED" if response_code == "SUCCESS" else "FAILED"
        error_msg = None if response_code == "SUCCESS" else str(resp)
        snow.cursor().execute(
            insert_sql,
            (
                "INGEST.INGEST.CLIENT_SUPPORT_ORDERS_PIPE",
                file_name,
                int(time.time() - start),
                len(batch),
                status,
                error_msg
            )
        )
    except Exception as e:
        logging.error(f"Error while inserting log: {e}")
        print(e)


if __name__ == "__main__":    
    args = sys.argv[1:]
    batch_size = int(args[0])
    snow = connect_snow()
    batch = []
    temp_dir = tempfile.TemporaryDirectory()
    private_key = "-----BEGIN PRIVATE KEY-----\n" + os.getenv("PRIVATE_KEY") + "\n-----END PRIVATE KEY-----\n"
    host = os.getenv("SNOWFLAKE_ACCOUNT") + ".snowflakecomputing.com"
    ingest_manager = SimpleIngestManager(account=os.getenv("SNOWFLAKE_ACCOUNT"),
                                         host=host,
                                         user=os.getenv("SNOWFLAKE_USER"),
                                         pipe='INGEST.INGEST.CLIENT_SUPPORT_ORDERS_PIPE',
                                         private_key=private_key)
    for message in sys.stdin:
        if message != '\n':
            record = json.loads(message)
            batch.append((record['txid'],record['rfid'],record["item"],record["purchase_time"],record["expiration_time"],record['days'],record['name'],record['address'],record['phone'],record['email'], record['emergency_contact']))
            if len(batch) == batch_size:
                save_to_snowflake(snow, batch, temp_dir, ingest_manager)
                batch = []
        else:
            break    
    if len(batch) > 0:
        save_to_snowflake(snow, batch, temp_dir, ingest_manager)
    temp_dir.cleanup()
    snow.close()
    logging.info("Ingest complete")
