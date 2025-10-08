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
from time import sleep

load_dotenv()
from cryptography.hazmat.primitives import serialization

logging.basicConfig(level=logging.WARN)

COLS_DB = [['ORDER_ID', 'CUSTOMER_ID', 'ORDER_DATETIME', 'CHANNEL', 'STORE_ID', 'ORDER_STATUS', 'TOTAL_AMOUNT', 'CURRENCY', 'PAYMENT_STATUS'],
            ['ORDER_ITEM_ID', 'ORDER_ID', 'PRODUCT_ID', 'QTY', 'UNIT_PRICE', 'DISCOUNT_PCT', 'LINE_TOTAL'],
            ['PAYMENT_ID', 'ORDER_ID', 'PAYMENT_DATETIME', 'PAYMENT_METHOD', 'AMOUNT', 'PAYMENT_STATUS']]

DB_TO_SEE = ["ORDERS", "ORDER_ITEMS", "PAYMENTS"]

LOGS_DB_TO_SEE = ["ORDERS", "ORDER_ITEMS", "PAYMENTS"]

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


def save_to_snowflake(snow, order_arr, order_item_arr, payment_arr, temp_dir, ingest_manager):
    logging.debug('inserting batch to db')
    to_see = [order_arr, order_item_arr, payment_arr]
    for i in range(len(to_see)):
        start = time.time()
        pandas_df = pd.DataFrame(to_see[i], columns=COLS_DB[i])
        arrow_table = pa.Table.from_pandas(pandas_df)
        file_name = f"{str(uuid.uuid1())}.parquet"
        out_path =  f"{temp_dir.name}/{file_name}"
#        print(arrow_table)
#        print('femi')
#        print(out_path)
#        print(file_name)
        pq.write_table(arrow_table, out_path, use_dictionary=False,compression='SNAPPY')
        print("PUT 'file://{0}' @%{1}".format(out_path, DB_TO_SEE[i]))
        snow.cursor().execute("PUT 'file://{0}' @%{1}".format(out_path, DB_TO_SEE[i]))
        # send the new file to snowpipe to ingest (serverless)
        resp = ingest_manager[i].ingest_files([StagedFile(file_name, None),])
        logging.info(f"response from snowflake for file {file_name}: {resp['responseCode']}")
        response_code = resp['responseCode']
        print(response_code)
        os.unlink(out_path)
        try:
            insert_sql = """
                INSERT INTO {0}_PIPE_LOAD_LOGS 
                (PIPE_NAME, FILE_NAME, ROUTINE_TIME_EXEC, ROW_COUNT, STATUS, ERROR_MESSAGE)
                VALUES (%s, %s, %s, %s, %s, %s)
            """.format(DB_TO_SEE[i])
            status = "SUCCESS" if response_code == "SUCCESS" else "FAILED"
            error_msg = None if response_code == "SUCCESS" else str(resp)
            snow.cursor().execute(
                insert_sql,
                (
                    "INGEST.INGEST.{0}".format(DB_TO_SEE[i]),
                    file_name,
                    int(time.time() - start),
                    len(to_see[i]),
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
    ingest_orders_manager = SimpleIngestManager(account=os.getenv("SNOWFLAKE_ACCOUNT"),
                                         host=host,
                                         user=os.getenv("SNOWFLAKE_USER"),
                                         pipe='INGEST.INGEST.ORDERS_PIPE',
                                         private_key=private_key)
    
    ingest_order_items_manager = SimpleIngestManager(account=os.getenv("SNOWFLAKE_ACCOUNT"),
                                         host=host,
                                         user=os.getenv("SNOWFLAKE_USER"),
                                         pipe='INGEST.INGEST.ORDER_ITEMS_PIPE',
                                         private_key=private_key)
    
    ingest_payments_manager = SimpleIngestManager(account=os.getenv("SNOWFLAKE_ACCOUNT"),
                                         host=host,
                                         user=os.getenv("SNOWFLAKE_USER"),
                                         pipe='INGEST.INGEST.PAYMENTS_PIPE',
                                         private_key=private_key)
    order_arr = []
    order_item_arr = []
    payment_arr = []
    ingest_managers = [ingest_orders_manager, ingest_order_items_manager, ingest_payments_manager]
    for message in sys.stdin:
        if message != '\n':
            record = json.loads(message)
            order_json = record['order']
            order_item_json = record['order_item']
            payment_json = record["payments"]
            order_arr.append([order_json['order_id'], order_json['customer_id'],order_json['order_datetime'],order_json['channel'], order_json['store_id'],order_json['order_status'],order_json['total_amount'],order_json['currency'], order_json['payment_status']])
            order_item_arr.append([order_item_json["order_item_id"],order_item_json["order_id"],order_item_json["product_id"],order_item_json["qty"],order_item_json["unit_price"],order_item_json["discount_pct"],order_item_json["line_total"]])
            payment_arr.append([payment_json["payment_id"],payment_json["order_id"],payment_json["payment_datetime"],payment_json["payment_method"],payment_json["amount"],payment_json["payment_status"]])
            batch.append(record)
#            batch.append((record['txid'],record['rfid'],record["item"],record["purchase_time"],record["expiration_time"],record['days'],record['name'],record['address'],record['phone'],record['email'], record['emergency_contact']))
            if len(order_item_arr) == batch_size:
                save_to_snowflake(snow, order_arr, order_item_arr, payment_arr, temp_dir, ingest_managers)
                order_item_arr = []
                payment_arr = []
                order_arr = []
        else:
            break    
    if len(order_arr) > 0:
        save_to_snowflake(snow, order_arr, order_item_arr, payment_arr, temp_dir, ingest_managers)
    temp_dir.cleanup()
    snow.close()
    logging.info("Ingest complete")