import os
import sys
import json
import logging
from confluent_kafka import Consumer, KafkaException, KafkaError
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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


# 🧠 Configuration
KAFKA_BROKERS = os.getenv("REDPANDA_BROKERS", "127.0.0.1:19092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "merchandise_orders")
GROUP_ID = os.getenv("KAFKA_CONSUMER_GROUP", "merchandise_analytics")

def create_consumer():
    """Create Kafka Consumer"""
    conf = {
        'bootstrap.servers': KAFKA_BROKERS,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest',  # start from beginning if no committed offsets
        'enable.auto.commit': True
    }
    consumer = Consumer(conf)
    consumer.subscribe([KAFKA_TOPIC])
    logging.info(f"📡 Listening to topic '{KAFKA_TOPIC}' on brokers {KAFKA_BROKERS}")
    return consumer

def main():
    consumer = create_consumer()
    snow = connect_snow()
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
    ingest_managers = [ingest_orders_manager, ingest_order_items_manager, ingest_payments_manager]
    try:
        while True:
            msg = consumer.poll(1.0)  # wait 1s for a message
            if msg is None:
                continue  # no message yet
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())

            # ✅ Message received
            message_value = msg.value().decode('utf-8')
            logging.info(f"📨 Received message: {message_value}")

            # Optionally parse JSON
            try:
                data = json.loads(message_value)
                logging.info(f"🧾 Parsed data: {data}")
                order_arr = []
                order_item_arr = []
                payment_arr = []
                order_json = data['order']
                order_item_json = data['order_item']
                payment_json = data["payments"]
                order_arr.append([order_json['order_id'], order_json['customer_id'],order_json['order_datetime'],order_json['channel'], order_json['store_id'],order_json['order_status'],order_json['total_amount'],order_json['currency'], order_json['payment_status']])
                order_item_arr.append([order_item_json["order_item_id"],order_item_json["order_id"],order_item_json["product_id"],order_item_json["qty"],order_item_json["unit_price"],order_item_json["discount_pct"],order_item_json["line_total"]])
                payment_arr.append([payment_json["payment_id"],payment_json["order_id"],payment_json["payment_datetime"],payment_json["payment_method"],payment_json["amount"],payment_json["payment_status"]])
                save_to_snowflake(snow, order_arr, order_item_arr, payment_arr, temp_dir, ingest_managers)

            except json.JSONDecodeError:
                logging.warning("⚠️ Message is not valid JSON")
            except Exception as e:
                print(e)

    except KeyboardInterrupt:
        logging.info("⏹️ Consumer stopped by user")
    finally:
        consumer.close()
        logging.info("🔒 Consumer connection closed")

if __name__ == "__main__":
    main()
