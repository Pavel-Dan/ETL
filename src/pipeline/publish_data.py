import os
import logging
import sys
import confluent_kafka
from kafka.admin import KafkaAdminClient, NewTopic
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)

# Configuration from environment variables
kafka_brokers = os.getenv("REDPANDA_BROKERS", "127.0.0.1:19092")
topic_name = os.getenv("KAFKA_TOPIC", "merchandise_orders")

def create_topic():
    """Create Kafka topic if it doesn't exist"""
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=kafka_brokers, 
            client_id='merchandise_publisher'
        )
        topic_metadata = admin_client.list_topics()
        
        if topic_name not in topic_metadata:
            logging.info(f"📝 Creating topic: {topic_name}")
            topic = NewTopic(
                name=topic_name, 
                num_partitions=10, 
                replication_factor=1
            )
            admin_client.create_topics(new_topics=[topic], validate_only=False)
            logging.info(f"✅ Topic '{topic_name}' created successfully")
        else:
            logging.info(f"✅ Topic '{topic_name}' already exists")
            
    except Exception as e:
        logging.error(f"❌ Error creating topic: {e}")
        raise

def get_kafka_producer():
    """Create and configure Kafka producer"""
    logging.info(f"🔌 Connecting to Kafka brokers: {kafka_brokers}")
    
    config = {
        'bootstrap.servers': kafka_brokers,
        'client.id': 'merchandise_producer',
        'acks': 'all',  # Wait for all replicas to acknowledge
        'retries': 3,   # Retry failed messages
        'retry.backoff.ms': 100,
        'compression.type': 'snappy',  # Compress messages for efficiency
        'batch.size': 16384,  # Batch messages for better throughput
        'linger.ms': 10,  # Wait up to 10ms to batch messages
    }
    
    return confluent_kafka.Producer(**config)

def publish_message(producer, message):
    """Publish a single message to Kafka with error handling"""
    try:
        # Produce message to topic
        producer.produce(
            topic_name, 
            value=bytes(message, encoding='utf8'),
            callback=delivery_callback
        )
        return True
        
    except BufferError as e:
        logging.warning(f"⚠️  Producer buffer full, flushing...")
        producer.flush()
        # Retry after flush
        producer.produce(topic_name, value=bytes(message, encoding='utf8'))
        return True
        
    except Exception as e:
        logging.error(f"❌ Error publishing message: {e}")
        return False

def delivery_callback(err, msg):
    """Callback function for message delivery confirmation"""
    if err is not None:
        logging.error(f"❌ Message delivery failed: {err}")
    else:
        logging.debug(f"✅ Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def main():
    """Main publishing function"""
    print(f"🚀 Starting Kafka Publisher for Uniclth  Merchandise Orders")
    print(f"📊 Topic: {topic_name}")
    print(f"🔌 Brokers: {kafka_brokers}")
    print("=" * 60)
    
    try:
        # Create topic if needed
        create_topic()
        
        # Create producer
        producer = get_kafka_producer()
        
        # Statistics
        messages_published = 0
        messages_failed = 0
        
        print(f"📡 Publishing messages... (Press Ctrl+C to stop)")
        
        # Process messages from stdin
        for message in sys.stdin:
            if message != '\n':
                # Clean the message
                message = message.strip()
                if not message:
                    continue
                
                # Publish message with retry logic
                failed = True
                retry_count = 0
                max_retries = 3
                
                while failed and retry_count < max_retries:
                    if publish_message(producer, message):
                        messages_published += 1
                        failed = False
                        
                        # Log progress every 100 messages
                        if messages_published % 100 == 0:
                            print(f"📊 Published {messages_published} messages...")
                    else:
                        retry_count += 1
                        if retry_count < max_retries:
                            logging.warning(f"⚠️  Retry {retry_count}/{max_retries} for message")
                        else:
                            messages_failed += 1
                            logging.error(f"❌ Failed to publish message after {max_retries} retries")
                            failed = False
            else:
                break
        
        # Flush any remaining messages
        print(f"🔄 Flushing remaining messages...")
        producer.flush(timeout=10)
        
        # Final statistics
        print(f"\n📊 Publishing Complete!")
        print(f"   ✅ Messages published: {messages_published}")
        print(f"   ❌ Messages failed: {messages_failed}")
        print(f"   📈 Success rate: {(messages_published/(messages_published+messages_failed)*100):.1f}%")
        
        if messages_failed > 0:
            print(f"\n⚠️  Some messages failed to publish. Check Kafka broker status.")
        
    except KeyboardInterrupt:
        print(f"\n⏹️  Publisher stopped by user")
        producer.flush(timeout=5)
        print(f"📊 Final stats: {messages_published} published, {messages_failed} failed")
        
    except Exception as e:
        logging.error(f"❌ Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

