import json
import time
import threading
import queue
import sys
import os
from datetime import datetime
from typing import Dict, List, Any

class SimpleKafkaBroker:
    """Simple Kafka-like message broker using Python queues"""
    
    def __init__(self):
        self.topics: Dict[str, queue.Queue] = {}
        self.consumers: Dict[str, List[queue.Queue]] = {}
        self.running = True
        
    def create_topic(self, topic_name: str, partitions: int = 1):
        """Create a new topic"""
        if topic_name not in self.topics:
            self.topics[topic_name] = queue.Queue()
            self.consumers[topic_name] = []
            print(f"✅ Created topic: {topic_name}")
        else:
            print(f"ℹ️  Topic {topic_name} already exists")
    
    def publish_message(self, topic_name: str, message: str) -> bool:
        """Publish a message to a topic"""
        if topic_name not in self.topics:
            print(f"❌ Topic {topic_name} does not exist")
            return False
        
        try:
            # Add timestamp and message ID
            message_data = {
                'id': f"{int(time.time() * 1000)}",
                'timestamp': datetime.now().isoformat(),
                'data': message,
                'topic': topic_name
            }
            
            # Put message in topic queue
            self.topics[topic_name].put(message_data)
            print(f"📤 Published to {topic_name}: {message[:50]}...")
            return True
            
        except Exception as e:
            print(f"❌ Error publishing message: {e}")
            return False
    
    def subscribe_to_topic(self, topic_name: str, consumer_id: str) -> queue.Queue:
        """Subscribe to a topic and return a consumer queue"""
        if topic_name not in self.topics:
            self.create_topic(topic_name)
        
        # Create consumer queue
        consumer_queue = queue.Queue()
        self.consumers[topic_name].append(consumer_queue)
        
        # Start consumer thread
        consumer_thread = threading.Thread(
            target=self._consumer_worker,
            args=(topic_name, consumer_queue, consumer_id),
            daemon=True
        )
        consumer_thread.start()
        
        print(f"👥 Consumer {consumer_id} subscribed to {topic_name}")
        return consumer_queue
    
    def _consumer_worker(self, topic_name: str, consumer_queue: queue.Queue, consumer_id: str):
        """Worker thread for consuming messages"""
        while self.running:
            try:
                # Get message from topic
                message = self.topics[topic_name].get(timeout=1.0)
                
                # Forward to consumer queue
                consumer_queue.put(message)
                
                print(f"📥 Consumer {consumer_id} received: {message['data'][:50]}...")
                
            except queue.Empty:
                continue
            except Exception as e:
                print(f"❌ Consumer {consumer_id} error: {e}")
                break
    
    def get_topic_stats(self, topic_name: str) -> Dict[str, Any]:
        """Get statistics for a topic"""
        if topic_name not in self.topics:
            return {"error": "Topic not found"}
        
        return {
            "topic": topic_name,
            "queue_size": self.topics[topic_name].qsize(),
            "consumer_count": len(self.consumers[topic_name]),
            "status": "active"
        }
    
    def list_topics(self) -> List[str]:
        """List all topics"""
        return list(self.topics.keys())
    
    def stop(self):
        """Stop the broker"""
        self.running = False
        print("🛑 Kafka broker stopped")

class SimpleKafkaProducer:
    """Simple Kafka producer"""
    
    def __init__(self, broker_instance):
        self.broker = broker_instance
        self.message_count = 0
    
    def publish(self, topic_name: str, message: str) -> bool:
        """Publish a message"""
        success = self.broker.publish_message(topic_name, message)
        if success:
            self.message_count += 1
        return success
    
    def flush(self):
        """Flush any pending messages (no-op in simple implementation)"""
        pass

class SimpleKafkaConsumer:
    """Simple Kafka consumer"""
    
    def __init__(self, broker_instance, consumer_id: str = None):
        self.broker = broker_instance
        self.consumer_id = consumer_id or f"consumer_{int(time.time())}"
        self.consumer_queue = None
        self.message_count = 0
    
    def subscribe(self, topic_name: str):
        """Subscribe to a topic"""
        self.consumer_queue = self.broker.subscribe_to_topic(topic_name, self.consumer_id)
        print(f"📡 Subscribed to {topic_name}")
    
    def poll(self, timeout: float = 1.0) -> Dict[str, Any]:
        """Poll for messages"""
        if not self.consumer_queue:
            return None
        
        try:
            message = self.consumer_queue.get(timeout=timeout)
            self.message_count += 1
            return message
        except queue.Empty:
            return None
        except Exception as e:
            print(f"❌ Consumer poll error: {e}")
            return None
    
    def close(self):
        """Close the consumer"""
        print(f"🔒 Consumer {self.consumer_id} closed")

def main():
    broker = SimpleKafkaBroker()
    """Main function for testing the simple Kafka setup"""
    print("🚀 Simple Kafka Setup (No Docker Required)")
    print("=" * 60)
    
    # Create topic
    topic_name = "kpop_merchandise_orders"
    broker.create_topic(topic_name)
    
    # Create producer
    producer = SimpleKafkaProducer(broker)
    
    # Create consumer
    consumer = SimpleKafkaConsumer(broker)
    consumer.subscribe(topic_name)
    
    print(f"\n📊 Testing message flow...")
    
    # Test publishing messages
    test_messages = [
        '{"txid": "test1", "item": "Kpop Demon Hunter T-Shirt", "name": "Alice"}',
        '{"txid": "test2", "item": "Kpop Demon Hunter Hoodie", "name": "Bob"}',
        '{"txid": "test3", "item": "Kpop Demon Hunter Poster", "name": "Charlie"}'
    ]
    
    for message in test_messages:
        producer.publish(topic_name, message)
        time.sleep(0.5)  # Small delay between messages
    
    # Test consuming messages
    print(f"\n📥 Consuming messages...")
    for i in range(5):  # Try to consume 5 messages
        message = consumer.poll(timeout=2.0)
        if message:
            print(f"   Message {i+1}: {message['data']}")
        else:
            print(f"   No message received (timeout)")
    
    # Show statistics
    stats = broker.get_topic_stats(topic_name)
    print(f"\n📊 Topic Statistics:")
    print(f"   Topic: {stats['topic']}")
    print(f"   Queue size: {stats['queue_size']}")
    print(f"   Consumer count: {stats['consumer_count']}")
    print(f"   Messages published: {producer.message_count}")
    print(f"   Messages consumed: {consumer.message_count}")
    
    # Cleanup
    consumer.close()
    broker.stop()
    
    print(f"\n✅ Simple Kafka test completed!")

if __name__ == "__main__":
    main()
