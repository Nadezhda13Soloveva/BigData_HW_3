import os
import json
import csv
import time
import glob
from kafka import KafkaProducer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CSVToKafkaProducer:
    def __init__(self):
        self.bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092')
        self.topic = os.getenv('KAFKA_TOPIC', 'mock_data_topic')
        self.data_dir = os.getenv('DATA_DIR', '/app/data')

        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8') if k else None,
            retries=5,
            acks='all'
        )

    def read_csv_files(self):
        csv_files = glob.glob(os.path.join(self.data_dir, 'MOCK_DATA*.csv'))
        logger.info(f"Found {len(csv_files)} CSV files")

        for csv_file in sorted(csv_files):
            logger.info(f"Processing file: {csv_file}")
            with open(csv_file, 'r', encoding='utf-8') as file:
                csv_reader = csv.DictReader(file)
                for row in csv_reader:
                    row['_source_file'] = os.path.basename(csv_file)
                    row['_timestamp'] = time.time()
                    yield row, row.get('id', str(time.time()))

    def send_messages(self):
        messages_sent = 0
        for row, key in self.read_csv_files():
            try:
                self.producer.send(self.topic, value=row, key=key)
                messages_sent += 1

                if messages_sent % 500 == 0:
                    logger.info(f"Sent {messages_sent} messages...")
                    time.sleep(0.1)

            except Exception as e:
                logger.error(f"Error sending message: {e}")

        self.producer.flush()
        logger.info(f"Completed! Total messages sent: {messages_sent}")

    def run(self):
        try:
            logger.info(f"Starting Kafka Producer - sending to {self.topic}")
            self.send_messages()
            logger.info("Producer completed successfully!")
            exit(0)
        except KeyboardInterrupt:
            logger.info("Producer stopped by user")
            exit(0)
        except Exception as e:
            logger.error(f"Producer failed with error: {e}")
            exit(1)
        finally:
            self.producer.close()


if __name__ == "__main__":
    producer = CSVToKafkaProducer()
    producer.run()
