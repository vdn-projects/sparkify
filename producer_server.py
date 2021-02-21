import json
import logging
import time

from confluent_kafka import Producer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic

logger = logging.getLogger(__name__)


class ProducerServer:
    """
    Basic Kafka consumer class
    """

    def __init__(self, conf, time_interval):
        self.conf = conf
        self.topic = self.conf.get("kafka", "topic")
        self.input_file = self.conf.get("kafka", "input_file")
        self.bootstrap_servers = self.conf.get("kafka", "bootstrap_servers")
        self.num_partitions = self.conf.getint("kafka", "num_partitions")
        self.replication_factor = self.conf.getint("kafka", "replication_factor")
        self.progress_interval = self.conf.getint("kafka", "progress_interval")
        self.admin_client = AdminClient({"bootstrap.servers": self.bootstrap_servers})
        self.producer = Producer({"bootstrap.servers": self.bootstrap_servers})
        self.time_interval = time_interval

    def create_topic(self):
        """
        Check if Kafka topic already exists. If not, create it, else continue
        """
        if self.topic not in self.admin_client.list_topics().topics:
            futures = self.admin_client.create_topics([NewTopic(topic=self.topic,
                                                                num_partitions=self.num_partitions,
                                                                replication_factor=self.replication_factor)])

            for _topic, future in futures.items():
                try:
                    future.result()
                    logger.info(f"Created topic: {_topic}")
                except KafkaError as err:
                    logger.critical(f"Failed to create topic {_topic}: {err}")
        else:
            logger.info(f"Topic {self.topic} already exists")

    def generate_data(self):
        """
        Read input JSON file from disk and produce individual serialized rows to Kafka
        """
        with open(self.input_file, "r", encoding="utf8") as f:
            line_count = 0
            for line in f:
                data = json.loads(line)

                # trigger delivery report callbacks from previous produce calls
                self.producer.poll(timeout=2)

                # serialize Python dict to string
                msg = self.serialize_json(data)
                logger.debug(f"Serialized JSON data:\n {msg}")

                # send data to Kafka
                self.producer.produce(topic=self.topic, value=msg, callback=self.delivery_callback)

                # log progress
                line_count = 1
                if line_count % self.progress_interval == 0:
                    logger.debug(f"Processed {line_count} rows of data")

                # wait 2 second before reading next line
                time.sleep(self.time_interval)

            # make sure all messages are delivered before closing producer
            logger.debug("Flushing producer")
            self.producer.flush()

    @staticmethod
    def serialize_json(json_data):
        """
        Serialize Python dict to JSON-formatted, UTF-8 encoded string
        """
        return json.dumps(json_data).encode("utf-8")

    @staticmethod
    def delivery_callback(err, msg):
        """
        Callback triggered by produce function
        """
        if err is not None:
            logger.error(f"Failed to deliver message: {err}")
        else:
            # logger.info(f"Successfully produced message to topic {msg.topic()}")
            print(f"---")

    def close(self):
        """
        Convenience method to flush producer
        """
        logger.debug("Flushing producer")
        self.producer.flush()
