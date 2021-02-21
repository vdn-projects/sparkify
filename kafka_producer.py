import logging
import logging.config
from configparser import ConfigParser
import argparse
import os
import subprocess

cur_path = os.path.dirname(os.path.realpath(__file__))

# make sure logging config is picked up by modules
logging.config.fileConfig(os.path.join(cur_path, "logging.ini"))

import producer_server

def run_cmd(args_list):
    """
    run linux commands
    """
    # import subprocess
    # print('Running system command: {0}'.format(' '.join(args_list)))
    proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    s_output, s_err = proc.communicate()
    s_return =  proc.returncode
    
    if s_return != 0:
        return s_return, s_output, s_err
    else:
        return "Command executed successfully!"

def run_kafka_producer(time_interval):
    """
    Create Kafka producer, check if relevant topic exists (if not create it) and start producing messages
    """

    # load config
    config = ConfigParser()
    config.read(os.path.join(cur_path, "app.cfg"))

    # start kafka producer
    logger.info("Starting Kafka Producer")
    producer = producer_server.ProducerServer(config, time_interval)

    # check if topic exists
    logger.info("Creating topic...")
    producer.create_topic()

    # generate data
    logger.info("Starting to generate data...")

    try:
        producer.generate_data()
    except KeyboardInterrupt:
        logging.info("Stopping Kafka Producer")
        producer.close()


if __name__ == "__main__":
    
    # start logging
    logger = logging.getLogger(__name__)

    # Clear topic
    clear_topic_res = run_cmd(["kafka-topics.sh", "--zookeeper", "zoo01-vn00c1.vn.infra:2181", "--delete", "--topic", "streaming.sparkify.medium_event_data"])
    logger.info(clear_topic_res)

    parser = argparse.ArgumentParser(description="Decode and Vectorize datascore raw data")
    parser.add_argument("--time_interval", type=int, required=True, help="Deployment mode (test & prod)", default=2)

    args = parser.parse_args()
    time_interval = args.time_interval

    run_kafka_producer(time_interval)
