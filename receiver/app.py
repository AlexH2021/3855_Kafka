from datetime import datetime
import logging.config, connexion, yaml, uuid, json
from pykafka import KafkaClient

import os
if "TARGET_ENV" in os.environ and os.environ["TARGET_ENV"] == "test":
  print("In Test Environment")
  app_conf_file = "/config/app_conf.yml"
  log_conf_file = "/config/log_conf.yml"
else:
  print("In Dev Environment")
  app_conf_file = "app_conf.yml"
  log_conf_file = "log_conf.yml"

with open(app_conf_file, 'r') as f:
  app_config = yaml.safe_load(f.read())
# External Logging Configuration
with open(log_conf_file, 'r') as f:
  log_config = yaml.safe_load(f.read())
  logging.config.dictConfig(log_config)

logger = logging.getLogger('basicLogger')
logger.info("App Conf File: %s" % app_conf_file)
logger.info("Log Conf File: %s" % log_conf_file)

def post_acc(body):
    trace_id = str(uuid.uuid4())
    body['traceID'] = trace_id
    body['createdAt'] = str(datetime.now().replace(microsecond=0))

    print(body)
    kafka_hosts = f"{app_config['events']['hostname']}:{app_config['events']['port']}"
    client = KafkaClient(hosts=kafka_hosts)
    topic = client.topics[str.encode(app_config['events']['topic'])]
    producer = topic.get_sync_producer()
    msg = { 
        "type": "requests_post_acc",
        "datetime" : datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "payload": body 
    }
    msg_str = json.dumps(msg)
    producer.produce(msg_str.encode('utf-8'))
    
    #request received log
    logger.info(f'Received event sent Account data request with a trace id of {trace_id}')

    return msg

def post_trade(body):
    trace_id = str(uuid.uuid4())
    body['traceID'] = trace_id
    body['createdAt'] = str(datetime.now().replace(microsecond=0))
   
    kafka_hosts = f"{app_config['events']['hostname']}:{app_config['events']['port']}"
    client = KafkaClient(hosts=kafka_hosts)
    topic = client.topics[str.encode(app_config['events']['topic'])]
    producer = topic.get_sync_producer()
    msg = { 
        "type": "requests_post_trade",
        "datetime" : datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "payload": body 
    }
    msg_str = json.dumps(msg)
    producer.produce(msg_str.encode('utf-8'))
    
    #request received log
    logger.info(f'Received event Account request with a trace id of {trace_id}')

    return msg

def retry_kafka_connect():
  hostname = "%s:%d" % (app_config['events']["hostname"], app_config['events']["port"])
  current_try = 0
  while current_try < 5:
    try:
      client = KafkaClient(hosts=hostname)
      topic = client.topics[str.encode(app_config['events']["topic"])]
      consumer = topic.get_simple_consumer(consumer_group=b'event_group', reset_offset_on_start=True, auto_offset_reset=OffsetType.LATEST, consumer_timeout_ms=100)
      break
    except Exception as e:
      logger.error("Error connecting to kafka %s" % e)
      current_try += 1

  if current_try == 5:
      logger.error("Failed to connect to kafka")
      exit(1)
  else:
      logger.info("Connected to kafka !!!")

def get_health():
  return {"status": "active"}, 200

app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api('receiver_api.yaml',strict_validation=True,validate_responses=True)

if __name__ == "__main__":
    app.run(port=8080, debug=True)