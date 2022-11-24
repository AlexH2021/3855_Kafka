import connexion, yaml, logging.config, json
from pykafka import KafkaClient
from flask_cors import CORS, cross_origin

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


def get_account_reading(index):
  """ Get Account Reading in History """
  hostname = "%s:%d" % (app_config['events']["hostname"], app_config['events']["port"])
  client = KafkaClient(hosts=hostname)
  topic = client.topics[str.encode(app_config['events']["topic"])]
  consumer = topic.get_simple_consumer(reset_offset_on_start=True, consumer_timeout_ms=1000)
  logger.info("Retrieving Account at index %d" % index)
  
  start = 1
  result = {}
  try:
    for msg in consumer:
      msg_str = msg.value.decode('utf-8')
      msg = json.loads(msg_str)
      if msg['type'] == 'requests_post_acc':
        result[start] = msg
        start += 1
    return result[index], 200
  except:
    logger.error("No more messages found")

  logger.error("Could not find Account at index %d" % index)
  
  return { "message": "Not Found"}, 404

def get_trade_reading(index):
  hostname = "%s:%d" % (app_config['events']["hostname"], app_config['events']["port"])
  client = KafkaClient(hosts=hostname)
  topic = client.topics[str.encode(app_config['events']["topic"])]
  consumer = topic.get_simple_consumer(reset_offset_on_start=True, consumer_timeout_ms=1000)
  logger.info("Retrieving Trade at index %d" % index)

  start = 1
  result = {}
  try:
    for msg in consumer:
      msg_str = msg.value.decode('utf-8')
      msg = json.loads(msg_str)
      if msg['type'] == 'requests_post_trade':
        result[start] = msg
        start += 1
    return result[index], 200
  except:
    logger.error("No more messages found")

  logger.error("Could not find Account at index %d" % index)
  
  return { "message": "Not Found"}, 404

def get_health():
  return {"status": "active"}, 200

app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api('audit_api.yaml',strict_validation=True,validate_responses=True,base_path="/audit_log")

if "TARGET_ENV" not in os.environ or os.environ["TARGET_ENV"] != "test":
  CORS(app.app)
  app.app.config['CORS_HEADERS'] = 'Content-Type'

if __name__ == "__main__":
    app.run(port=8110,debug=True)
