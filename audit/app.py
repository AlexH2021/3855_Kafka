import connexion, yaml, logging.config, app_conf as cfg, json
from pykafka import KafkaClient

# read log config yml
with open('log_conf.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basicLogger')

def get_account_reading(index):
  """ Get Account Reading in History """
  hostname = "%s:%d" % (cfg.events["hostname"], cfg.events["port"])
  client = KafkaClient(hosts=hostname)
  topic = client.topics[str.encode(cfg.events["topic"])]
  consumer = topic.get_simple_consumer(reset_offset_on_start=True, consumer_timeout_ms=1000)
  logger.info("Retrieving Account at index %d" % index)

  try:
    for msg in consumer:
      msg_str = msg.value.decode('utf-8')
      msg = json.loads(msg_str)
      # if msg["accountID"] == index:
      return msg, 200
  except:
    logger.error("No more messages found")

  logger.error("Could not find Account at index %d" % index)
  
  return { "message": "Not Found"}, 404

def get_trade_reading(index):
  hostname = "%s:%d" % (cfg.events["hostname"], cfg.events["port"])
  client = KafkaClient(hosts=hostname)
  topic = client.topics[str.encode(cfg.events["topic"])]
  consumer = topic.get_simple_consumer(reset_offset_on_start=True, consumer_timeout_ms=1000)
  logger.info("Retrieving Trade at index %d" % index)

  try:
    for msg in consumer:
      msg_str = msg.value.decode('utf-8')
      msg = json.loads(msg_str)
      # if msg["accountID"] == index:
      return msg, 200
  except:
    logger.error("No more messages found")

  logger.error("Could not find Account at index %d" % index)
  
  return { "message": "Not Found"}, 404

app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api('audit_api.yaml',strict_validation=True,validate_responses=True)

if __name__ == "__main__":
    app.run(port=8110)
