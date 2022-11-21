from datetime import datetime
import connexion, app_conf as cfg, logging.config, yaml, json
from sqlalchemy import create_engine, and_
from sqlalchemy.orm import sessionmaker
from base import Base
from accounts import Account
from trades import Trade
from pykafka import KafkaClient
from pykafka.common import OffsetType
from threading import Thread
from pykafka.exceptions import SocketDisconnectedError, LeaderNotAvailable

# read log config yml
with open('log_conf.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basicLogger')

db_con = f"mysql+pymysql://{cfg.datastore['user']}:{cfg.datastore['password']}@{cfg.datastore['hostname']}:{cfg.datastore['port']}/{cfg.datastore['db']}"
DB_ENGINE = create_engine(db_con)
Base.metadata.bind = DB_ENGINE
DB_SESSION = sessionmaker(bind=DB_ENGINE)
logger.info(f"Connecting to DB. Hostname: {cfg.datastore['hostname']}, Port: {cfg.datastore['port']}")

def post_acc(body):
  session = DB_SESSION()

  acc = Account(
      body['accountID'],
      body['holding'],
      body['cash'],
      body['value'],
      body['accountType'],
      body['currencyID'],
      body['createdAt'],
      body['traceID']
  )
  logger.info(f"Stored event added Account data request with a trace id of {acc.traceID}")

  session.add(acc)
  session.commit()
  session.close()

def post_trade(body):
  session = DB_SESSION()

  trade = Trade(
      body['tradeID'], 
      body['tradeType'],
      body['symbol'],
      body['shares'],
      body['price'],
      body['createdAt'],
      body['accountID'],
      body['traceID']
  )
  logger.info(f"Stored event added Trade data request with a trace id of {trade.traceID}")

  session.add(trade)
  session.commit()
  session.close()

def get_acc_stats(start_timestamp, end_timestamp):
  session = DB_SESSION()

  start_timestamp_datetime = datetime.strptime(start_timestamp, "%Y-%m-%d %H:%M:%S")
  end_timestamp_datetime = datetime.strptime(end_timestamp, "%Y-%m-%d %H:%M:%S")

  readings = session.query(Account).filter(and_(Account.createdAt >= start_timestamp_datetime, Account.createdAt < end_timestamp_datetime))

  result_list = [reading.to_dict() for reading in readings]

  session.close()

  logger.info("Query for Account readings after %s returns %d results" % (start_timestamp, len(result_list)))

  success_message = {
      'message': 'account stats',
      'status': 200,
      'content': result_list
  }

  return success_message

def get_trade_stats(start_timestamp, end_timestamp):
  session = DB_SESSION()
  session.expire_on_commit = False

  start_timestamp_datetime = datetime.strptime(start_timestamp, "%Y-%m-%d %H:%M:%S")
  end_timestamp_datetime = datetime.strptime(end_timestamp, "%Y-%m-%d %H:%M:%S")
  
  readings = session.query(Trade).filter(and_(Trade.createdAt >= start_timestamp_datetime, Trade.createdAt < end_timestamp_datetime))

  result_list = [reading.to_dict() for reading in readings]

  session.close()

  logger.info("Query for Trade readings after %s returns %d results" % (start_timestamp, len(result_list)))

  success_message = {
      'message': 'trade stats',
      'status': 200,
      'content': result_list
  }

  return success_message

def process_messages():
  hostname = "%s:%d" % (cfg.events["hostname"], cfg.events["port"])
  client = KafkaClient(hosts=hostname)
  topic = client.topics[str.encode(cfg.events["topic"])]
  
  consumer = topic.get_simple_consumer(consumer_group=b'event_group', reset_offset_on_start=False, auto_offset_reset=OffsetType.LATEST)
  try:
    consumer.consume()
  except (SocketDisconnectedError) as e:
    consumer = topic.get_simple_consumer(consumer_group=b'event_group', reset_offset_on_start=False, auto_offset_reset=OffsetType.LATEST)
    # use either the above method or the following:
    # consumer.stop()
    # consumer.start()

  for msg in consumer:
    msg_str = msg.value.decode('utf-8')
    msg = json.loads(msg_str)
    logger.info("Message: %s" % msg)
    payload = msg["payload"]

    if msg["type"] == "requests_post_acc": # Change this to your event type
      post_acc(payload)
    elif msg["type"] == "requests_post_trade": # Change this to your event type
      post_trade(payload)
    
    # Commit the new message as being read
    consumer.commit_offsets()

def retry_kafka_connect():
  hostname = "%s:%d" % (cfg.events["hostname"], cfg.events["port"])
  current_try = 0
  while current_try < 5:
    try:
      client = KafkaClient(hosts=hostname)
      topic = client.topics[str.encode(cfg.events["topic"])]
      consumer = topic.get_simple_consumer(consumer_group=b'event_group', reset_offset_on_start=True, auto_offset_reset=OffsetType.LATEST, consumer_timeout_ms=100)
      try:
        consumer.consume()
      except (SocketDisconnectedError) as e:
        consumer = topic.get_simple_consumer(consumer_group=b'event_group', reset_offset_on_start=True, auto_offset_reset=OffsetType.LATEST, consumer_timeout_ms=100)
      break
    except Exception as e:
      logger.error("Error connecting to kafka %s" % e)
      current_retry += 1

  if current_retry == 5:
      logger.error("Failed to connect to kafka")
      exit(1)
  else:
      logger.info("Connected to kafka !!!")

app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api('storage_api.yaml',strict_validation=True,validate_responses=True)

if __name__ == "__main__":
  retry_kafka_connect()
  t1 = Thread(target=process_messages)
  t1.setDaemon(True)
  t1.start()
  app.run(port=8090, debug=True)