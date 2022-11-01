from datetime import datetime
import connexion, app_conf as cfg, logging.config, yaml, json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from base import Base
from accounts import Account
from trades import Trade
from pykafka import KafkaClient
from pykafka.common import OffsetType
from threading import Thread

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

    session.add(acc)
    session.commit()
    session.close()

    logger.info(f"Stored event added Account data request with a trace id of {acc['traceID']}")

    return body

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

    session.add(trade)
    session.commit()
    session.close()

    return body

def get_acc_stats(timestamp):
    session = DB_SESSION()

    timestamp_datetime = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
    
    readings = session.query(Account).filter(Account.createdAt >= timestamp_datetime)

    result_list = [reading.to_dict() for reading in readings]

    session.close()

    logger.info("Query for Account readings after %s returns %d results" % (timestamp, len(result_list)))

    success_message = {
        'message': 'account stats',
        'status': 200,
        'content': result_list
    }

    return success_message

def get_trade_stats(timestamp):
    session = DB_SESSION()
    session.expire_on_commit = False

    timestamp_datetime = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
    
    readings = session.query(Trade).filter(Trade.createdAt >= timestamp_datetime)

    result_list = [reading.to_dict() for reading in readings]

    session.close()

    logger.info("Query for Trade readings after %s returns %d results" % (timestamp, len(result_list)))

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

    for msg in consumer:
        msg_str = msg.value.decode('utf-8')
        msg = json.loads(msg_str)
        logger.info("Message: %s" % msg)
        payload = msg["payload"]
        
        if msg["type"] == "requests_post_acc": # Change this to your event type
            post_acc(payload)
        elif msg["type"] == "requests_post_trade": # Change this to your event type
            # Store the event2 (i.e., the payload) to the DB
            post_trade(payload)
        
        # Commit the new message as being read
        consumer.commit_offsets()
    
app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api('storage_api.yaml',strict_validation=True,validate_responses=True)

if __name__ == "__main__":
    t1 = Thread(target=process_messages)
    t1.setDaemon(True)
    t1.start()
    app.run(port=8090, debug=True)