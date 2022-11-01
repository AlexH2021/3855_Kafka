from datetime import datetime, timedelta
from re import T
import uuid
import connexion, logging.config, yaml, requests
from sqlalchemy import create_engine
from apscheduler.schedulers.background import BackgroundScheduler
from base import Base
from sqlalchemy.orm import sessionmaker

from stats import Stats

with open('app_conf.yml', 'r') as f:
  app_config = yaml.safe_load(f.read())

ACC_STATS_URL = app_config['eventstore']['acc_stats_url']
TRADE_STATS_URL = app_config['eventstore']['trade_stats_url']
SQLITE_URL = f"sqlite:///{app_config['datastore']['filename']}"

DB_ENGINE = create_engine(SQLITE_URL)
Base.metadata.bind = DB_ENGINE
DB_SESSION = sessionmaker(bind=DB_ENGINE)

with open('log_conf.yml', 'r') as f:
  log_config = yaml.safe_load(f.read())
  logging.config.dictConfig(log_config)

logger = logging.getLogger('basicLogger')

def get_stats():
    session = DB_SESSION()
    results = session.query(Stats).order_by(Stats.last_updated.desc())
    session.close()

    return results

def sent_acc_get_request():
  url = ACC_STATS_URL + '?timestamp=' + str((datetime.now()-timedelta(0,5)).replace(microsecond=0))
  # url = ACC_STATS_URL + "?timestamp=2012-10-10 12:12:12"
  response = requests.get(url)
  
  if response.status_code == 204:
    return 204, {}
  elif response.status_code == 400:
    return 400, {}

  return response.status_code, response.json()['content']

def sent_trade_get_request():
  url = TRADE_STATS_URL + '?timestamp=' + str((datetime.now()-timedelta(0,5)).replace(microsecond=0))
  # url = TRADE_STATS_URL + "?timestamp=2012-10-10 12:12:12"

  response = requests.get(url) 
  
  if response.status_code == 204:
    return 204, {}
  elif response.status_code == 400:
    return 400, {}
  
  return response.status_code, response.json()['content']

def cal_stats():
  status1, acc_data = sent_acc_get_request()
  status2, trade_data = sent_trade_get_request()
  traceID = str(uuid.uuid4())


  processed_data = {
    "num_account": 0,
    "num_trade": 0,
    "total_cash": 0,
    "total_value": 0,
    "total_share": 0
  }

  merge_data = acc_data + trade_data

  if merge_data:
    for row in merge_data:
      for key in row:
        if key == "accountID":
          processed_data['num_account'] += 1
        elif key == "tradeID":
          processed_data['num_trade'] += 1
        elif key == "cash":
          processed_data['total_cash'] += row[key]
        elif key == "value":
          processed_data['total_value'] += row[key]
        elif key == "shares":
          processed_data['total_share'] += row[key]

    logger.info('Number of account events received %d at %s', len(acc_data), str(datetime.now().replace(microsecond=0)))
    if status1 != 200:
        logger.error('Error retrieving account stats: %d', acc_data)
    elif status1 == 204:
        logger.error('No account stats available')
    
    logger.info('Number of trade events received: %d at %s', len(trade_data), str(datetime.now().replace(microsecond=0)))
    if status2 != 200:
        logger.error('Error retrieving trade stats: %d', trade_data)
    elif status2 == 204:
        logger.error('No trade stats available')

    #log debug
    logger.debug('TraceID for account and trade stats: %s',traceID)

  return processed_data

def save_to_sqlite(body):
  session = DB_SESSION()
  session.expire_on_commit = False

  data = Stats(
    body['num_account'],
    body['num_trade'],
    body['total_cash'], 
    body['total_value'],
    body['total_share'],
  )

  id = session.add(data)
  session.commit()
  session.close()

  return id

def populate_stats():
  ### this function will keep running base on the schedule
  logger.info("Start Periodic Processing")

  calculated_data = cal_stats()

  if calculated_data:
    save_to_sqlite(calculated_data)
    print(len(calculated_data))
  else:
    print("No data")
    logger.info("INFO: No data to insert to database")
  
  logger.info(f'INFO: finish populating stats')
    
def init_scheduler():
  sched = BackgroundScheduler(daemon=True)
  sched.add_job(populate_stats, 'interval', seconds=app_config['scheduler']['period_sec'])
  sched.start()

app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api('processing_api.yaml',strict_validation=True,validate_responses=True)


if __name__ == "__main__":
  init_scheduler()
  app.run(port=8100, use_reloader=False, debug=True)