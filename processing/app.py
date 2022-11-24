from datetime import datetime, timedelta
import connexion, logging.config, yaml, requests, json, uuid
from sqlalchemy import create_engine
from apscheduler.schedulers.background import BackgroundScheduler
from base import Base
from sqlalchemy.orm import sessionmaker
from flask_cors import CORS, cross_origin

from stats import Stats

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

ACC_STATS_URL = app_config['eventstore']['acc_stats_url']
TRADE_STATS_URL = app_config['eventstore']['trade_stats_url']
SQLITE_URL = f"sqlite:///{app_config['datastore']['filename']}"

DB_ENGINE = create_engine(SQLITE_URL)
Base.metadata.bind = DB_ENGINE
DB_SESSION = sessionmaker(bind=DB_ENGINE)

def get_stats():
    session = DB_SESSION()
    readings = session.query(Stats).order_by(Stats.created_at.desc()).all()
    session.close()

    result_list = [reading.as_dict() for reading in readings]
    num_account = 0
    num_trade = 0 
    total_cash = 0 
    total_value = 0 
    for row in result_list:
      num_account += row['num_account']
      num_trade += row['num_trade']
      total_cash += row['total_cash']
      total_value += row['total_value']

    return { "num_account": num_account, "num_trade": num_trade, "total_cash": total_cash, "total_value": total_value }

def sent_acc_get_request(start_time, current_time):
  url = ACC_STATS_URL + '?start_timestamp=' + start_time + '&end_timestamp=' + current_time
  # url = ACC_STATS_URL + "?timestamp=2012-10-10 12:12:12"
  response = requests.get(url)
  
  if response.status_code == 204:
    return 204, []
  elif response.status_code == 400:
    return 400, []
  elif response.status_code == 500: 
    return 500, []

  return response.status_code, response.json()['content']

def sent_trade_get_request(start_time, current_time):
  url = TRADE_STATS_URL + '?start_timestamp=' + start_time + '&end_timestamp=' + current_time
  # url = TRADE_STATS_URL + "?timestamp=2012-10-10 12:12:12"
  response = requests.get(url) 
  
  if response.status_code == 204:
    return 204, []
  elif response.status_code == 400:
    return 400, []
  elif response.status_code == 500: 
    return 500, []
  
  return response.status_code, response.json()['content']

def cal_stats():
  start_time =  app_config['scheduler']['start_time']
  current_time = str(datetime.now().replace(microsecond=0))

  status1, acc_data = sent_acc_get_request(start_time, current_time)
  status2, trade_data = sent_trade_get_request(start_time, current_time)
  traceID = str(uuid.uuid4())

  processed_data = {
    "num_account": 0,
    "num_trade": 0,
    "total_cash": 0,
    "total_value": 0,
    "total_share": 0
  }
  have_data = False

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
    have_data = True

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

  print(merge_data)


  app_config['scheduler']['start_time'] = current_time
  with open('app_conf.yml', 'w') as f:
    yaml.dump(app_config, f)
  logger.debug(f'INFO: updated start time: {current_time}')

  return processed_data, have_data

def save_to_sqlite(body):
  session = DB_SESSION()
  body["created_at"] = datetime.strptime(str(datetime.now().replace(microsecond=0)), '%Y-%m-%d %H:%M:%S')

  data = Stats(
    body['num_account'],
    body['num_trade'],
    body['total_cash'], 
    body['total_value'],
    body['total_share'],
    body['created_at'],
  )

  id = session.add(data)
  session.commit()
  session.close()

  return id

def populate_stats():
  ### this function will keep running base on the schedule
  logger.info("Start Periodic Processing")

  calculated_data, have_data = cal_stats()
  
  # print(calculated_data, have_data)

  if have_data == True:
    save_to_sqlite(calculated_data)
  else:
    logger.info("INFO: No data to insert to database")

  logger.info(f'INFO: finish populating stats')
    
def init_scheduler():
  sched = BackgroundScheduler(daemon=True)
  sched.add_job(populate_stats, 'interval', seconds=app_config['scheduler']['period_sec'])
  sched.start()

def get_health():
  return {"status": "active"}, 200

app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api('processing_api.yaml',strict_validation=True,validate_responses=True,base_path="/processing")
CORS(app.app)
app.app.config['CORS_HEADERS'] = 'Content-Type'

if __name__ == "__main__":
  init_scheduler()
  app.run(port=8100, use_reloader=False)