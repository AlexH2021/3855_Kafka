from datetime import datetime
import json
from time import strptime
import connexion, logging.config, yaml, requests
from sqlalchemy import create_engine
from apscheduler.schedulers.background import BackgroundScheduler
from base import Base
from sqlalchemy.orm import sessionmaker

from accounts_stats import Account_Stats
from trade_stats import Trade_Stats

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

def post_acc():
  pass

def post_trade():
  pass

def get_trade_stats():
  pass

def get_acc_stats():
  pass

def sent_acc_get_request():
  url = ACC_STATS_URL + '?timestamp=' + str(datetime.now().replace(microsecond=0))
  response = requests.get(url)

  return response.status_code, response.json()['content']

def sent_trade_get_request():
  url = TRADE_STATS_URL + '?timestamp=' + str(datetime.now().replace(microsecond=0))
  # url = TRADE_STATS_URL + '?timestamp=' + str("2012-10-10 12:12:12")
  response = requests.get(url)
  
  return response.status_code, response.json()['content']

def calculate_acc_stats_data():
  status_code, data = sent_acc_get_request()

  store_data = {}
  processed_data = {}

  for rows in data:
    for i in rows:
      if i not in store_data:
        store_data[i] = [rows[i]]
      else:
        store_data[i].append(rows[i])
      
  if store_data:
    processed_data = {
      'total_acc_num': len(store_data['accountID']),
      'total_cash': sum(store_data['cash']),
      'total_value': sum(store_data['value']),
      'traceID': store_data['traceID']
    }

  return status_code, processed_data

def calculate_trade_stats_data():
  status_code, data = sent_trade_get_request()

  store_data = {}
  processed_data = {}

  for rows in data:
    for i in rows:
      if i not in store_data:
        store_data[i] = [rows[i]]
      else:
        store_data[i].append(rows[i])
  
  if store_data:
    processed_data = {
      'total_trade_num': len(store_data['tradeID']),
      'total_share': sum(store_data['shares']),
      'total_price': sum(store_data['price']),
      'traceID': store_data['traceID']
    }
  
  # print(data)
  # print("-------------------------------------")
  # print(store_data)
  # print("-------------------------------------")
  # print(processed_data)

  return status_code, processed_data

def save_acc_stats_data(body):
  session = DB_SESSION()
  session.expire_on_commit = False

  data = Account_Stats(
    body['total_acc_num'], 
    body['total_cash'],
    body['total_value'],
    str(body['traceID'])
  )

  id = session.add(data)
  session.commit()
  session.close()

  return id

def save_trade_stats_data(body):
  session = DB_SESSION()
  session.expire_on_commit = False

  data = Trade_Stats(
    body['total_trade_num'], 
    body['total_share'],
    body['total_price'],
    str(body['traceID'])
  )

  id = session.add(data)
  session.commit()
  session.close()

  return id

def populate_stats():
  ### this function will keep running base on the schedule
  logger.info("Start Periodic Processing")

  _, acc_data_with_traceID = sent_acc_get_request()
  _, trade_data_with_traceID = sent_trade_get_request()

  status_code, calculated_data = calculate_acc_stats_data()
  status_code2, calculated_data2 = calculate_trade_stats_data()

  if calculated_data:
    if status_code == 200:
      ## account data
      logger.info("Query for Account readings after %s returns %d results" % (datetime.now().replace(microsecond=0), len(calculated_data)))
      logger.debug(f'INFO: Account event with traceID: {acc_data_with_traceID}')
      save_acc_stats_data(calculated_data)
      logger.debug(f'INFO: Account stats: {calculated_data}')
    else:
      logger.error('Failed! query for Account readings after %s' % (datetime.now().replace(microsecond=0)))

  if calculated_data2:
    if status_code2 == 200:
      ## trade data
      logger.info("Query for Trade readings after %s returns %d results" % (datetime.now().replace(microsecond=0), len(calculated_data2)))
      logger.debug(f'INFO: Account event with traceID: {trade_data_with_traceID}')
      save_trade_stats_data(calculated_data2)
      logger.debug(f'INFO: Trade stats: {calculated_data2}')
    else:
      logger.error('Failed! query for Trade readings after %s' % (datetime.now().replace(microsecond=0)))
  
  logger.info(f'INFO: finish populating stats')
    
def init_scheduler():
  sched = BackgroundScheduler(daemon=True)
  sched.add_job(populate_stats, 'interval', seconds=app_config['scheduler']['period_sec'])
  sched.start()

app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api('processing_api.yaml',strict_validation=True,validate_responses=True)


if __name__ == "__main__":
  init_scheduler()
  app.run(port=8100, use_reloader=False)
  # code, test = calculate_trade_stats_data()
  # print(code, test)
  # a = save_trade_stats_data(test)
  # print(a)
  # sent_acc_get_request()