import yaml, os, logging.config, json, connexion, requests
from flask_cors import CORS
from multiprocessing import Pool
from datetime import datetime
from pathlib import Path

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

SERVICEOBJECT = app_config['services']

def retrieved_health_status(serviceObject):
  url = app_config['services'][serviceObject]

  try:
    request = requests.get(url, timeout=5)
    if request.status_code != 200:
      return {serviceObject: "Not active"}
    return {serviceObject: "Active"}
  except requests.exceptions.RequestException as e:
    return {serviceObject: "Not active"}

def write_to_json(new_data):
  filename = app_config['datastore']['filename']
  fle = Path(filename)
  fle.touch(exist_ok=True)

  print(fle)
  print("-----------")
  print(new_data)

  with open(filename, 'r+') as f:
    file_data = json.load(f)
    file_data.append(new_data)
    f.seek(0)
    json.dump(file_data, f, indent=4)
  

def health_check():
  current_time = {'Last Update': str(datetime.now().replace(microsecond=0))}
  data = {}
  
  with Pool() as p:
    status = p.map(retrieved_health_status, SERVICEOBJECT)
  
  logger.info("Performing health check on microservices ...")

  for i in status:
    data.update(i)
  
  data.update(current_time)
  write_to_json(data)

  logger.info("Health check completed: %", data)

  return data, 200

app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api('healthcheck_api.yaml', strict_validation=True, validate_responses=True)

CORS(app.app)
app.app.config['CORS_HEADERS'] = 'Content-Type'

if __name__ == "__main__":
  app.run(port=8120)