from sqlalchemy import create_engine, MetaData
from sqlalchemy_utils import database_exists, create_database
from accounts import Account
from trades import Trade

import os, yaml
if "TARGET_ENV" in os.environ and os.environ["TARGET_ENV"] == "test":
  print("In Test Environment")
  app_conf_file = "/config/app_conf.yml"
else:
  print("In Dev Environment")
  app_conf_file = "app_conf.yml"

with open(app_conf_file, 'r') as f:
  app_config = yaml.safe_load(f.read())


db_con = f"mysql+pymysql://{app_config['datastore']['user']}:{app_config['datastore']['password']}@{app_config['datastore']['hostname']}:{app_config['datastore']['port']}/{app_config['datastore']['db']}"
engine = create_engine(db_con, echo=True, future=True)

if not database_exists(engine.url):
    create_database(engine.url)
else:
  # Connect the database if exists.
  connection = engine.connect()
  
metadata = MetaData(engine)

Account.metadata.create_all(engine)
Trade.metadata.create_all(engine)