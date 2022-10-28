from sqlalchemy import create_engine, MetaData
from sqlalchemy_utils import database_exists, create_database
import app_conf as cfg
from accounts import Account
from trades import Trade

db_con = f"mysql+pymysql://{cfg.datastore['user']}:{cfg.datastore['password']}@{cfg.datastore['hostname']}:{cfg.datastore['port']}/{cfg.datastore['db']}"
engine = create_engine(db_con, echo=True, future=True)

if not database_exists(engine.url):
    create_database(engine.url)
else:
  # Connect the database if exists.
  connection = engine.connect()
  
metadata = MetaData(engine)

Account.metadata.create_all(engine)
Trade.metadata.create_all(engine)