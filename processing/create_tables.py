from sqlalchemy import create_engine, MetaData
from accounts_stats import Account_Stats
from trade_stats import Trade_Stats
import yaml

with open("app_conf.yml", "r") as f:
    cfg = yaml.safe_load(f.read())

url = f"sqlite:///{cfg['datastore']['filename']}"
engine = create_engine(url, echo=True, future=True)
connection = engine.connect()
metadata = MetaData(engine)


Account_Stats.metadata.create_all(engine)
Trade_Stats.metadata.create_all(engine)
