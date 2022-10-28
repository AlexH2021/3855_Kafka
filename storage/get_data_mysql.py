from sqlalchemy import create_engine, select, MetaData, Table
import app_conf as cfg

db_con = f"mysql+pymysql://{cfg.datastore['user']}:{cfg.datastore['password']}@{cfg.datastore['hostname']}:{cfg.datastore['port']}/{cfg.datastore['db']}"
engine = create_engine(db_con, echo=True, future=True)
metadata = MetaData(bind=None)

account = Table('account',metadata, 
    autoload=True, 
    autoload_with=engine
)
trade = Table('trade',metadata, 
    autoload=True, 
    autoload_with=engine
)

stmt = select([account])
stmt1 = select([trade])

connection = engine.connect()
results = connection.execute(stmt).fetchall()
results += connection.execute(stmt1).fetchall()
print(*results,sep='\n')
