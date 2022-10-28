from sqlalchemy import create_engine, MetaData
import mysql.connector, app_conf as cfg
import sqlalchemy

db_con = f"mysql+pymysql://{cfg.datastore['user']}:{cfg.datastore['password']}@{cfg.datastore['hostname']}:{cfg.datastore['port']}/{cfg.datastore['db']}"
engine = create_engine(db_con, echo=True, future=True)
metadata = MetaData(engine)
connection = engine.connect()

#drop table
# engine.execute(f"DROP table IF EXISTS {}")


#insert or update => upsert
# insert_stmt = insert(my_table).values(
#     id='some_existing_id',
#     data='inserted value'
# )
# on_duplicate_key_stmt = insert_stmt.on_duplicate_key_update(
#     data=insert_stmt.inserted.data,
#     status='U'
# )

