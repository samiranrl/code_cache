import psycopg2, sqlalchemy as sa, pandas as pd, time, sys
from sqlalchemy.orm import sessionmaker

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 200)
pd.set_option('display.max_colwidth', -1)


from tqdm import tqdm

ql = pd.read_csv('some_list.csv', sep = '|')['some_column']
ql = ql[ql.apply(lambda x:"'" not in x)]
sq = ','.join(ql.apply(lambda x: "'"+x.title()+"'"))


username = 'username'
password = 'password'

connstr = 'redshift+psycopg2://'+username+':'+password+'@<some_company>-dev.<some_random_number>.us-west-2.redshift.amazonaws.com:<some_port>/<some_database>'
engine = sa.create_engine(connstr)



start_time = time.time()
with engine.connect() as conn, conn.begin():
    query_total_count = "sql_query"
    print (query_total_count)
    df = pd.read_sql(query_total_count, conn) # Query results in a dataframe
print("--- %s seconds ---" % (time.time() - start_time))
