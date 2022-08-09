import snowflake.connector

conn = snowflake.connector.connect(
    user='ndx',
    password='secretxXx666',
    account='oi17984.eu-north-1.aws',
    warehouse='COMPUTE_WH',
    database='INNOWISE_TASK_6',
    schema='TEST_SCHEME'
)

curs=conn.cursor()
#execute SQL statement
curs.execute('select * from raw')
#fetch result
print(curs.fetchone())
