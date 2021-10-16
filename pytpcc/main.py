import time 
from multiprocessing.pool import *
import psycopg2



config = {
        "host":         ("The hostname to postgresql", "localhost" ),
        "port":         ("The port number to postgresql", 5432 ),
        "dbname":         ("Database name", "postgres"),
        "user":         ("user of the database", "postgres"),
        "password":     ("the password", "postgres")
    }

duration = 10
client = 3
sql = "insert into a values('3');"

def execute_sql(sql):
    start = time.time()
    conn = psycopg2.connect(database=config["dbname"][1], user=config["user"][1], password=config["password"][1], host=config["host"][1], port=config["port"][1])
    cursor = conn.cursor()
    while time.time()-start<duration:
        cursor.execute(sql)
    conn.commit()
    conn.close()


if __name__ == '__main__':
    pool = Pool(client)
    for i in range(client):
        pool.apply_async(execute_sql,(sql,))
    pool.close()
    pool.join()


