import psycopg2


if __name__=='__main__':
    conn = psycopg2.connect(database="postgres", user="postgres", password="ppostgres", host="162.105.88.171", port="5432")
    cursor = conn.cursor()
    cursor.execute("")