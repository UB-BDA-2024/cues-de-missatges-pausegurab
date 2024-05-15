import psycopg2
import os


class Timescale:
    def __init__(self):
        self.conn = psycopg2.connect(
            host=os.environ.get("TS_HOST"),
            port=os.environ.get("TS_PORT"),
            user=os.environ.get("TS_USER"),
            password=os.environ.get("TS_PASSWORD"),
            database=os.environ.get("TS_DBNAME"))
        self.cursor = self.conn.cursor()
        
    def getCursor(self):
            return self.cursor

    def close(self):
        self.cursor.close()
        self.conn.close()
    
    def ping(self):
        try:
            self.cursor.execute("SELECT 1")
            return True
        except psycopg2.OperationalError:
            return False
    
    def execute(self, query):
       return self.cursor.execute(query)
    
    def delete(self, table):
        self.cursor.execute("DELETE FROM " + table)
        self.conn.commit()

    def commit(self):
        self.cursor.execute("commit")


        
     
         