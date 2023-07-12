import mysql.connector


class Database(object):
    def __init__(self, host, port, user, password, databaseName):
        # property
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.databaseName = databaseName

    def connect(self):
        self.db = mysql.connector.connect(
            host=self.host,
            user=self.user,
            port=self.port,
            passwd=self.password,
            database=self.databaseName,
            autocommit=True,
        )

    def close(self):
        self.db.close()

    def select(self, query):
        cursor = self.db.cursor()
        cursor.execute(query)
        res = cursor.fetchall()
        cursor.close()
        return res

    def count(self, query):
        cursor = self.db.cursor()
        cursor.execute(query)
        res = cursor.fetchone()
        if res:
            return res[0]
        return 0

    def update(self, query):
        cursor = self.db.cursor()
        cursor.execute(query)
        self.db.commit()
        res = True if (cursor.rowcount > 0) else False
        cursor.close()
        return res

    def execute(self, query):
        cursor = self.db.cursor()
        cursor.execute(query)
        cursor.close()

    def insert(self, query):
        cursor = self.db.cursor()
        cursor.execute(query)
        self.db.commit()
        return cursor.lastrowid

    def delete(self, query):
        cursor = self.db.cursor()
        cursor.execute(query)
        self.db.commit()
        res = True if (cursor.rowcount > 0) else False
        cursor.close()
        return res
