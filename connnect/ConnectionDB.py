import psycopg2


class ConnectionDB:

    def __init__(self):
        self.connection = psycopg2.connect(user="postgres",
                                           password="postgres",
                                           host="127.0.0.1",
                                           port="5432",
                                           database="articles_db")
        self.cursor = self.connection.cursor()

    def close(self):
        self.cursor.close()
        self.connection.close()

    def commit(self):
        self.connection.commit()
