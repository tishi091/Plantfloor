import mysql.connector
from mysql.connector import Error
from mysql.connector import errorcode
from mysql.connector import pooling
import time

class MySQLPool(object):

    def __init__(self, host_name, user_name, user_password):
        self.host_name = host_name
        self.user_name = user_name
        self.user_pass = user_password
        self.pool = self.__creteConnectionPool()

    def __creteConnectionPool(self):
        """
        Creates a connection pool for connecting to a MySQL database.

        This function attempts to establish a connection pool to a MySQL database. 
        It retries the connection up to a maximum number of times, with a specified delay between retries. 
        If the connection is successful, it returns a `MySQLConnectionPool` object. 
        If the connection fails after the maximum number of retries, it raises an exception with the error message.

        Parameters:
            None

        Returns:
            MySQLConnectionPool: A connection pool object for connecting to the MySQL database.

        Raises:
            Exception: If the connection fails after the maximum number of retries.
        """

        max_retries = 5 # Max number of retries
        retry_delay = 2 # Retry delay in seconds

        for i in range(max_retries):
            try:
                print("MySQL Database connection successful")
                return pooling.MySQLConnectionPool(
                    pool_name="myConnectionPool",
                    pool_size=10,
                    host=self.host_name,
                    user=self.user_name,
                    password=self.user_pass
                )
            except Exception as e:
                if i != max_retries - 1:
                    print(f"Failed to connect to MySQL. Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    raise Exception(f"Failed to connect to MySQL after {max_retries} retries. Error: {e}")
                
    def getConnection(self):
        return self.pool.get_connection()
    
    