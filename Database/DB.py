import mysql.connector
from mysql.connector import Error
from threading import Lock
import mySQL
import datetime

# Establish a database connection
class Database(object):

    def __init__(self, host_name, user_name, user_password):
        self.host_name = host_name
        self.user_name = user_name
        self.user_password = user_password
        self.pool = self.createMySQLPool(host_name, user_name, user_password)
        self.dbname = None

    def createMySQLPool(self, host_name, user_name, user_password):
        """
        Create a MySQL connection pool.

        Args:
            host_name (str): The host name of the MySQL server.
            user_name (str): The username for the MySQL connection.
            user_password (str): The password for the MySQL connection.

        Returns:
            MySQLPool: A MySQL connection pool object.
        """
        return mySQL.MySQLPool(host_name, user_name, user_password)

    # Function to execute SQL queries
    def executeQuery(self, query, dbname = False):
        """
        Executes a SQL query using the provided connection pool.

        Args:
            query (str): The SQL query to be executed.
            dbname (bool, optional): If True, the database name will be set using the `USE` statement. Defaults to False.

        Returns:
            list: A list of tuples containing the result of the query.

        Raises:
            Error: If an error occurs during the execution of the query.

        Note:
            The function automatically commits the changes and closes the cursor.
        """
        with self.pool.getConnection() as conn:
            with conn.cursor() as cursor:
                if dbname:
                    cursor.execute(f"USE {self.dbname}")
                try:
                    cursor.execute(query)
                    result = cursor.fetchall()
                    conn.commit()
                    # print("Query executed successfully")
                    print(query)
                except Error as err:
                    print(f"[Query] Error: '{err}'")
                finally:
                    if (cursor):
                        cursor.close()
                        return result

    # Thread-safe function to access the database
    def threadSafeAccess(self, query, dbname = False):
        """
        Executes a SQL query in a thread-safe manner.

        Args:
            query (str): The SQL query to be executed.
            dbname (bool, optional): Whether to use the database name in the query. Defaults to False.

        Returns:
            None

        This function acquires a lock to ensure thread safety when executing a SQL query. It first acquires the lock, then executes the query using the `execute_query` method. Finally, it releases the lock.

        Note:
            The `execute_query` method is assumed to be defined in the same class as this method.

        Raises:
            No exceptions are raised by this function.
        """
        lock = Lock()
        lock.acquire()
        try:
            self.executeQuery(query, dbname)
        finally:
            lock.release()

    def changeDatabase(self, dbname):
        self.dbname = dbname

    # Function to create a table
    def create(self, type, name, body = '', database = None):
        try:
            if database is not None and database != self.dbname:
                self.changeDatabase(database)
            query = f"CREATE {type} IF NOT EXISTS {name} {body}"
            if(type != "DATABASE"):
                self.executeQuery(query, dbname=True)
            else:
                self.dbname = name
                self.executeQuery(query)
            print(f"{type.title()} created successfully")
        except Error as err:
            print(f"[Create] Error: '{err}'")

    # Function to insert data into the table
    def insertData(self, table, body, database = None):
        if database is not None and database != self.dbname:
            self.changeDatabase(database)
        try:
            query = f"INSERT INTO {table} {body}"
            # print(query)
            self.executeQuery(query, dbname=True)
            print("Data inserted successfully")
        except Error as err:
            print(f"[Insert] Error: '{err}'")

    # Function to read data from the table
    def readData(self, columns, table, body = "", database = None):
        result = None
        if database is not None and database != self.dbname:
            self.changeDatabase(database)
        try:
            query = f"SELECT {columns} FROM {table} {body}"
            print(query)
            result = self.executeQuery(query, dbname=True)
            print("Data read successfully, result: ", result)
            return result
        except Error as err:
            print(f"[Read] Error: '{err}'")

    # Function to update data in the table
    def updateData(self, table, updates, body = "", limit = 1, database = None):
        if database is not None and database != self.dbname:
            self.changeDatabase(database)
        try:
            self.executeQuery("SET SQL_SAFE_UPDATES = 0")
            query = f"UPDATE {table} SET {updates} {body} LIMIT {limit}"
            print(query)
            self.threadSafeAccess(query, dbname=True)
            self.executeQuery("SET SQL_SAFE_UPDATES = 1")
            print("Data updated successfully")
        except Error as err:
            print(f"[Update] Error: '{err}'")

    # Function to delete data from the table
    def delete(self, type = None, name = None, From = None, body = None, limit = 1, database = None):
        if database is not None and database != self.dbname:
            self.changeDatabase(database)
        try:
            self.executeQuery("SET SQL_SAFE_UPDATES = 0")
            if type == "TABLE":
                query = f"DROP {type} IF EXISTS {name}"
            elif type == "COLUMN":
                query = f"ALTER TABLE {name} DROP {type} IF EXISTS {name}"
            elif type is None:
                query = f"DELETE FROM {From} {body} LIMIT {limit}"
            print(query)
            self.threadSafeAccess(query, dbname=True)
            self.executeQuery("SET SQL_SAFE_UPDATES = 1")
            print("Data deleted successfully")
        except Error as err:
            print(f"[Delete] Error: '{err}'")




# ########### Tests performing CRUD operations on the database ###########
# # Example usage
# host_name = "localhost"
# user_name = "root"
# user_password = "admin"
# # Establish a database connection
# db = Database(host_name, user_name, user_password)

# create_table_body = """(
#     id INT NOT NULL AUTO_INCREMENT, 
#     done VARCHAR(1) NOT NULL DEFAULT '', 
#     delivered INT NOT NULL DEFAULT 0, 
#     start TIME, 
#     end TIME, 
#     client VARCHAR(30) NOT NULL, 
#     number INT NOT NULL, 
#     workpiece VARCHAR(2) NOT NULL, 
#     quantity INT NOT NULL, 
#     due_date INT NOT NULL, 
#     late_pen INT NOT NULL,
#     early_pen INT NOT NULL, 
#     PRIMARY KEY (id))
#     """

# db.create("DATABASE", "mes")
# db.create("TABLE", "orders", create_table_body)

# insert_table_body = "(start, client, number, workpiece, quantity, due_date, late_pen, early_pen) VALUES ({}, {}, {}, {}, {}, {}, {}, {})".format("CURRENT_TIME()", "'Client AA'", 15, "'P2'", 6, 3, 5, 10)
# db.insertData("orders", insert_table_body)

# customers_data = db.readData("*", "orders", "WHERE number = 16")

# db.updateData("orders", "number = 18", "WHERE number = 17")

# db.delete(From="orders", body= "WHERE number = 16")