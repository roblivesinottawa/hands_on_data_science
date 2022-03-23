import psycopg2
from config import config


class SalesData:
    def __init__(self):
        self.conn = None
        self.cursor = None
        self.params = config()
        
# this method creates the connection to postgresql database
    def connection(self):
        '''connects to the database'''
        try:
            self.conn = psycopg2.connect(**self.params)
            self.cursor = self.conn.cursor()
            print("Connected to the database")
            print('PostGres Database version:', self.conn.server_version)
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if self.conn is not None:
                self.conn.close()
                print("Database connection closed")

# this method creates the database
    def create_database(self, database_name):
        '''creates a database'''
        try:
            self.cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
            self.conn.commit()
            print(f"Database {database_name} created")
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

# this method creates the tables
    def create_table(self, database_name):
        '''creates tables in the database'''
        try:
            self.conn = psycopg2.connect(**self.params)
            self.cursor = self.conn.cursor()
            self.cursor.execute('''CREATE TABLE IF NOT EXISTS sales_data (
                                    RowID SERIAL PRIMARY KEY,
                                    OrderID INTEGER,
                                    OrderDate DATE,
                                    OrderMonthYear TEXT,
                                    Quantity INTEGER,
                                    Quote INTEGER,
                                    DiscountPct REAL,
                                    Rate REAL,
                                    SaleAmount REAL,
                                    CustomerName TEXT,
                                    CompanyName TEXT,
                                    Sector TEXT,
                                    Industry TEXT,
                                    City TEXT,
                                    ZipCode TEXT,
                                    State TEXT,
                                    Region TEXT,
                                    ProjectCompleteDate DATE,
                                    DaystoComplete INTEGER,
                                    ProductKey TEXT,
                                    ProductCategory TEXT,
                                    ProductSubCategory TEXT,
                                    Consultant TEXT,
                                    Manager TEXT,
                                    HourlyWage REAL,
                                    RowCount INTEGER,
                                    WageMargin REAL
                            );''')
            self.conn.commit()
            print("Table created")
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if self.conn is not None:
                self.conn.close()
                print("Database connection closed")

# this method will insert data into the table from a csv file
    def insert_data(self, database_name, file_name):
        '''inserts data into the table'''
        try:
            self.conn = psycopg2.connect(**self.params)
            self.cursor = self.conn.cursor()
            self.cursor.execute(f"COPY sales_data FROM '{file_name}' DELIMITER ',' CSV HEADER;")
            self.conn.commit()
            print("Data inserted")
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if self.conn is not None:
                self.conn.close()
                print("Database connection closed")




if __name__=='__main__':
    sales_data = SalesData()
    sales_data.connection()
    # sales_data.create_database('salesdata')
    # sales_data.create_table('salesdata')
    # sales_data.insert_data('salesdata', './files/CogsleyServices-SalesData-US.csv')



