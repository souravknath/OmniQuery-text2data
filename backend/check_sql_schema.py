import pyodbc
conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=(localdb)\\MSSQLLocalDB;DATABASE=Users;Trusted_Connection=yes;')
cursor = conn.cursor()
cursor.execute("SELECT COLUMN_NAME, COLUMNPROPERTY(OBJECT_ID('User_Orders'), COLUMN_NAME, 'IsIdentity') AS IsIdentity FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'User_Orders'")
for row in cursor.fetchall():
    print(row)
