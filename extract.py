from dotenv import load_dotenv
import os
import psycopg2
import csv
from psycopg2 import OperationalError


load_dotenv()


tables = os.getenv("TABLES").split(",")


# Access variables for postgres
db_name = os.getenv("POSTGRES_DB")
db_user = os.getenv("POSTGRES_USER")
db_password = os.getenv("POSTGRES_PASSWORD")
host = os.getenv("HOST")
port = int(os.getenv("PORT"))


try:
    # Connect to PostgreSQL
    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_password,
        host=host,
        port=port
    )
    cursor = conn.cursor()
    for table in tables:
        query = f"SELECT * FROM {table};"
        local_filename = f"extracted_tables/{table}_extract.csv"
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        with open(local_filename, 'w', newline='') as fp:
            csv_w = csv.writer(fp, delimiter='|')
            
            
            column_names = [desc[0] for desc in cursor.description]
            csv_w.writerow(column_names)
            
            
            csv_w.writerows(results)
        
        print(f"Table {table} exported to {local_filename}")


except Exception as e:
    print("Error:", e)

finally:
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals():
        conn.close()