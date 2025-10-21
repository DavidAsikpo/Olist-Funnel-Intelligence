"""
Funnel Analysis Data Loader
===========================

This script connects to a MySQL database (`funnel_analysis`) and loads multiple CSV datasets 
into their corresponding tables. It uses the `LOAD DATA LOCAL INFILE` method for efficient 
bulk import operations. The script is optimized for reusability, logs progress for each 
table, and measures the loading duration.

Author: David Asikpo
Date: October 2025
"""

import mysql.connector as my_sql
import time

# ----------------------------------------------------
# DATABASE CONNECTION SETUP
# ----------------------------------------------------
# Establish connection to the MySQL database
conn = my_sql.connect(
    host="localhost",              
    user="root",                   
    password="Davidasikpo2002!",   
    database="funnel_analysis",   
    allow_local_infile=True        
)

cursor = conn.cursor()

# ----------------------------------------------------
# TABLE AND FILE PATH MAPPINGS
# ----------------------------------------------------
# Map table names in MySQL to their corresponding local CSV file paths
table_and_file_paths = {
    'marketing_qualified_leads': '/Users/mac/PycharmProjects/Funnel_analysis /Data_sets/archive_3/olist_marketing_qualified_leads_dataset.csv',
    'closed_deals_dataset': '/Users/mac/PycharmProjects/Funnel_analysis /Data_sets/archive_3/olist_closed_deals_dataset.csv',
    'sellers_dataset': '/Users/mac/PycharmProjects/Funnel_analysis /Data_sets/archive_2/olist_sellers_dataset.csv',
    'order_items': '/Users/mac/PycharmProjects/Funnel_analysis /Data_sets/archive_2/olist_order_items_dataset.csv',
    'orders': '/Users/mac/PycharmProjects/Funnel_analysis /Data_sets/archive_2/olist_orders_dataset.csv'
}

# List of all table names to be loaded
table_names = [
    'marketing_qualified_leads',
    'closed_deals_dataset',
    'sellers_dataset',
    'order_items',
    'orders'
]

# ----------------------------------------------------
# LOG HEADER
# ----------------------------------------------------
print("\n" + "-" * 50)
print("LOADING funnel_analysis TABLES")
print("-" * 50 + "\n")

# ----------------------------------------------------
# FUNCTION: LOAD TABLE DATA
# ----------------------------------------------------
def load_table(table, file_path):
    """
    Loads data from a CSV file into the specified MySQL table.

    Steps:
    1. Truncates the table to remove old data.
    2. Loads data from the local CSV file using LOAD DATA LOCAL INFILE.
    3. Commits the transaction.
    4. Prints duration of loading operation.
    """

    try:
        print(f"\n>> TRUNCATING TABLE: funnel_analysis.{table}")
        cursor.execute(f"TRUNCATE TABLE {table};")

        
        start_time = time.time()
      
        query = f"""
        LOAD DATA LOCAL INFILE '{file_path}'
        INTO TABLE {table}
        FIELDS TERMINATED BY ','
        LINES TERMINATED BY '\\n'
        IGNORE 1 ROWS;
        """
        cursor.execute(query)

        conn.commit()

        end_time = time.time()
        duration = round(end_time - start_time, 2)
        print(f">> Duration to load {table}: {duration} seconds")

    except my_sql.Error as err:
        print(f"ERROR OCCURRED WHILE LOADING TABLE: {table}")
        print(f"MySQL Error: {err}")
        print(">> ----------")

# ----------------------------------------------------
# MAIN EXECUTION LOOP
# ----------------------------------------------------
# Record total start time
start_time_all = time.time()

    load_table(table, table_and_file_paths[table])


end_time_all = time.time()
duration_all = round(end_time_all - start_time_all, 2)

# ----------------------------------------------------
# SUMMARY LOGS AND CLEANUP
# ----------------------------------------------------
# Print total time taken for all table loads
print(f"\n>> Total load process duration: {duration_all} seconds")

cursor.close()
conn.close()

print("\nAll tables loaded successfully!")
