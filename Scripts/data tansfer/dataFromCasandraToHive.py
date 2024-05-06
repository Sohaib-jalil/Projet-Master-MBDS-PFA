from cassandra.cluster import Cluster
import pandas as pd
from pyhive import hive

def connect_to_cassandra(hosts, keyspace):
    try:
        cluster = Cluster(hosts)
        session = cluster.connect(keyspace)
        return session
    except Exception as e:
        print(f"Failed to connect to Cassandra: {e}")
        return None

def retrieve_data(session, table_name):
    try:
        query = f"SELECT * FROM {table_name}"
        rows = session.execute(query)
        return rows
    except Exception as e:
        print(f"Failed to retrieve data from Cassandra: {e}")
        return None

def save_to_hive(data, hive_table):
    try:
        conn = hive.Connection(host='localhost', port=10000, username='vagrant', database='mbds_g5')
        df = pd.DataFrame(list(data))
        df.to_sql(hive_table, conn, if_exists='replace', index=False)
        print("Data saved to Hive table successfully.")
    except Exception as e:
        print(f"Failed to save data to Hive: {e}")

def main():
    # Cassandra connection details
    hosts = ['127.0.0.1']  # Replace with your Cassandra host
    keyspace = 'ks'  # Replace with your keyspace name
    table_name = 'client'  # Replace with your table name

    # Hive table details
    hive_table = 'your_hive_table'  # Replace with your Hive table name

    # Connect to Cassandra
    session = connect_to_cassandra(hosts, keyspace)
    if not session:
        return

    # Retrieve data
    rows = retrieve_data(session, table_name)
    if rows:
        # Save data to Hive
        save_to_hive(rows, hive_table)

    # Close connection
    session.shutdown()
    print("Connection closed.")

if __name__ == "__main__":
    main()
