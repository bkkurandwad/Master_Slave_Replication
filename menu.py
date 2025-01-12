import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import time

def connect_to_db(host, port, dbname, user, password):
    """Connect to PostgreSQL database"""
    return psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password
    )

def create_test_table(table_name):
    """Create user-defined table on both master and slave"""
    try:
        # Connect to master
        master_conn = connect_to_db("localhost", 5432, "testdb", "postgres", "masterpass")
        master_cur = master_conn.cursor()

        # Create test table if it doesn't exist (with one column)
        master_cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id SERIAL PRIMARY KEY,
                data TEXT
            );
        """)
        #master_cur.execute(f"INSERT INTO {table_name} (data) VALUES ('test data');")
        master_conn.commit()

        print(f"Test table '{table_name}' created on master.")
        print(f"Test table '{table_name}' replicated on slave.")
        
        # Connect to slave
        slave_conn = connect_to_db("localhost", 5433, "testdb", "postgres", "slavepass")
        slave_cur = slave_conn.cursor()

        # Create test table if it doesn't exist on slave
        slave_cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id SERIAL PRIMARY KEY,
                data TEXT
            );
        """)
        slave_conn.commit()

        slave_conn.close()
        master_conn.close()

    except Exception as e:
        print(f"Error creating test table: {e}")

def setup_replication():
    """Setup logical replication between master and slave"""
    try:
        # Connect to master
        master_conn = connect_to_db("localhost", 5432, "testdb", "postgres", "masterpass")
        master_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        master_cur = master_conn.cursor()

        # Drop publication if it exists on master
        master_cur.execute("""
            DO $$
            BEGIN
                IF EXISTS (SELECT 1 FROM pg_catalog.pg_publications WHERE pubname = 'master_pub') THEN
                    DROP PUBLICATION master_pub;
                END IF;
            END
            $$;
        """)

        # Create publication on master
        master_cur.execute("CREATE PUBLICATION master_pub FOR ALL TABLES;")
        
        # Connect to slave
        slave_conn = connect_to_db("localhost", 5433, "testdb", "postgres", "slavepass")
        slave_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        slave_cur = slave_conn.cursor()

        # Drop subscription if it exists on slave
        slave_cur.execute("""
            DO $$
            BEGIN
                IF EXISTS (SELECT 1 FROM pg_catalog.pg_subscription WHERE subname = 'slave_sub') THEN
                    DROP SUBSCRIPTION slave_sub;
                END IF;
            END
            $$;
        """)

        # Create subscription on slave
        slave_cur.execute("""
            CREATE SUBSCRIPTION slave_sub 
            CONNECTION 'host=postgres_master port=5432 dbname=testdb user=postgres password=masterpass' 
            PUBLICATION master_pub;
        """)

        print("Replication setup completed successfully")

    except Exception as e:
        print(f"Error setting up replication: {e}")
    finally:
        if 'master_conn' in locals():
            master_conn.close()
        if 'slave_conn' in locals():
            slave_conn.close()

def test_replication(table_name):
    """Test replication by inserting data in master and checking slave"""
    try:
        # Connect to master
        master_conn = connect_to_db("localhost", 5432, "testdb", "postgres", "masterpass")
        master_cur = master_conn.cursor()

        # Create test table and insert data
        master_cur.execute(f"INSERT INTO {table_name} (data) VALUES ('test data');")
        master_conn.commit()

        # Wait for replication
        time.sleep(2)

        # Check slave
        slave_conn = connect_to_db("localhost", 5433, "testdb", "postgres", "slavepass")
        slave_cur = slave_conn.cursor()
        
        slave_cur.execute(f"SELECT * FROM {table_name};")
        results = slave_cur.fetchall()
        
        print(f"Data in slave: {results}")

        # List tables on both master and slave
        master_cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
        master_tables = master_cur.fetchall()
        print(f"Tables on master: {master_tables}")

        slave_cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
        slave_tables = slave_cur.fetchall()
        print(f"Tables on slave: {slave_tables}")

    except Exception as e:
        print(f"Error testing replication: {e}")
    finally:
        if 'master_conn' in locals():
            master_conn.close()
        if 'slave_conn' in locals():
            slave_conn.close()

def drop_table(table_name):
    """Drop the user-defined table from both master and slave"""
    try:
        # Connect to master
        master_conn = connect_to_db("localhost", 5432, "testdb", "postgres", "masterpass")
        master_cur = master_conn.cursor()

        # Drop the table from master
        master_cur.execute(f"DROP TABLE IF EXISTS {table_name};")
        master_conn.commit()

        # Connect to slave
        slave_conn = connect_to_db("localhost", 5433, "testdb", "postgres", "slavepass")
        slave_cur = slave_conn.cursor()

        # Drop the table from slave
        slave_cur.execute(f"DROP TABLE IF EXISTS {table_name};")
        slave_conn.commit()

        print(f"Table '{table_name}' dropped from both master and slave.")

    except Exception as e:
        print(f"Error dropping table: {e}")
    finally:
        if 'master_conn' in locals():
            master_conn.close()
        if 'slave_conn' in locals():
            slave_conn.close()

def add_row_to_table(table_name, data):
    """Add a row to the specified table on both master and slave"""
    try:
        # Connect to master
        master_conn = connect_to_db("localhost", 5432, "testdb", "postgres", "masterpass")
        master_cur = master_conn.cursor()

        # Insert row into master
        master_cur.execute(f"INSERT INTO {table_name} (data) VALUES (%s);", (data,))
        master_conn.commit()

        # Connect to slave
        slave_conn = connect_to_db("localhost", 5433, "testdb", "postgres", "slavepass")
        slave_cur = slave_conn.cursor()

        # Insert row into slave
        slave_cur.execute(f"INSERT INTO {table_name} (data) VALUES (%s);", (data,))
        slave_conn.commit()

        print(f"Row with data '{data}' added to '{table_name}' on master. and slave.")
        print(f"Row with data '{data}' replicated to slave.")

    except Exception as e:
        print(f"Error adding row: {e}")
    finally:
        if 'master_conn' in locals():
            master_conn.close()
        if 'slave_conn' in locals():
            slave_conn.close()

def show_data_in_tables():
    """Show data in both master and slave"""
    try:
        # Connect to master
        master_conn = connect_to_db("localhost", 5432, "testdb", "postgres", "masterpass")
        master_cur = master_conn.cursor()

        # Fetch data from master
        master_cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
        master_tables = master_cur.fetchall()
        print(f"Tables on master: {master_tables}")

        # Connect to slave
        slave_conn = connect_to_db("localhost", 5433, "testdb", "postgres", "slavepass")
        slave_cur = slave_conn.cursor()

        # Fetch data from slave
        slave_cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
        slave_tables = slave_cur.fetchall()
        print(f"Tables on slave: {slave_tables}")

    except Exception as e:
        print(f"Error showing data: {e}")
    finally:
        if 'master_conn' in locals():
            master_conn.close()
        if 'slave_conn' in locals():
            slave_conn.close()

def show_table_rows(table_name):
    """Show rows from the specified table on both master and slave"""
    try:
        # Connect to master
        master_conn = connect_to_db("localhost", 5432, "testdb", "postgres", "masterpass")
        master_cur = master_conn.cursor()

        # Fetch data from the specified table in master
        master_cur.execute(f"SELECT * FROM {table_name};")
        master_rows = master_cur.fetchall()
        print(f"Data in '{table_name}' on master:")
        for row in master_rows:
            print(row)

        # Connect to slave
        slave_conn = connect_to_db("localhost", 5433, "testdb", "postgres", "slavepass")
        slave_cur = slave_conn.cursor()

        # Fetch data from the specified table in slave
        slave_cur.execute(f"SELECT * FROM {table_name};")
        slave_rows = slave_cur.fetchall()
        print(f"Data in '{table_name}' on slave:")
        for row in slave_rows:
            print(row)

    except Exception as e:
        print(f"Error showing rows: {e}")
    finally:
        if 'master_conn' in locals():
            master_conn.close()
        if 'slave_conn' in locals():
            slave_conn.close()

def menu():
    while True:
        print("\n1. Add a new table")
        print("2. Add a row to a table")
        print("3. Show data in both master and slave")
        print("4. Drop a table")
        print("5. View table content")
        print("6. Exit")
        choice = input("Enter your choice: ")

        if choice == '1':
            table_name = input("Enter the table name to create: ")
            create_test_table(table_name)
            setup_replication()
        elif choice == '2':
            table_name = input("Enter the table name to add a row to: ")
            data = input("Enter the data to insert: ")
            add_row_to_table(table_name, data)
        elif choice == '3':
            show_data_in_tables()
        elif choice == '4':
            table_name = input("Enter the table name to drop: ")
            drop_table(table_name)
        elif choice == '5':
            # Show tables on both master and slave before asking for a table name
            master_conn = connect_to_db("localhost", 5432, "testdb", "postgres", "masterpass")
            master_cur = master_conn.cursor()
            master_cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
            master_tables = master_cur.fetchall()
            print("Tables on master:")
            for table in master_tables:
                print(table[0])

            slave_conn = connect_to_db("localhost", 5433, "testdb", "postgres", "slavepass")
            slave_cur = slave_conn.cursor()
            slave_cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
            slave_tables = slave_cur.fetchall()
            print("Tables on slave:")
            for table in slave_tables:
                print(table[0])

            # Ask for the table name and show its rows
            table_name = input("Enter the table name to view its content: ")
            show_table_rows(table_name)
        elif choice == '6':
            print("Exiting...")
            break
        else:
            print("Invalid choice, please try again.")

if __name__ == "__main__":
    menu()
