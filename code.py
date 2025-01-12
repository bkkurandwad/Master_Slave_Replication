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
        master_cur.execute(f"INSERT INTO {table_name} (data) VALUES ('test data');")
        master_conn.commit()

        print(f"Test table '{table_name}' created on both master and slave.")
        
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
        master_cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id SERIAL PRIMARY KEY,
                data TEXT
            );
        """)
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

if __name__ == "__main__":
    table_name = input("Enter the table name to create and replicate: ")
    create_test_table(table_name)  # Ensure the test table is created on both master and slave
    setup_replication()  # Set up replication
    test_replication(table_name)   # Test the replication by inserting data on master and checking slave
