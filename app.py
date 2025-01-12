import psycopg2
import time
import streamlit as st
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

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
        master_conn.commit()

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

        return f"Test table '{table_name}' created on master and slave."
        
    except Exception as e:
        return f"Error creating test table: {e}"

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
            CONNECTION 'host=localhost port=5432 dbname=testdb user=postgres password=masterpass' 
            PUBLICATION master_pub;
        """)

        return "Replication setup completed successfully"
    except Exception as e:
        return f"Error setting up replication: {e}"

def get_tables():
    """Get table names from both master and slave"""
    tables = {'master': [], 'slave': []}

    try:
        # Connect to master
        master_conn = connect_to_db("localhost", 5432, "testdb", "postgres", "masterpass")
        master_cur = master_conn.cursor()
        master_cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
        tables['master'] = [table[0] for table in master_cur.fetchall()]

        # Connect to slave
        slave_conn = connect_to_db("localhost", 5433, "testdb", "postgres", "slavepass")
        slave_cur = slave_conn.cursor()
        slave_cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
        tables['slave'] = [table[0] for table in slave_cur.fetchall()]

        master_conn.close()
        slave_conn.close()
        return tables
    except Exception as e:
        return f"Error fetching tables: {e}"

def delete_row_from_table(table_name, row_id):
    """Delete a row from the specified table on both master and slave"""
    try:
        # Connect to master
        master_conn = connect_to_db("localhost", 5432, "testdb", "postgres", "masterpass")
        master_cur = master_conn.cursor()

        # Delete row from master
        master_cur.execute(f"DELETE FROM {table_name} WHERE id = %s;", (row_id,))
        master_conn.commit()

        # Connect to slave
        slave_conn = connect_to_db("localhost", 5433, "testdb", "postgres", "slavepass")
        slave_cur = slave_conn.cursor()

        # Delete row from slave
        slave_cur.execute(f"DELETE FROM {table_name} WHERE id = %s;", (row_id,))
        slave_conn.commit()

        master_conn.close()
        slave_conn.close()
        return f"Row with ID {row_id} deleted from {table_name} on both master and slave."
    except Exception as e:
        return f"Error deleting row: {e}"

def show_table_rows(table_name):
    """Fetch rows from the specified table"""
    rows = {'master': [], 'slave': []}
    
    try:
        # Connect to master
        master_conn = connect_to_db("localhost", 5432, "testdb", "postgres", "masterpass")
        master_cur = master_conn.cursor()

        # Fetch data from the specified table in master
        master_cur.execute(f"SELECT * FROM {table_name};")
        rows['master'] = master_cur.fetchall()

        # Connect to slave
        slave_conn = connect_to_db("localhost", 5433, "testdb", "postgres", "slavepass")
        slave_cur = slave_conn.cursor()

        # Fetch data from the specified table in slave
        slave_cur.execute(f"SELECT * FROM {table_name};")
        rows['slave'] = slave_cur.fetchall()

        master_conn.close()
        slave_conn.close()

        return rows
    except Exception as e:
        return f"Error showing rows: {e}"

def app():
    """Streamlit app"""
    st.title("PostgreSQL Replication Management")
    st.sidebar.title("Options")
    
    tables = get_tables()
    
    # Add Table Section
    st.subheader("Add a new table")
    table_name = st.text_input("Enter the table name")
    if st.button("Create Table"):
        result = create_test_table(table_name)
        st.success(result)
        setup_result = setup_replication()
        st.success(setup_result)
    
    # Add Row Section
    st.subheader("Add a row to a table")
    table_choice = st.selectbox("Select Table", tables['master'])
    row_data = st.text_input("Enter data for row")
    if st.button("Add Row"):
        result = add_row_to_table(table_choice, row_data)
        st.success(result)
    
    # Delete Row Section
    st.subheader("Delete a row from a table")
    table_choice_for_deletion = st.selectbox("Select Table for Deletion", tables['master'])
    rows_to_delete = show_table_rows(table_choice_for_deletion)
    row_id = st.selectbox(f"Select Row ID to delete from {table_choice_for_deletion}", [row[0] for row in rows_to_delete['master']])
    if st.button("Delete Row"):
        result = delete_row_from_table(table_choice_for_deletion, row_id)
        st.success(result)
    
    # Drop Table Section
    st.subheader("Drop a table")
    drop_table_choice = st.selectbox("Select Table to Drop", tables['master'])
    if st.button("Drop Table"):
        result = drop_table(drop_table_choice)
        st.success(result)

if __name__ == "__main__":
    app()
