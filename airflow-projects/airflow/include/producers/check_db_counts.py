import duckdb

DB_PATH = '/usr/local/airflow/include/STG/chocolate_warehouse.duckdb'

def check_all_counts():
    print('\n DuckDB Table Row Counts')
    print('=' * 50)
    print(f'{"TABLE NAME":<35} | {"ROW COUNT"}')
    print('-' * 50)
    
    try:
        conn = duckdb.connect(DB_PATH)
        # Get all table names in the main schema
        tables = conn.execute("SELECT table_name FROM duckdb_tables() WHERE schema_name='main'").fetchall()
        
        if not tables:
            print("No tables found in the database.")
            return
            
        for t in tables:
            table_name = t[0]
            # Query the exact count for each table
            count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            print(f'{table_name:<35} | {count:,}')
            
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        print('=' * 50 + '\n')
        conn.close()

if __name__ == '__main__':
    check_all_counts()
