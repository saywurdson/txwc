import duckdb
import glob
import os
import dlt

def load_vocab_to_omop():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    try:
        db = dlt.config["destination.duckdb.credentials"]
    except KeyError:
        db = "tx_workers_comp.db"
    db_path = db if os.path.isabs(db) else os.path.join(base_dir, db)
    conn = duckdb.connect(db_path)
    conn.execute("CREATE SCHEMA IF NOT EXISTS omop;")

    vocab_dir = os.path.join(base_dir, 'vocab')
    csv_pattern = os.path.join(vocab_dir, '*.csv')

    # Process each CSV file
    for csv_path in glob.glob(csv_pattern):
        try:
            table_name = os.path.splitext(os.path.basename(csv_path))[0].lower()

            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS omop.{table_name} AS
                SELECT * FROM read_csv_auto('{csv_path}', ALL_VARCHAR=TRUE, quote='')
                WHERE 1=0;
            """)

            conn.execute(f"""
                INSERT INTO omop.{table_name}
                SELECT * FROM read_csv_auto('{csv_path}', ALL_VARCHAR=TRUE, quote='');
            """)

            print(f"Loaded {os.path.basename(csv_path)} -> omop.{table_name}")

        except Exception as e:
            print(f"Error loading {csv_path}: {str(e)}")

    conn.close()

if __name__ == "__main__":
    load_vocab_to_omop()
    print("Vocab loading completed.")
