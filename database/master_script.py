import os
import sys
import json
import csv
import argparse
from datetime import datetime
from pathlib import Path
import psycopg
from psycopg import sql

# Add tests dir to path to import test_data_generator
sys.path.append(str(Path(__file__).parent / "tests"))
from test_data_generator import TestDataGenerator

class DatabaseMaster:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.conn = psycopg.connect(self.dsn)
        self.backup_root = Path(__file__).parent.parent / "backups"
        self.backup_root.mkdir(exist_ok=True)

    def _get_tables(self):
        """Returns all tables in app, ref, and seguridad schemas."""
        query = """
            SELECT table_schema, table_name 
            FROM information_schema.tables 
            WHERE table_schema IN ('app', 'ref', 'seguridad') 
            AND table_type = 'BASE TABLE'
        """
        with self.conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchall()

    def backup(self):
        print("Starting backup...")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_dir = self.backup_root / f"backup_{timestamp}"
        backup_dir.mkdir()
        
        tables = self._get_tables()
        for schema, table in tables:
            print(f"  Backing up {schema}.{table}...")
            with self.conn.cursor() as cur:
                with cur.copy(f"COPY {schema}.{table} TO STDOUT WITH (FORMAT CSV, HEADER)") as copy:
                    with open(backup_dir / f"{schema}.{table}.csv", "wb") as f:
                        for data in copy:
                            f.write(data)
        
        print(f"Backup completed at: {backup_dir}")
        return backup_dir

    def clear(self):
        print("Clearing database data (keeping schema)...")
        # Important: Order matters due to FKs. Cascade is simpler.
        schemas = ['app', 'ref', 'seguridad']
        with self.conn.cursor() as cur:
            # We don't want to drop schemas, just truncate tables
            tables = self._get_tables()
            # Disable triggers to avoid audit log noise during mass clear
            cur.execute("SET session_replication_role = 'replica';")
            
            for schema, table in tables:
                if table == 'usuario' and schema == 'seguridad': continue # Protect admin user?
                try:
                    cur.execute(sql.SQL("TRUNCATE TABLE {}.{} RESTART IDENTITY CASCADE").format(
                        sql.Identifier(schema), sql.Identifier(table)
                    ))
                except Exception as e:
                    print(f"Warning: could not truncate {schema}.{table}: {e}")
            
            cur.execute("SET session_replication_role = 'origin';")
        self.conn.commit()
        print("Database cleared.")

    def seed(self, counts: Dict[str, int]):
        print("Seeding test data...")
        with self.conn.cursor() as cur:
            cur.execute("SET session_replication_role = 'replica';")
        
        gen = TestDataGenerator(self.conn)
        
        if counts.get('entities', 0) > 0:
            gen.generate_entities(counts['entities'])
        if counts.get('articles', 0) > 0:
            gen.generate_articles(counts['articles'])
        if counts.get('documents', 0) > 0:
            gen.generate_documents(counts['documents'])
        if counts.get('logs', 0) > 0:
            gen.generate_logs(counts['logs'])
            
        with self.conn.cursor() as cur:
            cur.execute("SET session_replication_role = 'origin';")
        self.conn.commit()
        print("Seeding completed.")

    def restore(self, backup_path: str):
        print(f"Restoring from: {backup_path}")
        path = Path(backup_path)
        if not path.exists():
            print(f"Error: {backup_path} does not exist.")
            return

        self.clear()
        
        with self.conn.cursor() as cur:
            cur.execute("SET session_replication_role = 'replica';")
            
            # Order is tricky. Reference tables first.
            files = list(path.glob("*.csv"))
            print(f"  Found {len(files)} files to restore.")
            for f in files: print(f"    - {f}")
            
            # Sort files: ref first, then app/seguridad
            ref_files = [f for f in files if f.name.startswith("ref.")]
            other_files = [f for f in files if not f.name.startswith("ref.")]
            
            for csv_file in ref_files + other_files:
                table_id = csv_file.stem # schema.table
                print(f"  Restoring {table_id} from {csv_file.name}...")
                with open(csv_file, "rb") as f:
                    with cur.copy(f"COPY {table_id} FROM STDIN WITH (FORMAT CSV, HEADER)") as copy:
                        copy.write(f.read())
            
            cur.execute("SET session_replication_role = 'origin';")
        self.conn.commit()
        self._reset_sequences()
        print("Restore completed.")

    def _reset_sequences(self):
        print("Resetting sequences...")
        with self.conn.cursor() as cur:
            # Find all IDENTITY columns
            cur.execute("""
                SELECT table_schema, table_name, column_name
                FROM information_schema.columns 
                WHERE is_identity = 'YES' 
                AND table_schema IN ('app', 'ref', 'seguridad')
            """)
            id_cols = cur.fetchall()
            for schema, table, col in id_cols:
                full_table = f"{schema}.{table}"
                # Get the name of the sequence associated with the identity column
                cur.execute(sql.SQL("SELECT pg_get_serial_sequence(%s, %s)"), [full_table, col])
                seq_name = cur.fetchone()[0]
                
                if seq_name:
                    # Set the sequence value to the max(id)
                    cur.execute(sql.SQL("SELECT setval(%s, COALESCE(MAX({}), 0) + 1, false) FROM {}").format(
                        sql.Identifier(col), sql.Identifier(schema, table)
                    ), [seq_name])
                    print(f"  Reset {seq_name} for {full_table}.{col}")
        self.conn.commit()
        print("Sequences reset.")

def main():
    parser = argparse.ArgumentParser(description="Nexoryn Database Master Script")
    parser.add_argument("--backup", action="store_true", help="Backup current database")
    parser.add_argument("--clear", action="store_true", help="Clear all data")
    parser.add_argument("--seed", action="store_true", help="Seed test data")
    parser.add_argument("--restore", type=str, help="Path to backup directory to restore")
    parser.add_argument("--reset-sequences", action="store_true", help="Reset all sequences to max(id)")
    parser.add_argument("--force", action="store_true", help="Skip confirmation prompts")
    
    parser.add_argument("--entities", type=int, default=100, help="Number of entities to seed")
    parser.add_argument("--articles", type=int, default=500, help="Number of articles to seed")
    parser.add_argument("--documents", type=int, default=200, help="Number of documents to seed")
    parser.add_argument("--logs", type=int, default=1000, help="Number of logs to seed")
    
    args = parser.parse_args()
    
    # Get DSN from env
    from dotenv import load_dotenv
    load_dotenv()
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        # Fallback to defaults
        dsn = f"host={os.getenv('DB_HOST', 'localhost')} port={os.getenv('DB_PORT', '5432')} dbname={os.getenv('DB_NAME', 'nexoryn')} user={os.getenv('DB_USER', 'postgres')} password={os.getenv('DB_PASSWORD', '')}"

    master = DatabaseMaster(dsn)
    
    if args.backup:
        master.backup()
    
    if args.clear:
        if args.force:
            master.clear()
        else:
            confirm = input("This will DELETE all data. Are you sure? (y/N): ")
            if confirm.lower() == 'y':
                master.clear()
            else:
                print("Aborted.")
                return

    if args.restore:
        master.restore(args.restore)

    if args.reset_sequences:
        master._reset_sequences()

    if args.seed:
        master.seed({
            'entities': args.entities,
            'articles': args.articles,
            'documents': args.documents,
            'logs': args.logs
        })

if __name__ == "__main__":
    main()
