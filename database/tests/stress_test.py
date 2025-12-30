import time
import statistics
import asyncio
import argparse
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import psycopg
from psycopg_pool import ConnectionPool
from dotenv import load_dotenv

load_dotenv()

class PerformanceTester:
    def __init__(self, dsn: str, max_connections: int = 10):
        self.dsn = dsn
        self.pool = ConnectionPool(conninfo=dsn, min_size=1, max_size=max_connections)
        self.latencies = []

    def run_query(self, query: str, params: tuple = ()):
        start = time.perf_counter()
        try:
            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, params)
                    cur.fetchone()
        except Exception as e:
            print(f"Query error: {e}")
        end = time.perf_counter()
        latency_ms = (end - start) * 1000
        self.latencies.append(latency_ms)
        return latency_ms

    def stress_test_search(self, num_queries: int = 100, workers: int = 5):
        print(f"Starting Search stress test: {num_queries} queries with {workers} parallel workers...")
        self.latencies = []
        test_query = "SELECT id, nombre FROM app.articulo WHERE lower(nombre) LIKE %s LIMIT 1"
        test_params = ("%a%",)
        self._run_concurrent(test_query, test_params, num_queries, workers)
        self._print_results("Search Article")

    def stress_test_dashboard(self, num_queries: int = 20, workers: int = 5):
        print(f"Starting Dashboard Stats stress test: {num_queries} queries with {workers} parallel workers...")
        self.latencies = []
        # Complex query joining multiple tables/views
        test_query = "SELECT SUM(total) FROM app.v_documento_resumen WHERE clase = 'VENTA' AND fecha >= date_trunc('month', now())"
        self._run_concurrent(test_query, (), num_queries, workers)
        self._print_results("Dashboard Stats")

    def _run_concurrent(self, query, params, count, workers):
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(self.run_query, query, params) for _ in range(count)]
            for f in futures: f.result()

    def _print_results(self, title: str = "Test"):
        if not self.latencies:
            print(f"No data collected for {title}.")
            return

        print(f"\n--- {title} Results ---")
        print(f"Total Queries: {len(self.latencies)}")
        print(f"Min Latency: {min(self.latencies):.2f} ms")
        print(f"Max Latency: {max(self.latencies):.2f} ms")
        print(f"Mean Latency: {statistics.mean(self.latencies):.2f} ms")
        print(f"Median Latency: {statistics.median(self.latencies):.2f} ms")
        if len(self.latencies) > 1:
            print(f"95th Percentile: {statistics.quantiles(self.latencies, n=20)[18]:.2f} ms")
        print("---------------------------\n")

def main():
    parser = argparse.ArgumentParser(description="Nexoryn Stress Test Tool")
    parser.add_argument("--queries", type=int, default=500, help="Number of queries to run")
    parser.add_argument("--workers", type=int, default=10, help="Number of parallel workers")
    args = parser.parse_args()

    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        dsn = f"host={os.getenv('DB_HOST', 'localhost')} port={os.getenv('DB_PORT', '5432')} dbname={os.getenv('DB_NAME', 'nexoryn')} user={os.getenv('DB_USER', 'postgres')} password={os.getenv('DB_PASSWORD', '')}"

    tester = PerformanceTester(dsn, max_connections=args.workers + 5)
    tester.stress_test_search(num_queries=args.queries, workers=args.workers)
    tester.stress_test_dashboard(num_queries=min(50, args.queries // 5), workers=min(5, args.workers))

if __name__ == "__main__":
    main()
