
#!/usr/bin/env python3
"""
bench.py — Benchmark simples comparando psycopg2 x pggo.

- Cria tabelas separadas para cada driver para isolar efeitos.
- Mede (para cada driver):
    - insert_single: INSERT 1-a-1 dentro de transação (com commits periódicos).
    - select_point: N selects por id aleatório.
    - select_count: COUNT(*).
- Reporta métricas: avg_ms, median_ms, p90_ms, std_ms, total_s, throughput_rows_per_s.
- Saída: JSON no stdout.

Requisitos:
    pip install psycopg2-binary pggo
    sudo docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=password postgres:14-alpine

Exemplo de uso:
    python3 bench.py > results.json
"""

import time
import random
import statistics
import json

import psycopg2
import pggo

CFG = {
    "dsn_pg": "dbname=postgres user=postgres password=password host=127.0.0.1 port=5432",
    "dsn_go": "postgres://postgres:password@127.0.0.1:5432/postgres?sslmode=disable",
    "rows": 100,          # total de linhas a inserir por driver
}


def metrics(samples, rows=None):
    if not samples:
        return {}
    total = sum(samples)
    out = {
        "count": len(samples),
        "total_s": total,
        "avg_ms": (total/len(samples))*1000,
        "median_ms": statistics.median(samples)*1000,
        "p90_ms": (statistics.quantiles(samples, n=10)[8]*1000) if len(samples) >= 10 else (max(samples)*1000),
        "std_ms": (statistics.pstdev(samples)*1000) if len(samples) > 1 else 0.0,
    }
    if rows and total > 0:
        out["throughput_rows_per_s"] = rows/total
    return out

# -------------------- psycopg2 --------------------
def psycopg2_setup(conn):
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS bench_kv_py")
        cur.execute("CREATE TABLE bench_kv_py (id SERIAL PRIMARY KEY, k INT NOT NULL, v TEXT NOT NULL)")
    conn.commit()

def psycopg2_insert_single(conn, rows):
    times = []
    with conn.cursor() as cur:
        for k in range(rows):
            t0 = time.perf_counter()
            v = "TEXT-BENCHMARK"
            cur.execute("INSERT INTO bench_kv_py(k, v) VALUES (%s, %s)", (k, v))
            times.append(time.perf_counter() - t0)

    return metrics(times, rows)


def psycopg2_select_count(conn):
    with conn.cursor() as cur:
        t0 = time.perf_counter()
        cur.execute("SELECT COUNT(*) FROM bench_kv_py")
        _ = cur.fetchone()
        elapsed = time.perf_counter() - t0
    return metrics([elapsed], 1)

def run_psycopg2():

    with psycopg2.connect(CFG["dsn_pg"]) as conn:
        psycopg2_setup(conn)
        res = {
            "insert_single": psycopg2_insert_single(conn, CFG["rows"]),
            "select_count": psycopg2_select_count(conn),
        }
        return res

# -------------------- pggo --------------------
def pggo_setup(conn):
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS bench_kv_go")
        cur.execute("CREATE TABLE bench_kv_go (id SERIAL PRIMARY KEY, k INT NOT NULL, v TEXT NOT NULL)")

def pggo_insert_single(conn, rows):
    times = []
    with conn.cursor() as cur:
        for k in range(rows):
            t0 = time.perf_counter()
            v = "TEXT-BENCHMARK"
            cur.execute("INSERT INTO bench_kv_go(k, v) VALUES ($1, $2)", [k, v])
            times.append(time.perf_counter() - t0)

    return metrics(times, rows)

def pggo_select_count(conn):
    with conn.cursor() as cur:
        t0 = time.perf_counter()
        cur.execute("SELECT COUNT(*) AS c FROM bench_kv_go")
        _ = cur.fetchone()              # ex.: {'c': 20000}
        elapsed = time.perf_counter() - t0
    return metrics([elapsed], 1)

def run_pggo():
    dsn = CFG["dsn_go"]
    with pggo.connect(dsn) as conn:
        pggo_setup(conn)
        res = {
            "insert_single": pggo_insert_single(conn, CFG["rows"]),
            "select_count": pggo_select_count(conn),
        }
        return res


# -------------------- main --------------------
def main():
    out = {"cfg": CFG, "psycopg2": {}, "pggo": {}}
    out["psycopg2"] = run_psycopg2()
    out["pggo"] = run_pggo()
    print(json.dumps(out, indent=2))

if __name__ == "__main__":
    main()