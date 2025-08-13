
#!/usr/bin/env python3
"""
pg_benchmark.py — Benchmark simples comparando psycopg2 x pggo.

- Cria tabelas separadas para cada driver (bench_kv_py e bench_kv_go) para isolar efeitos.
- Mede (para cada driver):
    - insert_single: INSERT 1-a-1 dentro de transação (com commits periódicos).
    - select_point: N selects por id aleatório.
    - select_count: COUNT(*).
- Reporta métricas: avg_ms, median_ms, p90_ms, std_ms, total_s, throughput_rows_per_s.
- Saída: JSON no stdout.

Requisitos:
    pip install psycopg2-binary pggo==0.0.1
    sudo docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=password postgres:14-alpine

Exemplo de uso:
    export PG_DSN="postgres://postgres:password@127.0.0.1:5432/postgres?sslmode=disable"
    python pg_benchmark.py > results.json
"""
import os
import time
import random
import string
import statistics
import json
import contextlib

import psycopg2
# from psycopg2.extras import execute_values  # opcional, não usado no teste simples
import pggo

CFG = {
    "dsn_pg": "dbname=postgres user=postgres password=password host=127.0.0.1 port=5432",
    "dsn_go": "postgres://postgres:password@127.0.0.1:5432/postgres?sslmode=disable",
    "rows": 20000,          # total de linhas a inserir por driver
    "commit_every": 5000,
    "point_queries": 1000,
    "text_len": 32,
    "seed": 1337,
}

def rnd_text(n):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=n))

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

def psycopg2_insert_single(conn, rows, commit_every, txtlen):
    times = []
    with conn.cursor() as cur:
        for k in range(rows):
            v = rnd_text(txtlen)
            t0 = time.perf_counter()
            cur.execute("INSERT INTO bench_kv_py(k, v) VALUES (%s, %s)", (k, v))
            times.append(time.perf_counter() - t0)
            if (k+1) % commit_every == 0:
                conn.commit()
    conn.commit()
    return metrics(times, rows)

def psycopg2_select_point(conn, nqueries):
    # ids válidos: 1..N (assume sequência padrão SERIAL)
    with conn.cursor() as cur:
        cur.execute("SELECT COALESCE(MAX(id),0) FROM bench_kv_py")
        max_id = cur.fetchone()[0]
    if max_id == 0:
        return {}
    ids = [random.randint(1, max_id) for _ in range(nqueries)]
    times = []
    with conn.cursor() as cur:
        for i in ids:
            t0 = time.perf_counter()
            cur.execute("SELECT v FROM bench_kv_py WHERE id=%s", (i,))
            _ = cur.fetchone()
            times.append(time.perf_counter() - t0)
    return metrics(times, nqueries)

def psycopg2_select_count(conn):
    with conn.cursor() as cur:
        t0 = time.perf_counter()
        cur.execute("SELECT COUNT(*) FROM bench_kv_py")
        _ = cur.fetchone()
        elapsed = time.perf_counter() - t0
    return metrics([elapsed], 1)

def run_psycopg2():
    conn = psycopg2.connect(CFG["dsn_pg"])
    conn.autocommit = False
    try:
        psycopg2_setup(conn)
        res = {
            "insert_single": psycopg2_insert_single(conn, CFG["rows"], CFG["commit_every"], CFG["text_len"]),
            "select_point": psycopg2_select_point(conn, CFG["point_queries"]),
            "select_count": psycopg2_select_count(conn),
        }
        return res
    finally:
        with contextlib.suppress(Exception):
            conn.close()

# -------------------- pggo --------------------
def pggo_setup(conn):
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS bench_kv_go")
        cur.execute("CREATE TABLE bench_kv_go (id SERIAL PRIMARY KEY, k INT NOT NULL, v TEXT NOT NULL)")

def pggo_insert_single(conn, rows, commit_every, txtlen):
    times = []
    # pggo commit: usa "BEGIN"/"COMMIT" implícitos via conexão; não há .commit(), então faz blocos manuais
    inserted = 0
    while inserted < rows:
        block = min(commit_every, rows - inserted)
        with conn.cursor() as cur:
            # inicia bloco (pggo gerencia transação internamente por cursor)
            for off in range(block):
                k = inserted + off
                v = rnd_text(txtlen)
                t0 = time.perf_counter()
                cur.execute("INSERT INTO bench_kv_go(k, v) VALUES ($1, $2)", [k, v])
                times.append(time.perf_counter() - t0)
            # ao sair do with, o cursor é fechado; transação é finalizada pelo driver
        inserted += block
    return metrics(times, rows)

def pggo_select_point(conn, nqueries):
    # pega max_id como ESCALAR via dict
    with conn.cursor() as cur:
        cur.execute("SELECT COALESCE(MAX(id), 0) AS max_id FROM bench_kv_go")
        row = cur.fetchone()              # ex.: {'max_id': 12345}
        max_id = int(row.get('max_id', 0)) if row else 0

    if max_id == 0:
        return {}

    ids = [random.randint(1, max_id) for _ in range(nqueries)]
    times = []
    with conn.cursor() as cur:
        for i in ids:
            t0 = time.perf_counter()
            cur.execute("SELECT v FROM bench_kv_go WHERE id = $1", [i])
            _ = cur.fetchone()            # dict, ex.: {'v': '...'} (ou {'id':..., 'k':..., 'v':...} dependendo do SELECT)
            times.append(time.perf_counter() - t0)
    return metrics(times, nqueries)

def pggo_select_count(conn):
    with conn.cursor() as cur:
        t0 = time.perf_counter()
        cur.execute("SELECT COUNT(*) AS c FROM bench_kv_go")
        row = cur.fetchone()              # ex.: {'c': 20000}
        _ = row.get('c', 0) if row else 0
        elapsed = time.perf_counter() - t0
    return metrics([elapsed], 1)

def run_pggo():
    dsn = CFG["dsn_go"]
    conn = pggo.connect(dsn)
    try:
        pggo_setup(conn)
        res = {
            "insert_single": pggo_insert_single(conn, CFG["rows"], CFG["commit_every"], CFG["text_len"]),
            "select_point": pggo_select_point(conn, CFG["point_queries"]),
            "select_count": pggo_select_count(conn),
        }
        return res
    finally:
        # pggo conexão possui close()
        conn.close()

# -------------------- main --------------------
def main():
    random.seed(CFG["seed"])
    out = {"cfg": CFG, "psycopg2": {}, "pggo": {}}
    out["psycopg2"] = run_psycopg2()
    out["pggo"] = run_pggo()
    print(json.dumps(out, indent=2))

if __name__ == "__main__":
    main()