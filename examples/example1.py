import pggo

DSN = "postgres://postgres:password@localhost:5432/postgres?sslmode=disable"

create_sql = """
    create table if not exists cliente (
        id serial primary key,
        nome text
    );
"""

conn = pggo.connect(DSN)
cur = conn.cursor()

cur.execute(create_sql)
cur.close()
conn.close()

# funciona aninhado só o cursor:
conn = pggo.connect(DSN)
try:
    with conn.cursor() as cur:
        cur.execute("insert into cliente (nome) values ($1)", ["Lucas"])
        print("rows:", cur.rowcount)
finally:
    conn.close()

# with na conexão + with no cursor
with pggo.connect(DSN) as conn:
    with conn.cursor() as cur:
        cur.execute("select * from cliente where id = $1", [1])
        print(cur.fetchall())

with pggo.connect(DSN) as conn:
    with conn.cursor() as cur:
        cur.execute("select * from cliente where id = $1", [1])
        print(cur.fetchone())

# sem parametros
with pggo.connect(DSN) as conn:
    with conn.cursor() as cur:
        cur.execute("select * from cliente")
        print(cur.fetchall())
