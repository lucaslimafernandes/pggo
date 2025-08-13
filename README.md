# pggo

Author: Lucas Lima Fernandes

PostgreSQL driver implemented in Go (pgx) exposed to Python via shared C lib, packaged as wheel.


## Instalation

```bash
pip install pggo
```

## Teste

```python
from pggo import connect
c = connect("postgres://user:pass@host:5432/db?sslmode=disable")
cur = c.cursor()
cur.execute("select 1 as x")
print(cur.fetchall())  # [{'x': 1}]
c.close()
```

You can execute tests in `examples/`

## Passing Parameters to Queries

pggo supports PostgreSQL positional parameters using the $1, $2, $n syntax.
You must pass a list or tuple as the second argument to execute.

```python
from pggo import connect

DSN = "postgres://postgres:password@localhost:5432/postgres?sslmode=disable"

with connect(DSN) as conn:
    with conn.cursor() as cur:
        cur.execute("INSERT INTO cliente (nome) VALUES ($1)", ["Lucas"])
        print("Rows affected:", cur.rowcount)

with connect(DSN) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM cliente WHERE nome = $1", ["Lucas"])
        print(cur.fetchall())  # [{'id': 2, 'nome': 'Lucas'}]
```

## Contributing

Your contributions are welcome! If you encounter any bugs or have feature requests, please open an issue. To contribute code, follow these steps:

Fork the repository.

Clone your forked repository to your local machine.

Create a new branch for your feature or bugfix (git checkout -b feature-name).

Make your changes and commit them (git commit -m "Description of changes").

Push your branch (git push origin feature-name).

Open a pull request with a clear description of your changes.

For more details, check the Contributing Guide.

License This project is licensed under the MIT License. See the LICENSE file for more information.