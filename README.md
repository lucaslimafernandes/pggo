# pggo

Author: Lucas Lima Fernandes

Driver PostgreSQL implementado em Go (pgx) exposto para Python via lib C compartilhada, empacotado como wheel.


## Instalação 

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