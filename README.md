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