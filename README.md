## Consulta de Filmes Bem Avaliados

**Contexto:**
Usando o banco de dados de filmes do [The Movies Dataset (Kaggle)](https://www.kaggle.com/datasets/rounakbanik/the-movies-dataset/data), foi criada uma estrutura de consultas (CTEs) para identificar filmes lançados nos últimos 10 anos e que tiveram uma média de avaliações igual ou superior a 4.

Observações importantes:

- A base vai apenas até 2020 (não contém filmes mais recentes).

- As avaliações possuem notas de 0 até 5 — e não de 0 a 10, como costuma ser mais comum.

- Os arquivos .csv utilizados devem ser colocados dentro da pasta movies_database/ na raiz do projeto.

Query usada:
```
WITH filmes_ultimos_10_anos AS (
  SELECT * 
  FROM filmes 
  WHERE ano >= EXTRACT(YEAR FROM CURRENT_DATE) - 10
),
avaliacoes_validas AS (
  SELECT *
  FROM avaliacoes
  WHERE nota IS NOT NULL
    AND nota BETWEEN 1 AND 10
),
media_avaliacoes_filmes AS (
  SELECT 
    f.id_filme,
    f.titulo,
    ROUND(AVG(a.nota),2) AS media_nota
  FROM filmes_ultimos_10_anos f
  JOIN avaliacoes_validas a ON f.id_filme = a.id_filme
  GROUP BY f.id_filme, f.titulo
),
filmes_bem_avaliados AS (
  SELECT *
  FROM media_avaliacoes_filmes
  WHERE media_nota >= 4
)
SELECT *
FROM filmes_bem_avaliados
ORDER BY media_nota DESC;
```
Resultado:
O resultado completo da query foi exportado e está disponível no arquivo ```cte_query-result.csv``` neste repositório.

## Rode localmente
Para rodar esse projeto localmente, você vai precisar de um ambiente com Python 3+ e PostgreSQL instalado e funcionando.

### ✅ Pré-requisitos
PostgreSQL instalado e rodando localmente (ou em outro host acessível)

Um banco de dados chamado filmes (ou outro nome, mas ajuste no código)

Python 3.x

Instale as bibliotecas necessárias com:

```pip install pandas psycopg2```

### 📂 Estrutura de pastas

O projeto deve conter:
```
.
├── movies_database/
│   ├── movies_metadata.csv
│   ├── ratings.csv
│   ├── credits.csv
│   ├── keywords.csv
│   └── links.csv
├── scripts/
│   ├── script_movies_metadata.py
│   ├── script_credits.py
│   ├── script_keywords.py
│   ├── script_links.py
│   ├── script_ratings.py
│   └── load_all.py
├── cte_query-result.csv
├── README.md
└── ...

```

### 🛠️ Configuração do banco

No início de cada script (ou no load_all.py), você verá algo assim:
```
DB_NAME = "filmes"
DB_USER = "postgres"
DB_PASSWORD = "postgres"
DB_HOST = "localhost"
DB_PORT = "5432"
```
**Essa é uma configuração de exemplo. Ajuste conforme o seu ambiente.**

### ✅ Carregar os dados
Baixe os arquivos CSV do [The Movies Dataset (Kaggle)](https://www.kaggle.com/datasets/rounakbanik/the-movies-dataset/data)
e coloque todos dentro da pasta movies_database/.

Depois, rode o script que executa tudo em sequência:

```python scripts/load_all.py```

E pronto. Agora é só sair explorando a base com as queries que você quiser. 😉

## 🚀 Veja o post no LinkedIn

[🔗 Clique aqui para ler a publicação no LinkedIn](https://www.linkedin.com/posts/victorhscampos_sql-cte-dados-activity-7320772274175762432-9vVJ)
