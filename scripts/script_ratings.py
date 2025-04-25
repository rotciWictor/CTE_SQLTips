import pandas as pd
import psycopg2
from io import StringIO

# === CONFIGURAÇÃO ===
DB_NAME = "filmes"
DB_USER = "postgres"
DB_PASSWORD = "postgres"
DB_HOST = "localhost"
DB_PORT = "5432"

# === LEITURA DO ARQUIVO ===
df = pd.read_csv("movies_database/ratings.csv", low_memory=False)

# === RENOMEIA COLUNAS PARA PADRÃO DA QUERY FINAL ===
df.rename(columns={
    "userId": "user_id",
    "movieId": "id_filme",
    "rating": "nota"
}, inplace=True)

# === CONEXÃO COM O BANCO ===
conn = psycopg2.connect(
    dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
)
cur = conn.cursor()

# === CRIAÇÃO DA TABELA NO BANCO ===
cur.execute("""
    DROP TABLE IF EXISTS avaliacoes;
    CREATE TABLE avaliacoes (
        user_id INTEGER,
        id_filme BIGINT,
        nota NUMERIC,
        timestamp BIGINT
    );
""")
conn.commit()

# === EXPORTA O DATAFRAME COMO CSV EM MEMÓRIA PARA COPY ===
buffer = StringIO()
df.to_csv(buffer, index=False, header=False)
buffer.seek(0)

# === IMPORTAÇÃO COM COPY (RÁPIDA E EFICIENTE) ===
print("[INICIO] Iniciando importação com copy_expert...")
cur.copy_expert("COPY avaliacoes(user_id, id_filme, nota, timestamp) FROM STDIN WITH CSV", buffer)
conn.commit()
cur.close()
conn.close()
print("[OK] Importação concluída com sucesso!")
