import pandas as pd
import psycopg2

# === CONFIGURAÇÃO DO BANCO ===
DB_NAME = "filmes"
DB_USER = "postgres"
DB_PASSWORD = "postgres"
DB_HOST = "localhost"
DB_PORT = "5432"

# === LEITURA E TRATAMENTO DO CSV ===
df = pd.read_csv("movies_database/links.csv")

links = pd.DataFrame({
    "movie_id": pd.to_numeric(df["movieId"], errors="coerce", downcast="integer"),
    "imdb_id": pd.to_numeric(df["imdbId"], errors="coerce", downcast="integer"),
    "tmdb_id": pd.to_numeric(df["tmdbId"], errors="coerce", downcast="integer")
})

links = links.dropna(subset=["movie_id"])

# === CONEXÃO COM O BANCO ===
conn = psycopg2.connect(
    dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
)
cur = conn.cursor()

# === CRIAÇÃO DA TABELA ===
cur.execute("""
    DROP TABLE IF EXISTS links;
    CREATE TABLE links (
        movie_id BIGINT PRIMARY KEY,
        imdb_id BIGINT,
        tmdb_id BIGINT
    );
""")
conn.commit()

# === INSERÇÃO DOS DADOS ===
print(f"Iniciando inserção de {len(links)} linhas...")
linhas_inseridas = 0
linhas_com_erro = 0

for idx, row in links.iterrows():
    try:
        row_data = list(row)
        for i in range(3):
            if pd.isna(row_data[i]):
                row_data[i] = None

        cur.execute("""
            INSERT INTO links (movie_id, imdb_id, tmdb_id)
            VALUES (%s, %s, %s)
            ON CONFLICT (movie_id) DO NOTHING;
        """, tuple(row_data))

        if cur.rowcount > 0:
            linhas_inseridas += 1

    except Exception as e:
        conn.rollback()
        linhas_com_erro += 1
        if linhas_com_erro <= 10:
            print(f"Erro na linha {idx}: {e}")
        elif linhas_com_erro == 11:
            print("Muitos erros, suprimindo detalhes...")
        continue

conn.commit()
cur.close()
conn.close()

print(f"[OK] Inserção finalizada!")
print(f"   {linhas_inseridas} linhas inseridas com sucesso.")
print(f"   {linhas_com_erro} erros encontrados.")
