import pandas as pd
import psycopg2
import ast

# === CONFIGURAÇÃO DO BANCO ===
DB_NAME = "filmes"
DB_USER = "postgres"
DB_PASSWORD = "postgres"
DB_HOST = "localhost"
DB_PORT = "5432"

# === FUNÇÃO DE EXTRAÇÃO DE GÊNERO ===
def extrair_genero(genero_json):
    try:
        lista = ast.literal_eval(genero_json)
        return lista[0]['name'] if lista else None
    except:
        return None

# === LEITURA E TRANSFORMAÇÃO ===
df = pd.read_csv("movies_database/movies_metadata.csv", low_memory=False)

filmes = pd.DataFrame({
    "id": pd.to_numeric(df["id"], errors='coerce', downcast='integer'),
    "titulo": df["title"],
    "data_lancamento": pd.to_datetime(df["release_date"], errors='coerce'),
    "duracao": pd.to_numeric(df["runtime"], errors='coerce'),
    "genero": df["genres"].apply(extrair_genero),
    "orcamento": pd.to_numeric(df["budget"], errors='coerce'),
    "receita": pd.to_numeric(df["revenue"], errors='coerce'),
    "nota_media": pd.to_numeric(df["vote_average"], errors='coerce'),
    "qtd_votos": pd.to_numeric(df["vote_count"], errors='coerce')
})

filmes = filmes.dropna(subset=['id'])

# Novas colunas:
filmes["ano"] = filmes["data_lancamento"].dt.year
filmes["id_filme"] = filmes["id"].astype("Int64")

# === CONEXÃO COM O BANCO ===
conn = psycopg2.connect(
    dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
)
cur = conn.cursor()

# === CRIAÇÃO DA TABELA COM NOVAS COLUNAS ===
cur.execute("""
    DROP TABLE IF EXISTS filmes;
    CREATE TABLE filmes (
        id BIGINT PRIMARY KEY,
        titulo TEXT,
        data_lancamento DATE,
        duracao INTEGER,
        genero TEXT,
        orcamento NUMERIC,
        receita NUMERIC,
        nota_media NUMERIC,
        qtd_votos INTEGER,
        ano INTEGER,
        id_filme BIGINT
    );
""")
conn.commit()

# === INSERÇÃO DOS DADOS ===
print(f"Iniciando inserção de {len(filmes)} linhas...")
linhas_inseridas = 0
linhas_com_erro = 0

for idx, row in filmes.iterrows():
    try:
        row_data = list(row)

        for i in range(len(row_data)):
            if pd.isna(row_data[i]):
                row_data[i] = None

        cur.execute("""
            INSERT INTO filmes (id, titulo, data_lancamento, duracao, genero, orcamento, receita, nota_media, qtd_votos, ano, id_filme)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING;
        """, tuple(row_data))

        if cur.rowcount > 0:
            linhas_inseridas += 1

    except Exception as e:
        conn.rollback()
        linhas_com_erro += 1
        if linhas_com_erro <= 20:
            print(f"Erro ao processar linha {idx}: {e}")
        elif linhas_com_erro == 21:
            print("Mais de 20 erros detectados. Suprimindo mensagens adicionais.")
        continue

conn.commit()
cur.close()
conn.close()

print("✅ Inserção finalizada!")
print(f"   {linhas_inseridas} linhas inseridas com sucesso.")
print(f"   {linhas_com_erro} linhas encontraram erros.")
