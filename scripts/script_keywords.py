import pandas as pd
import psycopg2
import ast

# === CONFIGURAÇÃO DO BANCO ===
DB_NAME = "filmes"
DB_USER = "postgres"
DB_PASSWORD = "postgres"
DB_HOST = "localhost"
DB_PORT = "5432"

# === FUNÇÃO DE EXTRAÇÃO DOS NOMES DAS KEYWORDS ===
def extrair_keywords(lista_json):
    try:
        lista = ast.literal_eval(lista_json)
        nomes = [item["name"] for item in lista if "name" in item]
        return ", ".join(nomes)
    except:
        return None

# === LEITURA E TRATAMENTO DO CSV ===
df = pd.read_csv("movies_database/keywords.csv")

keywords = pd.DataFrame({
    "id": pd.to_numeric(df["id"], errors="coerce", downcast="integer"),
    "keywords": df["keywords"].apply(extrair_keywords)
})

keywords = keywords.dropna(subset=["id"])

# === CONEXÃO COM O BANCO ===
conn = psycopg2.connect(
    dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
)
cur = conn.cursor()

# === CRIAÇÃO DA TABELA ===
cur.execute("""
    DROP TABLE IF EXISTS keywords;
    CREATE TABLE keywords (
        id BIGINT PRIMARY KEY,
        keywords TEXT
    );
""")
conn.commit()

# === INSERÇÃO DOS DADOS ===
print(f"Iniciando inserção de {len(keywords)} linhas...")
linhas_inseridas = 0
linhas_com_erro = 0

for idx, row in keywords.iterrows():
    try:
        row_data = list(row)
        if pd.isna(row_data[1]):
            row_data[1] = None

        cur.execute("""
            INSERT INTO keywords (id, keywords)
            VALUES (%s, %s)
            ON CONFLICT (id) DO NOTHING;
        """, tuple(row_data))

        if cur.rowcount > 0:
            linhas_inseridas += 1

    except Exception as e:
        conn.rollback()
        linhas_com_erro += 1
        if linhas_com_erro <= 20:
            print(f"Erro na linha {idx}: {e}")
        elif linhas_com_erro == 21:
            print("Muitos erros, suprimindo detalhes...")
        continue

conn.commit()
cur.close()
conn.close()

print(f"[OK] Inserção finalizada!")
print(f"   {linhas_inseridas} linhas inseridas com sucesso.")
print(f"   {linhas_com_erro} erros encontrados.")
