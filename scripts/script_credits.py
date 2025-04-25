import pandas as pd
import psycopg2
import ast

# === CONFIGURAÇÃO DO BANCO ===
DB_NAME = "filmes"
DB_USER = "postgres"
DB_PASSWORD = "postgres"
DB_HOST = "localhost"
DB_PORT = "5432"

# === FUNÇÕES DE TRANSFORMAÇÃO ===
def extrair_primeiro_nome(campo_json):
    try:
        lista = ast.literal_eval(campo_json)
        return lista[0]['name'] if lista else None
    except:
        return None

def extrair_diretor(campo_json):
    try:
        lista = ast.literal_eval(campo_json)
        return next(
            (p['name'] for p in lista if p.get('job') == 'Director'), None
        ) if lista else None
    except:
        return None

# === LEITURA E TRANSFORMAÇÃO DOS DADOS ===
df = pd.read_csv("movies_database/credits.csv", low_memory=False)

# Preparamos o DataFrame
credits = pd.DataFrame({
    "id_filme": pd.to_numeric(df["id"], errors='coerce', downcast='integer'),
    "ator_principal": df["cast"].apply(extrair_primeiro_nome),
    "diretor": df["crew"].apply(extrair_diretor)
})

# Remover entradas com ID nulo
credits = credits.dropna(subset=["id_filme"])

# === INSERÇÃO NO BANCO ===
conn = psycopg2.connect(
    dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
)
cur = conn.cursor()

# Criação da tabela (garante que a tabela existe)
cur.execute("""
    DROP TABLE IF EXISTS creditos;
    CREATE TABLE creditos (
        id_filme BIGINT PRIMARY KEY,
        ator_principal TEXT,
        diretor TEXT
    );
""")
conn.commit()

# === INSERÇÃO DAS LINHAS ===
print(f"Iniciando inserção de {len(credits)} linhas...")
linhas_inseridas = 0
linhas_com_erro = 0

for idx, row in credits.iterrows():
    try:
        row_data = list(row)

        # Tratamento de NaN para valores nulos
        if pd.isna(row_data[1]): row_data[1] = None  # ator_principal
        if pd.isna(row_data[2]): row_data[2] = None  # diretor

        # Insere os dados no banco
        cur.execute("""
            INSERT INTO creditos (id_filme, ator_principal, diretor)
            VALUES (%s, %s, %s)
            ON CONFLICT (id_filme) DO NOTHING;
        """, tuple(row_data))

        if cur.rowcount > 0:
            linhas_inseridas += 1

    except Exception as e:
        conn.rollback()
        linhas_com_erro += 1
        if linhas_com_erro <= 20:
            print(f"Erro na linha {idx}: {e}")
        elif linhas_com_erro == 21:
            print("Erros demais, suprimindo o resto...")
        continue  # Continua com a próxima linha

conn.commit()
cur.close()
conn.close()

print(f"[OK] Inserção finalizada: {linhas_inseridas} linhas inseridas com sucesso, {linhas_com_erro} erros.")
