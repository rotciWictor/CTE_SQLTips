import subprocess

scripts = [
    "scripts/script_movies_metadata.py",
    "scripts/script_credits.py",
    "scripts/script_keywords.py",
    "scripts/script_links.py",
    "scripts/script_ratings.py"
]

print("[INICIO] Iniciando carga completa dos dados...\n")

for script in scripts:
    print(f"[EXEC] Executando: {script}")
    result = subprocess.run(["python", script], capture_output=True, text=True)
    if result.returncode == 0:
        print(f"[OK] Finalizado com sucesso: {script}\n")
    else:
        print(f"[ERRO] Erro ao executar {script}:\n{result.stderr}")
        break

print("[FIM] Processo concluído.")
