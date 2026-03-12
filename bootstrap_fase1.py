import os

BASE_PATH = r"G:\Meu Drive\Opyta\Opyta_Data"

folders = [
    "app",
    "app/pages",
    "app/ui",
    "core",
    "runners",
    "validators",
    "scripts",
]

files = {
    "NORTE.md": "",
    "README.md": "# OPYTA DATA\n",
    "requirements.txt": "streamlit\nsupabase\npandas\npython-dotenv\n",
    ".gitignore": ".env\n__pycache__/\n*.pyc\n.venv/\n",
    ".env.example": "SUPABASE_URL=\nSUPABASE_ANON_KEY=\n",
    "app/main.py": "",
    "app/state.py": "",
    "app/ui/layout.py": "",
    "app/pages/01_Importacao.py": "",
    "app/pages/02_Consolidacao.py": "",
    "core/settings.py": "",
    "core/supabase_client.py": "",
    "runners/registry.py": "",
    "runners/script_runner.py": "",
    "validators/base.py": "",
    "validators/registry.py": "",
    "validators/common_checks.py": "",
}

print(f"Criando estrutura em: {BASE_PATH}\n")

# Criar pastas
for folder in folders:
    path = os.path.join(BASE_PATH, folder)
    os.makedirs(path, exist_ok=True)
    print(f"[OK] Pasta criada: {folder}")

# Criar arquivos
for file_path, content in files.items():
    full_path = os.path.join(BASE_PATH, file_path)
    if not os.path.exists(full_path):
        with open(full_path, "w", encoding="utf-8") as f:
            f.write(content)
        print(f"[OK] Arquivo criado: {file_path}")
    else:
        print(f"[SKIP] Já existe: {file_path}")

print("\nEstrutura da Fase 1 criada com sucesso 🚀")