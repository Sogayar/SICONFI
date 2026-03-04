import os
import re
import unicodedata
import pandas as pd

def slugify(txt, fallback="item"):
    if not txt:
        return fallback
    t = unicodedata.normalize("NFKD", txt).encode("ascii", "ignore").decode("ascii")
    t = re.sub(r"[^a-zA-Z0-9._-]+", "_", t).strip("_").lower()
    return t or fallback

def garantir_dir(path_dir):
    os.makedirs(path_dir, exist_ok=True)

def garantir_pasta_arquivo(path_file):
    pasta = os.path.dirname(path_file)
    if pasta:
        os.makedirs(pasta, exist_ok=True)

def read_cidades_csv(path_csv):
    df = pd.read_csv(path_csv)
    if not {"ente", "cod_ibge"}.issubset(df.columns):
        raise ValueError("CSV precisa conter colunas 'ente' e 'cod_ibge'.")
    return df

def parse_anos(txt):
    anos = []
    for p in str(txt).split(","):
        p = p.strip()
        if p.isdigit():
            anos.append(int(p))
    if not anos:
        raise ValueError("Informe pelo menos um ano (ex.: 2022,2023,2024).")
    anos.sort()
    return anos