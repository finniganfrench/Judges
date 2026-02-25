import requests
import pandas as pd
import time

API_KEY = "cDZHYzlZa0JadVREZDJCendQbXY6SkJlTzNjLV9TRENyQk1RdnFKZGRQdw=="
HEADERS = {
    "Authorization": f"APIKey {API_KEY}",
    "Content-Type": "application/json"
}

COURT_ENDPOINTS = {
    "TRE-AC": "https://api-publica.datajud.cnj.jus.br/api_publica_tre-ac/_search",
    "TRE-AL": "https://api-publica.datajud.cnj.jus.br/api_publica_tre-al/_search",
    "TRE-AM": "https://api-publica.datajud.cnj.jus.br/api_publica_tre-am/_search",
    "TRE-AP": "https://api-publica.datajud.cnj.jus.br/api_publica_tre-ap/_search",
    "TRE-BA": "https://api-publica.datajud.cnj.jus.br/api_publica_tre-ba/_search",
    "TRE-CE": "https://api-publica.datajud.cnj.jus.br/api_publica_tre-ce/_search",
    "TRE-DF": "https://api-publica.datajud.cnj.jus.br/api_publica_tre-dft/_search",
    "TRE-ES": "https://api-publica.datajud.cnj.jus.br/api_publica_tre-es/_search",
    "TRE-GO": "https://api-publica.datajud.cnj.jus.br/api_publica_tre-go/_search",
    "TRE-MA": "https://api-publica.datajud.cnj.jus.br/api_publica_tre-ma/_search",
    "TRE-MG": "https://api-publica.datajud.cnj.jus.br/api_publica_tre-mg/_search",
    "TRE-MS": "https://api-publica.datajud.cnj.jus.br/api_publica_tre-ms/_search",
    "TRE-MT": "https://api-publica.datajud.cnj.jus.br/api_publica_tre-mt/_search",
    "TRE-PA": "https://api-publica.datajud.cnj.jus.br/api_publica_tre-pa/_search",
    "TRE-PB": "https://api-publica.datajud.cnj.jus.br/api_publica_tre-pb/_search",
    "TRE-PE": "https://api-publica.datajud.cnj.jus.br/api_publica_tre-pe/_search",
    "TRE-PI": "https://api-publica.datajud.cnj.jus.br/api_publica_tre-pi/_search",
    "TRE-PR": "https://api-publica.datajud.cnj.jus.br/api_publica_tre-pr/_search",
    "TRE-RJ": "https://api-publica.datajud.cnj.jus.br/api_publica_tre-rj/_search",
    "TRE-RN": "https://api-publica.datajud.cnj.jus.br/api_publica_tre-rn/_search",
    "TRE-RO": "https://api-publica.datajud.cnj.jus.br/api_publica_tre-ro/_search",
    "TRE-RR": "https://api-publica.datajud.cnj.jus.br/api_publica_tre-rr/_search",
    "TRE-RS": "https://api-publica.datajud.cnj.jus.br/api_publica_tre-rs/_search",
    "TRE-SC": "https://api-publica.datajud.cnj.jus.br/api_publica_tre-sc/_search",
    "TRE-SE": "https://api-publica.datajud.cnj.jus.br/api_publica_tre-se/_search",
    "TRE-SP": "https://api-publica.datajud.cnj.jus.br/api_publica_tre-sp/_search",
    "TRE-TO": "https://api-publica.datajud.cnj.jus.br/api_publica_tre-to/_search",
    "TSE":    "https://api-publica.datajud.cnj.jus.br/api_publica_tse/_search"
}

def parse_dt(x):
    if x is None:
        return pd.NaT
    x = str(x)
    if x.isdigit() and len(x) == 14:
        return pd.to_datetime(x, format="%Y%m%d%H%M%S", errors="coerce")
    if x.isdigit() and len(x) == 8:
        return pd.to_datetime(x, format="%Y%m%d", errors="coerce")
    return pd.to_datetime(x, errors="coerce")

def fetch_cases_by_cabinet(url, cabinet_code):
    """Fetches up to 1000 cases belonging to a specific judge cabinet (orgaoJulgador_codigo)"""
    payload = {
        "size": 1000,  # Max number of cases to return per cabinet. Increase if needed (API max is usually 10000).
        "query": {"match": {"orgaoJulgador.codigo": cabinet_code}},
        "_source": True
    }
    
    try:
        r = requests.post(url, headers=HEADERS, json=payload, timeout=30)
        if r.status_code != 200:
            return []
        
        hits = r.json().get("hits", {}).get("hits", [])
        return [hit["_source"] for hit in hits]
    
    except requests.exceptions.RequestException as e:
        print(f"Request failed for cabinet {cabinet_code}: {e}")
        return []

# Load the mapping CSV you generated in the previous step
# NOTE: Make sure your CSV has a column for the court/tribunal (e.g., 'TRE-SP') 
# so the script knows which endpoint to query.
mapping_df = pd.read_csv("Judge_Cabinet_Mapping.csv", dtype=str)

all_rows = []

for idx, row in mapping_df.iterrows():
    judge_name = row.get("assigned_judge")
    cabinet_code = row.get("orgaoJulgador_codigo")
    court = row.get("tribunal")  # Ensure this column exists in your CSV!

    if pd.isna(cabinet_code) or pd.isna(court):
        continue

    url = COURT_ENDPOINTS.get(court)
    if not url:
        print(f"⚠️ Unknown court for cabinet {cabinet_code}: {court}")
        continue

    # Fetch all cases for this cabinet
    cases = fetch_cases_by_cabinet(url, cabinet_code)
    
    if not cases:
        print(f"NOT FOUND or NO CASES for cabinet {cabinet_code} in {court}")
        continue

    print(f"FOUND {len(cases)} cases for cabinet {cabinet_code} ({judge_name}) in {court}")

    # Process every case returned for this cabinet
    for src in cases:
        # ---- Capa (processo) ----
        base = {
            "judge_name": judge_name,
            "cabinet_code": cabinet_code,
            "id": src.get("id"),
            "tribunal": src.get("tribunal"),
            "numeroProcesso": src.get("numeroProcesso"),
            "dataAjuizamento_raw": src.get("dataAjuizamento"),
            "dataAjuizamento": parse_dt(src.get("dataAjuizamento")),
            "grau": src.get("grau"),
            "nivelSigilo": src.get("nivelSigilo"),
            "formato_codigo": (src.get("formato") or {}).get("codigo"),
            "formato_nome": (src.get("formato") or {}).get("nome"),
            "sistema_codigo": (src.get("sistema") or {}).get("codigo"),
            "sistema_nome": (src.get("sistema") or {}).get("nome"),
            "classe_codigo": (src.get("classe") or {}).get("codigo"),
            "classe_nome": (src.get("classe") or {}).get("nome"),
            "orgaoJulgador_codigo": (src.get("orgaoJulgador") or {}).get("codigo"),
            "orgaoJulgador_nome": (src.get("orgaoJulgador") or {}).get("nome"),
            "orgaoJulgador_codigoMunicipioIBGE": (src.get("orgaoJulgador") or {}).get("codigoMunicipioIBGE"),
            "dataHoraUltimaAtualizacao_raw": src.get("dataHoraUltimaAtualizacao"),
            "dataHoraUltimaAtualizacao": parse_dt(src.get("dataHoraUltimaAtualizacao")),
            "timestamp_raw": src.get("@timestamp"),
            "timestamp": parse_dt(src.get("@timestamp")),
        }

        assuntos = src.get("assuntos", []) or [None]
        movimentos = src.get("movimentos", []) or [None]

        for a in assuntos:
            for i, m in enumerate(movimentos):
                comps = (m.get("complementosTabelados") if m else None) or [None]

                for j, c in enumerate(comps):
                    r = base.copy()

                    # assunto
                    r["assunto_codigo"] = a.get("codigo") if a else None
                    r["assunto_nome"] = a.get("nome") if a else None

                    # movimento
                    r["mov_index"] = i if m else None
                    r["mov_codigo"] = m.get("codigo") if m else None
                    r["mov_nome"] = m.get("nome") if m else None
                    r["mov_dataHora"] = parse_dt(m.get("dataHora")) if m else None

                    # complemento
                    r["comp_index"] = j if c else None
                    r["comp_codigo"] = c.get("codigo") if c else None
                    r["comp_nome"] = c.get("nome") if c else None
                    r["comp_valor"] = c.get("valor") if c else None
                    r["comp_descricao"] = c.get("descricao") if c else None

                    all_rows.append(r)

    time.sleep(0.3)  # Be nice to the API

# Save to one big CSV
df = pd.DataFrame(all_rows)

# Reorder columns to guarantee judge_name and cabinet_code are exactly columns 1 and 2
if not df.empty:
    cols = ["judge_name", "cabinet_code"] + [c for c in df.columns if c not in ["judge_name", "cabinet_code"]]
    df = df[cols]

df.to_csv("datajud_cases_by_cabinet.csv", index=False)
print("DONE — one big CSV written: datajud_cases_by_cabinet.csv")
