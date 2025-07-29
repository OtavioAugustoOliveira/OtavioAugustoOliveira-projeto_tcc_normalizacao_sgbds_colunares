import sys
import os
import re
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import snowflake.connector
import pandas as pd
from config_loader import obter_configuracoes
import time

print("--- INICIANDO SCRIPT DE COLETA DE MÉTRICAS (Versão Corrigida) ---")

# ====== CONFIGURAÇÃO ======
RESULTS_DIR = os.path.join(os.path.dirname(__file__), 'results')
# Expressão regular para encontrar os arquivos de IDs
re_query_ids = re.compile(r'query_ids_(E\d)_(normalizado|desnormalizado)\.txt$')
# ==========================

# --- 1. ENCONTRAR E LER TODOS OS IDs DE UMA SÓ VEZ ---
all_query_ids = []
id_to_experiment_map = {}
query_id_files = [f for f in os.listdir(RESULTS_DIR) if re_query_ids.match(f)]

if not query_id_files:
    print(f"ERRO: Nenhum arquivo de IDs (query_ids_*.txt) encontrado em '{RESULTS_DIR}'.")
    exit(1)

print(f"Encontrados {len(query_id_files)} arquivos de experimentos para processar.")
for fname in query_id_files:
    m = re_query_ids.match(fname)
    exp_id, cenario = m.group(1), m.group(2)
    query_ids_file = os.path.join(RESULTS_DIR, fname)
    
    with open(query_ids_file, 'r') as f:
        ids_in_file = [line.strip() for line in f if line.strip()]
        if not ids_in_file:
            print(f"  -> Aviso: Arquivo '{fname}' está vazio. Será ignorado.")
            continue
            
        all_query_ids.extend(ids_in_file)
        for qid in ids_in_file:
            id_to_experiment_map[qid] = {'EXPERIMENTO_ID': exp_id, 'CENARIO': cenario}

if not all_query_ids:
    print("ERRO: Nenhum Query ID válido foi encontrado em nenhum dos arquivos.")
    exit(1)

unique_query_ids = sorted(list(set(all_query_ids)))
print(f"Total de {len(unique_query_ids)} Query IDs únicos encontrados para coletar.")

# --- 2. CONEXÃO COM O SNOWFLAKE (SUA LÓGICA RESTAURADA) ---
try:
    config = obter_configuracoes()
    required_keys = [
        "SNOW_USER", "SNOW_PASSWORD", "SNOW_ACCOUNT",
        "SNOW_WAREHOUSE", "SNOW_DATABASE", "SNOW_SCHEMA"
    ]
    for key in required_keys:
        if not config.get(key):
            print(f"ERRO: Variável de ambiente/configuração '{key}' não encontrada.")
            exit(1)
    sf_config = {
        "user": config["SNOW_USER"],
        "password": config["SNOW_PASSWORD"],
        "account": config["SNOW_ACCOUNT"],
        "warehouse": config["SNOW_WAREHOUSE"],
        "database": config["SNOW_DATABASE"],
        "schema": config["SNOW_SCHEMA"]
    }
    conn = snowflake.connector.connect(**sf_config)
    print("✓ Conexão com o Snowflake estabelecida.")
except Exception as e:
    print(f"✗ ERRO de conexão: {e}")
    exit(1)

# --- 3. QUERY DE MÉTRICAS AJUSTADA ---
# Esta query agora tenta buscar todas as métricas, incluindo as de partição
METRICS_QUERY = '''
SELECT
    QUERY_ID,
    QUERY_TEXT,
    EXECUTION_STATUS,
    WAREHOUSE_NAME,
    WAREHOUSE_SIZE,
    TOTAL_ELAPSED_TIME,
    EXECUTION_TIME,
    BYTES_SCANNED,
    ROWS_PRODUCED,
    PARTITIONS_SCANNED,
    PARTITIONS_TOTAL
FROM
    SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE
    QUERY_ID = %s;
'''

# --- 4. EXECUÇÃO DA COLETA (SUA LÓGICA RESTAURADA) ---
all_metrics = []
try:
    cursor = conn.cursor()
    print("\nExecutando a coleta de métricas no Snowflake (uma por uma)...")
    for idx, qid in enumerate(unique_query_ids):
        print(f"  Coletando ({idx+1}/{len(unique_query_ids)}): {qid}")
        try:
            cursor.execute(METRICS_QUERY, [qid])
            result = cursor.fetchone()
            if result:
                all_metrics.append(result)
            else:
                print(f"    -> Aviso: QUERY_ID não encontrado no histórico: {qid}")
        except Exception as e:
            print(f"    -> Erro ao coletar QUERY_ID {qid}: {e}")
        time.sleep(1)  # Sua pausa de 1 segundo entre cada requisição

    column_names = [desc[0] for desc in cursor.description]
    all_metrics_df = pd.DataFrame(all_metrics, columns=column_names)
    print("✓ Métricas recebidas do Snowflake.")
except Exception as e:
    print(f"✗ ERRO CRÍTICO durante a coleta: {e}")
finally:
    cursor.close()
    conn.close()

# --- 5. VALIDAÇÃO E ENRIQUECIMENTO DOS DADOS (SUA LÓGICA RESTAURADA) ---
print("\nValidando e enriquecendo os dados coletados...")
if all_metrics_df.empty:
    print("ERRO: Nenhuma métrica foi retornada pelo Snowflake para os IDs fornecidos.")
    exit(1)

found_ids = set(all_metrics_df['QUERY_ID'])
missing_ids = set(unique_query_ids) - found_ids
if missing_ids:
    print(f"  -> Alerta: {len(missing_ids)} de {len(unique_query_ids)} Query IDs não foram encontrados.")

experiment_info_df = pd.DataFrame.from_dict(id_to_experiment_map, orient='index').reset_index().rename(columns={'index': 'QUERY_ID'})
final_df = pd.merge(all_metrics_df, experiment_info_df, on='QUERY_ID', how='left')
final_df['REPETICAO'] = final_df.groupby(['EXPERIMENTO_ID', 'CENARIO']).cumcount() + 1

# --- 6. SALVAMENTO DO ARQUIVO FINAL ---
final_cols_order = [
    'EXPERIMENTO_ID', 'CENARIO', 'REPETICAO', 'QUERY_ID', 'EXECUTION_STATUS',
    'WAREHOUSE_SIZE', 'TOTAL_ELAPSED_TIME', 'EXECUTION_TIME',
    'BYTES_SCANNED', 'ROWS_PRODUCED', 'QUERY_TEXT', 'PARTITIONS_SCANNED', 'PARTITIONS_TOTAL'
]
final_cols_order = [col for col in final_cols_order if col in final_df.columns]
final_df = final_df.reindex(columns=final_cols_order)
output_path = os.path.join(RESULTS_DIR, 'TODOS_OS_RESULTADOS_CONSOLIDADOS.csv')
final_df.to_csv(output_path, index=False)

print(f"\n✓ PROCESSO FINALIZADO! {len(final_df)} registros de métricas salvos em:\n{output_path}")