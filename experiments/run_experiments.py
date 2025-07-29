import sys
import os
import time
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import snowflake.connector
from config_loader import obter_configuracoes

# ====== CONFIGURAÇÃO ======
cenarios = ['normalizado', 'desnormalizado']
exp_ids = ['E1', 'E2', 'E3', 'E4', 'E5', 'E6']
repeticoes = 60
# ==========================

# --- CONEXÃO ---
try:
    config = obter_configuracoes()
    sf_config = {
        "user": config["SNOW_USER"],
        "password": config["SNOW_PASSWORD"],
        "account": config["SNOW_ACCOUNT"],
        "warehouse": config["SNOW_WAREHOUSE"],
        "database": config["SNOW_DATABASE"],
        "schema": config["SNOW_SCHEMA"]
    }
    conn = snowflake.connector.connect(**sf_config)
    warehouse_name = config.get('SNOW_WAREHOUSE')
    print(f"✓ Conexão com o Snowflake estabelecida.")
except Exception as e:
    print(f"✗ ERRO de conexão: {e}")
    exit(1)

def run_single_query(query_sql, query_ids_file, exec_num, repeticoes):
    print(f"\n--- Repetição {exec_num}/{repeticoes} ---")
    cursor = conn.cursor()
    try:
        print("  1. Limpando caches...")
        if warehouse_name:
            try:
                cursor.execute(f"ALTER WAREHOUSE {warehouse_name} SUSPEND;")
            except Exception as e:
                print(f"    (Aviso: {e})")
        cursor.execute("ALTER SESSION SET USE_CACHED_RESULT = FALSE;")
        time.sleep(2)

        print(f"  2. Executando a query principal...")
        cursor.execute(query_sql)
        sfqid = cursor.sfqid
        print(f"  ... Query executada com sucesso. ID: {sfqid}")
        
        # ETAPA 3: SALVAR O ID DA QUERY
        with open(query_ids_file, 'a') as f:
            f.write(f"{sfqid}\n")
        print(f"  ✓ ID salvo em '{os.path.basename(query_ids_file)}'")

    except Exception as e:
        print(f'  ✗ ERRO na execução {exec_num}: {e}')
    finally:
        cursor.close()

# --- EXECUÇÃO PARA TODOS OS CENÁRIOS E EXPERIMENTOS ---
for cenario in cenarios:
    for exp_id in exp_ids:
        print(f"\n==============================")
        print(f"Iniciando experimento '{exp_id}' para o cenário '{cenario}'")
        print(f"==============================")
        SQL_DIR = os.path.join(os.path.dirname(__file__), 'snowflake', cenario)
        RESULTS_DIR = os.path.join(os.path.dirname(__file__), 'results')
        os.makedirs(RESULTS_DIR, exist_ok=True)

        sql_file = os.path.join(SQL_DIR, f'{exp_id}.sql')
        query_ids_file = os.path.join(RESULTS_DIR, f'query_ids_{exp_id}_{cenario}.txt')

        # Limpa o arquivo de IDs de execuções anteriores
        if os.path.exists(query_ids_file):
            os.remove(query_ids_file)

        if not os.path.exists(sql_file):
            print(f"✗ Arquivo SQL '{sql_file}' não encontrado. Pulando...")
            continue

        with open(sql_file, 'r', encoding='utf-8') as f:
            query_sql = f.read().strip()

        for i in range(1, repeticoes + 1):
            run_single_query(query_sql, query_ids_file, i, repeticoes)

print("\nExecução de todos os experimentos finalizada. Todos os IDs foram salvos em seus respectivos arquivos.")
conn.close()