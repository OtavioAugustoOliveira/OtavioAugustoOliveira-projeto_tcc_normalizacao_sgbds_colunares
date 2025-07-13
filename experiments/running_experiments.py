import pandas as pd
import time
import psycopg2
import pyodbc
import snowflake.connector
import argparse
import re
import sys
import os

# Adiciona o diretório raiz ao path para importar config_loader
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from config_loader import obter_configuracoes, imprimir_configuracoes

# --- CONFIGURAÇÃO GLOBAL ---
# Carrega as configurações do arquivo JSON via config_loader
config = obter_configuracoes()

# Extrai as variáveis de configuração
PG_USER = config['PG_USER']
PG_PASSWORD = config['PG_PASSWORD']
PG_HOST = config['PG_HOST']
PG_PORT = config['PG_PORT']
PG_DBNAME = config['PG_DBNAME']

SQL_SERVER_NAME = config['SQL_SERVER_NAME']
SQL_DBNAME = config['SQL_DBNAME']
SQL_USER = config['SQL_USER']
SQL_PASSWORD = config['SQL_PASSWORD']

SNOW_USER = config['SNOW_USER']
SNOW_PASSWORD = config['SNOW_PASSWORD']
SNOW_ACCOUNT = config['SNOW_ACCOUNT']
SNOW_WAREHOUSE = config['SNOW_WAREHOUSE']
SNOW_DATABASE = config['SNOW_DATABASE']
SNOW_SCHEMA = config['SNOW_SCHEMA']

# Imprime as configurações carregadas
print("=== CONFIGURAÇÕES CARREGADAS ===")
imprimir_configuracoes()

# Debug: Mostra as configurações do PostgreSQL
print(f"\nDEBUG - Configurações PostgreSQL:")
print(f"Host: {PG_HOST}")
print(f"Port: {PG_PORT}")
print(f"Database: {PG_DBNAME}")
print(f"User: {PG_USER}")
print(f"Password: {'*' * len(PG_PASSWORD) if PG_PASSWORD else 'VAZIO'}")

# --- CONFIGURAÇÕES DE CONEXÃO ---
CONEXOES = {
    'PostgreSQL': {
        'host': PG_HOST,
        'port': PG_PORT,
        'database': PG_DBNAME,
        'user': PG_USER,
        'password': PG_PASSWORD
    },
    'SQL Server': {
        'server': SQL_SERVER_NAME,
        'database': SQL_DBNAME,
        'user': SQL_USER,
        'password': SQL_PASSWORD,
        'driver': '{ODBC Driver 17 for SQL Server}'
    },
    'Snowflake': {
        'user': SNOW_USER,
        'password': SNOW_PASSWORD,
        'account': SNOW_ACCOUNT,
        'warehouse': SNOW_WAREHOUSE,
        'database': SNOW_DATABASE,
        'schema': SNOW_SCHEMA
    }
}

# --- BIBLIOTECA DE QUERIES ---
# Queries coletadas dos arquivos de configuração para cada banco

QUERIES = {
    'PostgreSQL': {
        'Desnormalizado': {
            'E1': "SELECT SIGLA_UF, SUM(AREA_KM2) AS area_total_km2 FROM cenario_nao_normalizado GROUP BY SIGLA_UF ORDER BY SIGLA_UF;",
            'E2': "SELECT NM_MUN, NM_UF FROM cenario_nao_normalizado WHERE ST_Contains(geometry, ST_SetSRID(ST_MakePoint(-34.8779, -8.0578), 4326));",
            'E3': "SELECT NM_MUN, AREA_KM2 FROM cenario_nao_normalizado WHERE NM_REGIAO = 'Nordeste' AND AREA_KM2 > 5000;",
            'E4': "SELECT A.NM_MUN AS municipio_origem, B.NM_MUN AS municipio_vizinho FROM cenario_nao_normalizado AS A JOIN cenario_nao_normalizado AS B ON ST_Touches(A.geometry, B.geometry) WHERE A.SIGLA_UF = 'PE' AND A.CD_MUN < B.CD_MUN;"
        },
        'Normalizado': {
            'E1': "SELECT est.SIGLA_UF, SUM(mun.AREA_KM2) AS area_total_km2 FROM fato_municipios AS mun JOIN dim_estados AS est ON mun.ID_UF_FK = est.ID_UF GROUP BY est.SIGLA_UF ORDER BY est.SIGLA_UF;",
            'E2': "SELECT mun.NM_MUN, est.NM_UF FROM fato_municipios AS mun JOIN dim_estados AS est ON mun.ID_UF_FK = est.ID_UF WHERE ST_Contains(mun.geometry, ST_SetSRID(ST_MakePoint(-34.8779, -8.0578), 4326));",
            'E3': "SELECT mun.NM_MUN, mun.AREA_KM2 FROM fato_municipios AS mun JOIN dim_estados AS est ON mun.ID_UF_FK = est.ID_UF JOIN dim_regioes AS reg ON est.ID_REGIAO_FK = reg.ID_REGIAO WHERE reg.NM_REGIAO = 'Nordeste' AND mun.AREA_KM2 > 5000;",
            'E4': "SELECT A.NM_MUN AS municipio_origem, B.NM_MUN AS municipio_vizinho FROM fato_municipios AS A JOIN fato_municipios AS B ON ST_Touches(A.geometry, B.geometry) WHERE A.ID_UF_FK = '26' AND A.ID_MUNICIPIO < B.ID_MUNICIPIO;"
        }
    },
    'SQL Server': {
        'Desnormalizado': {
            'E1': "SELECT SIGLA_UF, SUM(AREA_KM2) AS area_total_km2 FROM cenario_nao_normalizado GROUP BY SIGLA_UF ORDER BY SIGLA_UF;",
            'E2': "SELECT NM_MUN, NM_UF FROM cenario_nao_normalizado WHERE geometry.STContains(geography::Point(-8.0578, -34.8779, 4326)) = 1;",
            'E3': "SELECT NM_MUN, AREA_KM2 FROM cenario_nao_normalizado WHERE NM_REGIAO = 'Nordeste' AND AREA_KM2 > 5000;",
            'E4': "SELECT A.NM_MUN AS municipio_origem, B.NM_MUN AS municipio_vizinho FROM cenario_nao_normalizado AS A JOIN cenario_nao_normalizado AS B ON A.geometry.STTouches(B.geometry) = 1 WHERE A.SIGLA_UF = 'PE' AND A.CD_MUN < B.CD_MUN;"
        },
        'Normalizado': {
            'E1': "SELECT est.SIGLA_UF, SUM(mun.AREA_KM2) AS area_total_km2 FROM fato_municipios AS mun JOIN dim_estados AS est ON mun.ID_UF_FK = est.ID_UF GROUP BY est.SIGLA_UF ORDER BY est.SIGLA_UF;",
            'E2': "SELECT mun.NM_MUN, est.NM_UF FROM fato_municipios AS mun JOIN dim_estados AS est ON mun.ID_UF_FK = est.ID_UF WHERE mun.geometry.STContains(geography::Point(-8.0578, -34.8779, 4326)) = 1;",
            'E3': "SELECT mun.NM_MUN, mun.AREA_KM2 FROM fato_municipios AS mun JOIN dim_estados AS est ON mun.ID_UF_FK = est.ID_UF JOIN dim_regioes AS reg ON est.ID_REGIAO_FK = reg.ID_REGIAO WHERE reg.NM_REGIAO = 'Nordeste' AND mun.AREA_KM2 > 5000;",
            'E4': "SELECT A.NM_MUN AS municipio_origem, B.NM_MUN AS municipio_vizinho FROM fato_municipios AS A JOIN fato_municipios AS B ON A.geometry.STTouches(B.geometry) = 1 WHERE A.ID_UF_FK = 26 AND A.ID_MUNICIPIO < B.ID_MUNICIPIO;"
        }
    },
    'Snowflake': {
        'Desnormalizado': {
            'E1': "SELECT SIGLA_UF, SUM(AREA_KM2) AS area_total_km2 FROM cenario_nao_normalizado GROUP BY SIGLA_UF ORDER BY SIGLA_UF;",
            'E2': "SELECT NM_MUN, NM_UF FROM CENARIO_NAO_NORMALIZADO WHERE ST_CONTAINS(GEOMETRY, ST_POINT(-34.8779, -8.0578));",
            'E3': "SELECT NM_MUN, AREA_KM2 FROM CENARIO_NAO_NORMALIZADO WHERE NM_REGIAO = 'Nordeste' AND AREA_KM2 > 5000;",
            'E4': "SELECT A.NM_MUN AS municipio_origem, B.NM_MUN AS municipio_vizinho FROM cenario_nao_normalizado AS A JOIN cenario_nao_normalizado AS B ON ST_Touches(A.geometry, B.geometry) WHERE A.SIGLA_UF = 'PE' AND A.CD_MUN < B.CD_MUN;"
        },
        'Normalizado': {
            'E1': "SELECT est.SIGLA_UF, SUM(mun.AREA_KM2) AS area_total_km2 FROM fato_municipios AS mun JOIN dim_estados AS est ON mun.ID_UF_FK = est.ID_UF GROUP BY est.SIGLA_UF ORDER BY est.SIGLA_UF;",
            'E2': "SELECT mun.NM_MUN, est.NM_UF FROM FATO_MUNICIPIOS AS mun JOIN DIM_ESTADOS AS est ON mun.ID_UF_FK = est.ID_UF WHERE ST_CONTAINS(mun.GEOMETRY, ST_POINT(-34.8779, -8.0578));",
            'E3': "SELECT mun.NM_MUN, mun.AREA_KM2 FROM FATO_MUNICIPIOS AS mun JOIN DIM_ESTADOS AS est ON mun.ID_UF_FK = est.ID_UF JOIN DIM_REGIOES AS reg ON est.ID_REGIAO_FK = reg.ID_REGIAO WHERE reg.NM_REGIAO = 'Nordeste' AND mun.AREA_KM2 > 5000;",
            'E4': "SELECT A.NM_MUN AS municipio_origem, B.NM_MUN AS municipio_vizinho FROM fato_municipios AS A JOIN fato_municipios AS B ON ST_Touches(A.geometry, B.geometry) WHERE A.ID_UF_FK = '26' AND A.ID_MUNICIPIO < B.ID_MUNICIPIO;"
        }
    }
}


NUM_EXECUCOES = 11  # 1 de aquecimento + 10 válidas

def get_connection(sgbd_name, config):
    """Cria e retorna uma conexão com o banco de dados apropriado."""
    conn = None
    if sgbd_name == 'PostgreSQL':
        conn = psycopg2.connect(**config)
    elif sgbd_name == 'SQL Server':
        conn_str = (
            f"DRIVER={config['driver']};"
            f"SERVER={config['server']};"
            f"DATABASE={config['database']};"
            f"UID={config['user']};"
            f"PWD={config['password']};"
        )
        conn = pyodbc.connect(conn_str)
    elif sgbd_name == 'Snowflake':
        conn = snowflake.connector.connect(**config)
    return conn

def run_and_measure(sgbd, modelo, query_id):
    """Executa um único experimento e retorna os resultados detalhados."""
    config = CONEXOES[sgbd]
    sql = QUERIES[sgbd][modelo][query_id]
    
    if "-- COLE AQUI" in sql:
        print(f"ERRO: A query para {sgbd}/{modelo}/{query_id} não foi preenchida. Pulando experimento.")
        return []
        
    resultados_experimento = []
    
    print(f"Iniciando experimento: SGBD={sgbd}, Modelo={modelo}, Query={query_id}")

    try:
        conn = get_connection(sgbd, config)
        cursor = conn.cursor()

        for i in range(NUM_EXECUCOES):
            tempo_ms, io_reads, io_hits = None, None, None
            
            # --- PostgreSQL ---
            if sgbd == 'PostgreSQL':
                query_modificada = f"EXPLAIN (ANALYZE, BUFFERS) {sql}"
                start_time = time.time()
                cursor.execute(query_modificada)
                explain_output = "\n".join(str(row[0]) for row in cursor.fetchall())
                end_time = time.time()
                
                exec_time_match = re.search(r"Execution Time: ([\d.]+)", explain_output)
                tempo_ms = float(exec_time_match.group(1)) if exec_time_match else (end_time - start_time) * 1000

                io_reads = sum([int(val) for val in re.findall(r"shared read=(\d+)", explain_output)])
                io_hits = sum([int(val) for val in re.findall(r"shared hit=(\d+)", explain_output)])

            # --- SQL Server ---
            elif sgbd == 'SQL Server':
                cursor.execute("SET STATISTICS TIME ON;")
                start_time = time.time()
                cursor.execute(sql)
                _ = cursor.fetchall()
                end_time = time.time()
                tempo_ms = (end_time - start_time) * 1000
                cursor.execute("SET STATISTICS TIME OFF;")
                # Lembrete: I/O para SQL Server deve ser coletado manualmente via DBeaver/SSMS.

            # --- Snowflake ---
            elif sgbd == 'Snowflake':
                start_time = time.time()
                cursor.execute(sql)
                query_id_sf = cursor.sfqid
                _ = cursor.fetchall()
                end_time = time.time()
                
                tempo_ms = (end_time - start_time) * 1000
                
                # Espera o histórico do Snowflake ser atualizado
                time.sleep(5) 
                
                hist_query = f"SELECT BYTES_SCANNED FROM snowflake.account_usage.query_history WHERE QUERY_ID = '{query_id_sf}';"
                cursor.execute(hist_query)
                result = cursor.fetchone()
                if result:
                    io_reads = result[0] / (1024*1024) if result[0] is not None else 0 # Converte bytes para MB

            # Mostra resultado de cada execução
            if i == 0:
                print(f"  🔥 Execução de aquecimento: Tempo={tempo_ms:.2f} ms")
            else:
                print(f"  ✅ Execução {i}/{NUM_EXECUCOES-1}: Tempo={tempo_ms:.2f} ms, I/O Reads={io_reads}, I/O Hits={io_hits}")
                resultados_experimento.append({
                    'SGBD': sgbd,
                    'Modelo': modelo,
                    'Experimento': query_id,
                    'N_Execucao': i,
                    'Tempo_ms': tempo_ms,
                    'IO_Reads': io_reads,
                    'IO_Hits_Cache': io_hits
                })
    
    except Exception as e:
        print(f"ERRO: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            
    return resultados_experimento

# --- CONFIGURAÇÃO DO EXPERIMENTO ---
# Preencha as variáveis abaixo com os valores desejados

SGBD_ESCOLHIDO = "PostgreSQL"  # Opções: "PostgreSQL", "SQL Server", "Snowflake"
MODELO_ESCOLHIDO = "Desnormalizado"  # Opções: "Desnormalizado", "Normalizado"
EXPERIMENTO_ESCOLHIDO = "E1"  # Opções: "E1", "E2", "E3", "E4"

# --- EXECUÇÃO DO EXPERIMENTO ---

RESULTS_DIR = os.path.join(os.path.dirname(__file__), 'results')
os.makedirs(RESULTS_DIR, exist_ok=True)

if __name__ == "__main__":
    print(f"=== EXECUTANDO EXPERIMENTO ===")
    print(f"SGBD: {SGBD_ESCOLHIDO}")
    print(f"Modelo: {MODELO_ESCOLHIDO}")
    print(f"Experimento: {EXPERIMENTO_ESCOLHIDO}")
    print("=" * 40)
    
    # Executa o experimento
    resultados_finais = run_and_measure(SGBD_ESCOLHIDO, MODELO_ESCOLHIDO, EXPERIMENTO_ESCOLHIDO)
    
    if resultados_finais:
        df = pd.DataFrame(resultados_finais)
        
        # Salva o resultado em um arquivo CSV na pasta apropriada
        nome_arquivo = f"resultado_{SGBD_ESCOLHIDO}_{MODELO_ESCOLHIDO}_{EXPERIMENTO_ESCOLHIDO}.csv"
        caminho_arquivo = os.path.join(RESULTS_DIR, nome_arquivo)
        df.to_csv(caminho_arquivo, index=False)
        
        # Mostra estatísticas dos resultados
        print(f"\n=== RESULTADOS DO EXPERIMENTO ===")
        print(f"SGBD: {SGBD_ESCOLHIDO}")
        print(f"Modelo: {MODELO_ESCOLHIDO}")
        print(f"Experimento: {EXPERIMENTO_ESCOLHIDO}")
        print(f"Arquivo salvo: {caminho_arquivo}")
        
        if len(resultados_finais) > 0:
            tempos = [r['Tempo_ms'] for r in resultados_finais if r['Tempo_ms'] is not None]
            if tempos:
                print(f"\nEstatísticas de Tempo:")
                print(f"  • Média: {sum(tempos)/len(tempos):.2f} ms")
                print(f"  • Mínimo: {min(tempos):.2f} ms")
                print(f"  • Máximo: {max(tempos):.2f} ms")
                print(f"  • Total de execuções: {len(tempos)}")
        
        print(f"\nExperimento concluído!")
    else:
        print("Nenhum resultado foi gerado. Verifique as configurações.")