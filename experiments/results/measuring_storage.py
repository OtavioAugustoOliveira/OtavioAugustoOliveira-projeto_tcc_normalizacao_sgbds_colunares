import pandas as pd
import re
import sys
import os
import psycopg2
import pyodbc
import snowflake.connector

# Adiciona o diretório raiz ao path para importar config_loader
try:
    sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
    from config_loader import obter_configuracoes, imprimir_configuracoes
except ImportError:
    print("ERRO: Não foi possível encontrar 'config_loader'. Certifique-se de que o script está na estrutura de pastas correta.")
    sys.exit(1)

# --- CONFIGURAÇÃO GLOBAL ---
config = obter_configuracoes()

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

# --- CONFIGURAÇÕES DE CONEXÃO ---
CONEXOES = {
    'PostgreSQL': { 'host': PG_HOST, 'port': PG_PORT, 'database': PG_DBNAME, 'user': PG_USER, 'password': PG_PASSWORD },
    'SQL Server': { 'server': SQL_SERVER_NAME, 'database': SQL_DBNAME, 'user': SQL_USER, 'password': SQL_PASSWORD, 'driver': '{ODBC Driver 17 for SQL Server}' },
    'Snowflake': { 'user': SNOW_USER, 'password': SNOW_PASSWORD, 'account': SNOW_ACCOUNT, 'warehouse': SNOW_WAREHOUSE, 'database': SNOW_DATABASE, 'schema': SNOW_SCHEMA }
}

def get_connection(sgbd_name, config):
    """Cria e retorna uma conexão com o banco de dados apropriado."""
    conn = None
    if sgbd_name == 'PostgreSQL':
        conn = psycopg2.connect(**config)
    elif sgbd_name == 'SQL Server':
        conn_str = f"DRIVER={config['driver']};SERVER={config['server']};DATABASE={config['database']};UID={config['user']};PWD={config['password']};"
        conn = pyodbc.connect(conn_str)
    elif sgbd_name == 'Snowflake':
        conn = snowflake.connector.connect(**config)
    return conn

def get_storage_metrics(sgbd, tabelas):
    """Coleta métricas de armazenamento para as tabelas especificadas e retorna o tamanho total em MB."""
    config = CONEXOES[sgbd]
    tamanho_total_mb = 0
    conn = None
    try:
        conn = get_connection(sgbd, config)
        cursor = conn.cursor()
        total_bytes = 0
        if sgbd == 'PostgreSQL':
            for tabela in tabelas:
                cursor.execute(f"SELECT pg_total_relation_size('{tabela.lower()}');")
                size_bytes = cursor.fetchone()[0]
                if size_bytes: total_bytes += size_bytes
        elif sgbd == 'SQL Server':
            total_kb = 0
            for tabela in tabelas:
                cursor.execute(f"EXEC sp_spaceused '{tabela}';")
                row = cursor.fetchone()
                data_kb = int(re.search(r'(\d+)', row.data).group(1))
                index_kb = int(re.search(r'(\d+)', row.index_size).group(1))
                total_kb += data_kb + index_kb
            total_bytes = total_kb * 1024
        elif sgbd == 'Snowflake':
            for tabela in tabelas:
                db, schema = config['database'], config['schema']
                cursor.execute(f"SELECT BYTES FROM {db}.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{schema.upper()}' AND TABLE_NAME = '{tabela.upper()}'")
                size_bytes = cursor.fetchone()
                if size_bytes and size_bytes[0]: total_bytes += size_bytes[0]
        tamanho_total_mb = total_bytes / (1024 * 1024) if total_bytes > 0 else 0
    except Exception as e:
        print(f"❌ ERRO ao coletar armazenamento para {sgbd} em '{tabelas}': {e}")
        return None
    finally:
        if conn: conn.close()
    return tamanho_total_mb

if __name__ == "__main__":
    print("=" * 60)
    print("|| 📏 MEDIDOR DE ARMAZENAMENTO DOS CENÁRIOS 📏 ||")
    print("=" * 60)

    SGDB_LIST = ["PostgreSQL", "SQL Server", "Snowflake"]
    MODELO_LIST = ["Desnormalizado", "Normalizado"]
    
    tabelas_cenarios = {
        'Desnormalizado': ['cenario_nao_normalizado'],
        'Normalizado': ['fato_setores_censitarios', 'dim_localizacao', 'dim_situacao']
    }
    
    resultados_finais = []

    for sgbd in SGDB_LIST:
        for modelo in MODELO_LIST:
            print(f"🔎 Medindo armazenamento para: {sgbd} | {modelo}...")
            
            tabelas_do_modelo = tabelas_cenarios[modelo]
            tamanho_mb = get_storage_metrics(sgbd, tabelas_do_modelo)
            
            if tamanho_mb is not None:
                print(f"   ✅ Resultado: {tamanho_mb:.3f} MB")
                resultados_finais.append({
                    'SGBD': sgbd,
                    'Modelo': modelo,
                    'Armazenamento_MB': tamanho_mb
                })
            else:
                print(f"   ❌ Falha ao medir.")

    # --- Salva o