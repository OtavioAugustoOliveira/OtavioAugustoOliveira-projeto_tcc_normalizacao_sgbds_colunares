import pandas as pd
from sqlalchemy import create_engine
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import urllib
import os
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
imprimir_configuracoes()



# --- CAMINHOS DOS ARQUIVOS ---
# -----------------------------------------------------------------
PASTA_DADOS = os.path.join('..', 'data_ingestion', 'treated_dataset')

ARQUIVOS_PARA_CARGA = {
    'cenario_nao_normalizado': os.path.join(PASTA_DADOS, 'cenario_nao_normalizado.csv'),
    'dim_regioes': os.path.join(PASTA_DADOS, 'dim_regioes.csv'),
    'dim_estados': os.path.join(PASTA_DADOS, 'dim_estados.csv'),
    'fato_municipios': os.path.join(PASTA_DADOS, 'fato_municipios.csv')
}

# --- FUNÇÕES DE CARGA (SIMPLIFICADAS) ---
# -----------------------------------------------------------------

def carregar_para_postgres():
    print("\n--- INICIANDO CARGA PARA POSTGRESQL ---")
    try:
        connection_string = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DBNAME}"
        engine = create_engine(connection_string)
        
        for nome_tabela, caminho_csv in ARQUIVOS_PARA_CARGA.items():
            print(f"\nProcessando {caminho_csv} para a tabela {nome_tabela}...")
            df = pd.read_csv(caminho_csv)
            
            # Adiciona a coluna de tempo para as tabelas que serão hypertables
            if nome_tabela in ['fato_municipios', 'cenario_nao_normalizado']:
                # Garante que a coluna não seja adicionada se já existir
                if 'ts_updated' not in df.columns:
                    df['ts_updated'] = pd.Timestamp.now('UTC')

            # Carga direta, assumindo que as colunas correspondem
            df.to_sql(nome_tabela, engine, if_exists='append', index=False, method='multi', chunksize=1000)
            print(f"SUCESSO: {len(df)} linhas inseridas em {nome_tabela}.")

    except Exception as e:
        print(f"!!! ERRO no PostgreSQL: {e}")

def carregar_para_sql_server():
    print("\n--- INICIANDO CARGA PARA SQL SERVER ---")
    try:
        params = urllib.parse.quote_plus(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={SQL_SERVER_NAME};"
            f"DATABASE={SQL_DBNAME};"
            f"UID={SQL_USER};"
            f"PWD={SQL_PASSWORD};"
        )
        connection_string = f"mssql+pyodbc:///?odbc_connect={params}"
        engine = create_engine(connection_string, fast_executemany=True)

        for nome_tabela, caminho_csv in ARQUIVOS_PARA_CARGA.items():
            print(f"\nProcessando {caminho_csv} para a tabela {nome_tabela}...")
            df = pd.read_csv(caminho_csv)

            # Carga direta, assumindo que as colunas correspondem
            df.to_sql(nome_tabela, engine, if_exists='append', index=False, chunksize=1000)
            print(f"SUCESSO: {len(df)} linhas inseridas em {nome_tabela}.")
            
    except Exception as e:
        print(f"!!! ERRO no SQL Server: {e}")

def carregar_para_snowflake():
    print("\n--- INICIANDO CARGA PARA SNOWFLAKE ---")
    try:
        conn = snowflake.connector.connect(
            user=SNOW_USER,
            password=SNOW_PASSWORD,
            account=SNOW_ACCOUNT,
            warehouse=SNOW_WAREHOUSE,
            database=SNOW_DATABASE,
            schema=SNOW_SCHEMA
        )
        print("Conexão com Snowflake estabelecida.")

        for nome_tabela, caminho_csv in ARQUIVOS_PARA_CARGA.items():
            print(f"\nProcessando {caminho_csv} para a tabela {nome_tabela}...")
            df = pd.read_csv(caminho_csv)

            # MANTER colunas em minúsculo (como estão no CSV e no Snowflake)
            # df.columns = df.columns  # Desnecessário, mantemos como está

            # Usar nome da tabela exatamente como definido no Snowflake (case-sensitive)
            success, nchunks, nrows, _ = write_pandas(
                conn,
                df,
                table_name=nome_tabela,
                auto_create_table=True,
                overwrite=False,
                quote_identifiers=True  # Garante o uso de aspas duplas para preservar o case
            )

            if success:
                print(f"SUCESSO: {nrows} linhas inseridas em {nome_tabela}.")
            else:
                print(f"!!! FALHA ao carregar para a tabela {nome_tabela}.")

    except Exception as e:
        print(f"!!! ERRO no Snowflake: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            print("Conexão com Snowflake fechada.")


# --- EXECUÇÃO ---
# -----------------------------------------------------------------
# Descomente as linhas abaixo para os bancos que você quer carregar

if __name__ == "__main__":
    #carregar_para_postgres()
    #carregar_para_sql_server()
    carregar_para_snowflake()
    
    print("\n--- Script de carga concluído. ---")

