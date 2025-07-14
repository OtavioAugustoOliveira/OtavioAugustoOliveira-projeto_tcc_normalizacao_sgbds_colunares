import pandas as pd
from sqlalchemy import create_engine
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import urllib
import os
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
imprimir_configuracoes()



# --- CAMINHOS DOS ARQUIVOS ---
# -----------------------------------------------------------------
PASTA_DADOS = os.path.join('data_ingestion', 'treated_dataset')

ARQUIVOS_PARA_CARGA = {
    'cenario_nao_normalizado': os.path.join(PASTA_DADOS, 'cenario_nao_normalizado.csv'),
    'dim_localizacao': os.path.join(PASTA_DADOS, 'DIM_LOCALIZACAO.csv'),
    'dim_situacao': os.path.join(PASTA_DADOS, 'DIM_SITUACAO.csv'),
    'dim_tipo': os.path.join(PASTA_DADOS, 'DIM_TIPO.csv'),
    'dim_bairro': os.path.join(PASTA_DADOS, 'DIM_BAIRRO.csv'),
    'dim_concurb': os.path.join(PASTA_DADOS, 'DIM_CONCURB.csv'),
    'dim_nu': os.path.join(PASTA_DADOS, 'DIM_NU.csv'),
    'dim_fcu': os.path.join(PASTA_DADOS, 'DIM_FCU.csv'),
    'dim_aglom': os.path.join(PASTA_DADOS, 'DIM_AGLOM.csv'),
    'fato_setores_censitarios': os.path.join(PASTA_DADOS, 'FATO_SETORES_CENSITARIOS.csv')
}

# Ordem de carga: cenario_nao_normalizado, dimensões, fato
ORDEM_CARGA = [
    'cenario_nao_normalizado'#,
    #'dim_localizacao',
    #'dim_situacao',
    #'dim_tipo',
    #'dim_bairro',
    #'dim_concurb',
    #'fato_setores_censitarios'
]

# Dicionário de colunas esperadas para cada tabela
COLUNAS_ESPERADAS = {
    'cenario_nao_normalizado': [
        'cd_setor', 'nm_situacao', 'cd_sit', 'cd_tipo', 'area_km2', 'cd_regiao', 'nm_regiao',
        'cd_uf', 'nm_uf', 'cd_mun', 'nm_mun', 'cd_dist', 'nm_dist', 'cd_subdist',
        'cd_bairro', 'nm_bairro', 'cd_rgint', 'nm_rgint', 'cd_rgi', 'nm_rgi', 'cd_concurb', 'nm_concurb', 'geometry_wkt'
    ],
    'dim_localizacao': [
        'id_localizacao', 'cd_mun', 'nm_mun', 'cd_dist', 'nm_dist', 'cd_subdist',
        'cd_rgi', 'nm_rgi', 'cd_rgint', 'nm_rgint', 'cd_uf', 'nm_uf', 'cd_regiao', 'nm_regiao'
    ],
    'dim_situacao': ['id_situacao', 'cd_sit', 'nm_situacao'],
    'dim_tipo': ['id_tipo', 'cd_tipo', 'nm_tipo'],
    'dim_bairro': ['id_bairro', 'cd_bairro', 'nm_bairro'],
    'dim_concurb': ['id_concurb', 'cd_concurb', 'nm_concurb'],
    'fato_setores_censitarios': [
        'id_fato_setor', 'cd_setor', 'area_km2', 'geometry_wkt',
        'id_localizacao_fk', 'id_situacao_fk', 'id_tipo_fk', 'id_bairro_fk',
        'id_concurb_fk'
    ]
}

# --- FUNÇÕES DE CARGA (ATUALIZADAS) ---
# -----------------------------------------------------------------

def carregar_para_postgres():
    print("\n--- INICIANDO CARGA PARA POSTGRESQL ---")
    try:
        connection_string = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DBNAME}"
        engine = create_engine(connection_string)
        
        for nome_tabela in ORDEM_CARGA:
            caminho_csv = ARQUIVOS_PARA_CARGA[nome_tabela]
            print(f"\nProcessando {caminho_csv} para a tabela {nome_tabela}...")
            df = pd.read_csv(caminho_csv)
            df.columns = [col.lower() for col in df.columns]
            # Ajusta o DataFrame para conter apenas as colunas esperadas
            if nome_tabela in COLUNAS_ESPERADAS:
                cols = [col for col in COLUNAS_ESPERADAS[nome_tabela] if col in df.columns]
                df = df[cols]
            print(f"Colunas do CSV para {nome_tabela}:", df.columns.tolist())
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

        for nome_tabela in ORDEM_CARGA:
            caminho_csv = ARQUIVOS_PARA_CARGA[nome_tabela]
            print(f"\nProcessando {caminho_csv} para a tabela {nome_tabela}...")
            df = pd.read_csv(caminho_csv)
            df.columns = [col.lower() for col in df.columns]
            # Remover colunas de IDENTITY antes de inserir (exceto FKs na tabela fato)
            if nome_tabela in [
                'dim_localizacao', 'dim_situacao', 'dim_tipo', 'dim_bairro',
                'dim_concurb', 'dim_nu', 'dim_fcu', 'dim_aglom'
            ]:
                id_col = [col for col in df.columns if col.startswith('id_')]
                for col in id_col:
                    if col in df.columns:
                        df = df.drop(columns=[col])
            elif nome_tabela == 'fato_setores_censitarios':
                # Para tabela fato, só remove id_fato_setor (IDENTITY), mantém as FKs
                if 'id_fato_setor' in df.columns:
                    df = df.drop(columns=['id_fato_setor'])
            # Remover coluna geometry antes de inserir nas tabelas espaciais
            if nome_tabela in ['fato_setores_censitarios', 'cenario_nao_normalizado']:
                if 'geometry' in df.columns:
                    df = df.drop(columns=['geometry'])
            # Ajusta para as colunas esperadas
            if nome_tabela == 'fato_setores_censitarios':
                # Para SQL Server, envie cd_setor, area_km2, geometry_wkt e as FKs
                cols = [col for col in ['cd_setor', 'area_km2', 'geometry_wkt', 'id_localizacao_fk', 'id_situacao_fk', 'id_tipo_fk', 'id_bairro_fk', 'id_concurb_fk'] if col in df.columns]
                df = df[cols]
            elif nome_tabela in COLUNAS_ESPERADAS:
                cols = [col for col in COLUNAS_ESPERADAS[nome_tabela] if col in df.columns]
                df = df[cols]
            print(f"Colunas do CSV para {nome_tabela}:", df.columns.tolist())
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

        for nome_tabela in ORDEM_CARGA:
            caminho_csv = ARQUIVOS_PARA_CARGA[nome_tabela]
            print(f"\nProcessando {caminho_csv} para a tabela {nome_tabela}...")
            df = pd.read_csv(caminho_csv)
            # Para Snowflake, converter colunas para minúsculo (padrão do Snowflake)
            df.columns = [col.lower() for col in df.columns]
            print(f"Colunas do CSV para {nome_tabela}:", df.columns.tolist())
            
            # Para Snowflake, usar nome da tabela em maiúsculo
            nome_tabela_snowflake = nome_tabela.upper()
            
            success, nchunks, nrows, _ = write_pandas(
                conn,
                df,
                table_name=nome_tabela_snowflake,
                auto_create_table=False,
                overwrite=False,
                quote_identifiers=False
            )
            if success:
                print(f"SUCESSO: {nrows} linhas inseridas em {nome_tabela_snowflake}.")
            else:
                print(f"!!! FALHA ao carregar para a tabela {nome_tabela_snowflake}.")

    except Exception as e:
        print(f"!!! ERRO no Snowflake: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            print("Conexão com Snowflake fechada.")

# --- EXECUÇÃO ---
# -----------------------------------------------------------------
if __name__ == "__main__":
    carregar_para_postgres()
    #carregar_para_sql_server()
    #carregar_para_snowflake()
    print("\n--- Script de carga concluído. ---")

