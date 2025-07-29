import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import os
import sys

# Adiciona o diretório raiz ao path para importar config_loader
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from config_loader import obter_configuracoes, imprimir_configuracoes

# --- CONFIGURAÇÃO GLOBAL ---
# Carrega as configurações do arquivo JSON via config_loader
config = obter_configuracoes()

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
    'normalizado_municipios': os.path.join(PASTA_DADOS, 'normalizado_municipios.csv'),
    'normalizado_estados': os.path.join(PASTA_DADOS, 'normalizado_estados.csv'),
    'normalizado_regioes': os.path.join(PASTA_DADOS, 'normalizado_regioes.csv'),
    'normalizado_logradouros': os.path.join(PASTA_DADOS, 'normalizado_logradouros.csv'),
    'desnormalizado_tabelao': os.path.join(PASTA_DADOS, 'desnormalizado_tabelao.csv')
}

ORDEM_CARGA = [
    #'normalizado_municipios',
    #'normalizado_estados',
    #'normalizado_logradouros',
    'desnormalizado_tabelao'
]

COLUNAS_ESPERADAS = {
    'normalizado_municipios': [
        'cd_mun', 'nm_mun', 'sigla_uf', 'area_km2', 'geometry_wkt'
    ],
    'normalizado_estados': [
        'cd_uf', 'nm_uf', 'sigla_uf', 'geometry_wkt'
    ],
    'normalizado_regioes': [
        'nm_regiao', 'sigla_rg', 'geometry_wkt'
    ],
    'normalizado_logradouros': [
        'cd_face', 'nm_log', 'cd_mun', 'geometry_wkt'
    ],
    'desnormalizado_tabelao': [
        'id_face_logradouro', 'nome_logradouro', 'geometry_wkt', 'id_municipio_logradouro', 'nome_municipio', 'sigla_estado', 'nome_estado', 'sigla_regiao', 'nome_regiao', 'area_km2'
    ]
}

# --- FUNÇÃO DE CARGA PARA SNOWFLAKE ---
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
            chunk_size = 100000
            for chunk in pd.read_csv(caminho_csv, chunksize=chunk_size):
                chunk.columns = [col.lower() for col in chunk.columns]
                print(f"Colunas do CSV para {nome_tabela}:", chunk.columns.tolist())
                nome_tabela_snowflake = nome_tabela.upper()
                success, nchunks, nrows, _ = write_pandas(
                    conn,
                    chunk,
                    table_name=nome_tabela_snowflake,
                    auto_create_table=False,
                    overwrite=False,
                    quote_identifiers=False
                )
                if not success:
                    print(f"!!! FALHA ao carregar para a tabela {nome_tabela_snowflake}.")
            print(f"SUCESSO: Dados inseridos em {nome_tabela_snowflake}.") 
    except Exception as e:
        print(f"!!! ERRO no Snowflake: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            print("Conexão com Snowflake fechada.")

# --- EXECUÇÃO ---
# -----------------------------------------------------------------
if __name__ == "__main__":
    carregar_para_snowflake()
    print("\n--- Script de carga concluído. ---")

