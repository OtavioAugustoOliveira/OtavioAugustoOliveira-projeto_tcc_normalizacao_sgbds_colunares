import json
import os

def carregar_configuracoes(arquivo_config='data_ingestion_environment_keys.json'):
    """Carrega as configurações do arquivo JSON"""
    try:
        with open(arquivo_config, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"ERRO: Arquivo de configuração {arquivo_config} não encontrado!")
        return {}
    except Exception as e:
        print(f"ERRO ao ler arquivo de configuração: {e}")
        return {}

def obter_configuracoes():
    """Retorna todas as configurações de banco de dados"""
    config = carregar_configuracoes()
    
    # PostgreSQL
    pg_config = config.get('postgresql', {})
    PG_USER = pg_config.get('user', '')
    PG_PASSWORD = pg_config.get('password', '')
    PG_HOST = pg_config.get('host', '')
    PG_PORT = pg_config.get('port', '')
    PG_DBNAME = pg_config.get('dbname', '')

    # SQL Server
    sql_config = config.get('sql_server', {})
    SQL_SERVER_NAME = sql_config.get('server_name', '')
    SQL_DBNAME = sql_config.get('dbname', '')
    SQL_USER = sql_config.get('user', '')
    SQL_PASSWORD = sql_config.get('password', '')

    # Snowflake
    snow_config = config.get('snowflake', {})
    SNOW_USER = snow_config.get('user', '')
    SNOW_PASSWORD = snow_config.get('password', '')
    SNOW_ACCOUNT = snow_config.get('account', '')
    SNOW_WAREHOUSE = snow_config.get('warehouse', '')
    SNOW_DATABASE = snow_config.get('database', '')
    SNOW_SCHEMA = snow_config.get('schema', '')

    return {
        'PG_USER': PG_USER,
        'PG_PASSWORD': PG_PASSWORD,
        'PG_HOST': PG_HOST,
        'PG_PORT': PG_PORT,
        'PG_DBNAME': PG_DBNAME,
        'SQL_SERVER_NAME': SQL_SERVER_NAME,
        'SQL_DBNAME': SQL_DBNAME,
        'SQL_USER': SQL_USER,
        'SQL_PASSWORD': SQL_PASSWORD,
        'SNOW_USER': SNOW_USER,
        'SNOW_PASSWORD': SNOW_PASSWORD,
        'SNOW_ACCOUNT': SNOW_ACCOUNT,
        'SNOW_WAREHOUSE': SNOW_WAREHOUSE,
        'SNOW_DATABASE': SNOW_DATABASE,
        'SNOW_SCHEMA': SNOW_SCHEMA
    }

def imprimir_configuracoes():
    """Imprime as configurações carregadas"""
    config = obter_configuracoes()
    print("=== CONFIGURAÇÕES CARREGADAS ===")
    print(f"PostgreSQL: {config['PG_HOST']}:{config['PG_PORT']}/{config['PG_DBNAME']}")
    print(f"SQL Server: {config['SQL_SERVER_NAME']}/{config['SQL_DBNAME']}")
    print(f"Snowflake: {config['SNOW_ACCOUNT']}/{config['SNOW_DATABASE']}/{config['SNOW_SCHEMA']}")
    print("=" * 40) 