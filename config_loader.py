import json
import os

def obter_configuracoes():
    caminho_config = os.path.join(os.path.dirname(__file__), 'environment_keys.json')
    with open(caminho_config, 'r') as f:
        config = json.load(f)
    return config

def imprimir_configuracoes():
    config = obter_configuracoes()
    print("\n--- CONFIGURAÇÕES CARREGADAS ---")
    print(f"SNOW_USER: {config.get('SNOW_USER')}")
    print(f"SNOW_ACCOUNT: {config.get('SNOW_ACCOUNT')}")
    print(f"SNOW_WAREHOUSE: {config.get('SNOW_WAREHOUSE')}")
    print(f"SNOW_DATABASE: {config.get('SNOW_DATABASE')}")
    print(f"SNOW_SCHEMA: {config.get('SNOW_SCHEMA')}")
    print("-------------------------------\n") 