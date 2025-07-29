import geopandas as gpd
import pandas as pd
import os
import glob
import time
import gc # <-- ADICIONADO: Essencial para o gerenciamento de memória

start_time = time.time()

print("--- INICIANDO SCRIPT: Geração dos Cenários (VERSÃO OTIMIZADA PARA MEMÓRIA) ---")

# --- 1. CONFIGURAÇÃO ---
pasta_raw = 'data_ingestion/raw_dataset'
pasta_saida = 'data_ingestion/treated_dataset'
os.makedirs(pasta_saida, exist_ok=True)
print(f"Pasta de saída '{pasta_saida}' garantida.")

# --- 2. CARREGAMENTO DOS DADOS GEOGRÁFICOS ---
try:
    print("\n--- Carregando Shapefiles (Municípios, Regiões, Estados) ---")
    caminho_municipios = os.path.join(pasta_raw, "BR_Municipios_2022", "BR_Municipios_2022.shp")
    gdf_municipios = gpd.read_file(caminho_municipios)

    caminho_regioes = os.path.join(pasta_raw, "BR_Regioes_2022", "BR_Regioes_2022.shp")
    gdf_regioes = gpd.read_file(caminho_regioes)

    caminho_estados = os.path.join(pasta_raw, "BR_UF_2022", "BR_UF_2022.shp")
    gdf_estados = gpd.read_file(caminho_estados)
    print("SUCESSO: Shapefiles de Municípios, Regiões e Estados carregados.")

    print("\n--- Carregando Shapefiles de Logradouros (todos os estados encontrados) ---")
    pasta_base_logradouros = os.path.join(pasta_raw, "lougradores")
    padrao_shp_logradouros = os.path.join(pasta_base_logradouros, "**", "*.shp")
    lista_arquivos_logradouros = glob.glob(padrao_shp_logradouros, recursive=True)

    if not lista_arquivos_logradouros:
        raise FileNotFoundError("Nenhum arquivo de logradouro (.shp) encontrado.")

    lista_gdfs_logradouros = []
    for arquivo in lista_arquivos_logradouros:
        print(f"  Lendo: {arquivo}")
        gdf_temp = gpd.read_file(arquivo)
        lista_gdfs_logradouros.append(gdf_temp)

    gdf_logradouros = pd.concat(lista_gdfs_logradouros, ignore_index=True)
    del lista_gdfs_logradouros # Libera memória da lista de dataframes
    gc.collect()
    print(f"SUCESSO: {len(gdf_logradouros)} registros de logradouros carregados de {len(lista_arquivos_logradouros)} arquivos.")

except Exception as e:
    print(f"ERRO CRÍTICO ao carregar os shapefiles. Detalhe: {e}")
    exit()

# --- 3. GERAÇÃO DO CENÁRIO NORMALIZADO ---
print("\n--- PARTE 1: Gerando arquivos para o CENÁRIO NORMALIZADO ---")

# ▼▼▼ FUNÇÃO MODIFICADA PARA ACEITAR MODO DE ESCRITA E CABEÇALHO ▼▼▼
def salvar_como_csv_com_wkt(gdf, colunas_desejadas, caminho_saida, mode='w', header=True):
    try:
        colunas_existentes = [col for col in colunas_desejadas if col in gdf.columns]
        df = gdf[colunas_existentes].copy()
        df['geometry_wkt'] = df['geometry'].apply(lambda geom: geom.wkt if pd.notnull(geom) else None)
        df = df.drop(columns=['geometry'])
        df.to_csv(caminho_saida, index=False, mode=mode, header=header)
        print(f"  ✓ Lote salvo em: {os.path.basename(caminho_saida)} (modo={mode}, header={header})")
    except Exception as e:
        print(f"  ✗ ERRO ao salvar {os.path.basename(caminho_saida)}: {e}")

# ▼▼▼ PROCESSAMENTO OTIMIZADO ▼▼▼

# 1. Salva os arquivos pequenos de uma vez, pois não causam problemas de memória.
print("\nSalvando arquivos geográficos menores (municípios, regiões, estados)...")
salvar_como_csv_com_wkt(gdf_municipios, ['CD_MUN', 'NM_MUN', 'SIGLA_UF', 'AREA_KM2', 'geometry'], os.path.join(pasta_saida, "normalizado_municipios.csv"))
salvar_como_csv_com_wkt(gdf_regioes, ['NM_REGIAO', 'SIGLA_RG', 'geometry'], os.path.join(pasta_saida, "normalizado_regioes.csv"))
salvar_como_csv_com_wkt(gdf_estados, ['CD_UF', 'NM_UF', 'SIGLA_UF', 'geometry'], os.path.join(pasta_saida, "normalizado_estados.csv"))

# 2. Libera a memória dos DataFrames que não serão mais usados.
del gdf_municipios, gdf_regioes, gdf_estados
gc.collect()
print("Memória dos arquivos menores liberada.")

# 3. Processa o arquivo gigante de LOGRADOUROS em lotes.
print("\nProcessando e salvando arquivo de logradouros em lotes para economizar memória...")
caminho_saida_logradouros = os.path.join(pasta_saida, "normalizado_logradouros.csv")
n_lotes = 20  # Ajuste conforme a memória disponível (pode ser 20, 50, 100...)
tamanho_lote = len(gdf_logradouros) // n_lotes + 1
header_escrito = False

for i in range(n_lotes):
    ini = i * tamanho_lote
    fim = min((i + 1) * tamanho_lote, len(gdf_logradouros))

    if ini >= len(gdf_logradouros):
        break # Encerra o loop se não houver mais dados a processar

    print(f"  Processando lote {i+1}/{n_lotes} de logradouros (registros {ini} a {fim-1})...")

    # Cria o lote (uma fatia do DataFrame original)
    gdf_lote = gdf_logradouros.iloc[ini:fim]

    # Salva o lote, usando 'w' para o primeiro e 'a' para os seguintes
    salvar_como_csv_com_wkt(
        gdf_lote,
        ['CD_FACE', 'NM_LOG', 'CD_MUN', 'geometry'],
        caminho_saida_logradouros,
        mode='a' if header_escrito else 'w',
        header=not header_escrito
    )
    header_escrito = True

    # Libera a memória usada por este lote
    del gdf_lote
    gc.collect()

print("Arquivo de logradouros salvo com sucesso.")

end_time = time.time()
total_time = end_time - start_time
minutes, seconds = divmod(total_time, 60)

print("\n--- Processo concluído! ---")
print(f"Tempo total de execução: {int(minutes)} minutos e {seconds:.2f} segundos.")