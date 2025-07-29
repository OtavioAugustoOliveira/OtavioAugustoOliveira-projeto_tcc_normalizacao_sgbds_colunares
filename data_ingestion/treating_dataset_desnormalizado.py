import geopandas as gpd
import pandas as pd
import os
import glob
import gc
import sys
import time  # <-- ADICIONADO: Importa o módulo para cronometragem

# --- MARCA O TEMPO DE INÍCIO ---
start_time = time.time()  # <-- ADICIONADO

print("--- INICIANDO SCRIPT: Geração do Cenário Desnormalizado (Versão Multi-Estado, em Lotes) ---")

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

    # --- CARREGANDO LOGRADOUROS (LÓGICA MULTI-ESTADO RESTAURADA) ---
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
    print(f"SUCESSO: {len(gdf_logradouros)} registros de logradouros carregados de {len(lista_arquivos_logradouros)} arquivos.")

except Exception as e:
    print(f"ERRO CRÍTICO ao carregar os shapefiles. Detalhe: {e}")
    exit()

def salvar_como_csv_com_wkt(gdf, colunas_desejadas, caminho_saida, modo='w', header=True):
    try:
        colunas_existentes = [col for col in colunas_desejadas if col in gdf.columns]
        df = gdf[colunas_existentes].copy()
        if 'geometry' in df.columns:
            df['geometry_wkt'] = df['geometry'].apply(lambda geom: geom.wkt if pd.notnull(geom) else None)
            df = df.drop(columns=['geometry'])
        df.to_csv(caminho_saida, index=False, mode=modo, header=header)
        print(f"  ✓ Lote salvo: {os.path.basename(caminho_saida)} (modo={modo})")
    except Exception as e:
        print(f"  ✗ ERRO ao salvar {os.path.basename(caminho_saida)}: {e}")

# --- 3. GERAÇÃO DO CENÁRIO DESNORMALIZADO ("Tabelão") EM LOTES ---
print("\n--- Gerando arquivo para o CENÁRIO DESNORMALIZADO em lotes ---")
try:
    print("  Iniciando junções espaciais em lotes para criar o 'tabelão'...")
    crs_padrao = gdf_logradouros.crs
    gdf_municipios = gdf_municipios.to_crs(crs_padrao)
    gdf_estados = gdf_estados.to_crs(crs_padrao)
    gdf_regioes = gdf_regioes.to_crs(crs_padrao)

    n_lotes = 20  # Ajuste conforme a memória disponível
    tamanho_lote = len(gdf_logradouros) // n_lotes + 1
    print(f"  Processando em {n_lotes} lotes de até {tamanho_lote} registros cada.")

    caminho_saida_final = os.path.join(pasta_saida, "desnormalizado_tabelao.csv")
    header_escrito = False

    for i in range(n_lotes):
        ini = i * tamanho_lote
        fim = min((i+1) * tamanho_lote, len(gdf_logradouros))
        print(f"    Lote {i+1}/{n_lotes}: registros {ini} até {fim-1}")
        gdf_lote = gdf_logradouros.iloc[ini:fim].copy()

        tabelao = gpd.sjoin(gdf_lote, gdf_municipios, how='inner', predicate='within')
        print('Colunas após join com municípios:', tabelao.columns)
        tabelao = tabelao.drop(columns=['index_right'], errors='ignore')
        tabelao = gpd.sjoin(tabelao, gdf_estados, how='inner', predicate='within')
        print('Colunas após join com estados:', tabelao.columns)
        tabelao = tabelao.drop(columns=['index_right', 'CD_UF'], errors='ignore')
        tabelao = gpd.sjoin(tabelao, gdf_regioes, how='inner', predicate='within')
        print('Colunas após join com regiões:', tabelao.columns)
        tabelao = tabelao.drop(columns=['index_right'], errors='ignore')

        colunas_finais = {
            'CD_FACE': 'id_face_logradouro',
            'NM_LOG': 'nome_logradouro',
            'geometry': 'geometry_wkt',
            'CD_MUN': 'id_municipio_logradouro',
            'NM_MUN': 'nome_municipio',
            'SIGLA_UF_right': 'sigla_estado',
            'NM_UF': 'nome_estado',
            'SIGLA_RG': 'sigla_regiao',
            'NM_REGIAO_right': 'nome_regiao'
        }
        # Garante que todas as colunas finais estejam presentes, mesmo que ausentes no DataFrame
        for k in colunas_finais:
            if k not in tabelao.columns:
                tabelao[k] = None
        tabelao_final = tabelao[list(colunas_finais.keys())].rename(columns=colunas_finais)

        # Converter geometry para WKT e substituir a coluna geometry_wkt
        if 'geometry_wkt' not in tabelao_final.columns and 'geometry' in tabelao_final.columns:
            tabelao_final['geometry_wkt'] = tabelao_final['geometry'].apply(lambda geom: geom.wkt if pd.notnull(geom) else None)
            tabelao_final = tabelao_final.drop(columns=['geometry'])

        salvar_como_csv_com_wkt(
            tabelao_final,
            list(tabelao_final.columns),
            caminho_saida_final,
            modo='a' if header_escrito else 'w',
            header=not header_escrito
        )
        header_escrito = True

        # Libera memória
        del gdf_lote, tabelao, tabelao_final
        gc.collect()

    print(f"  Todos os lotes processados e salvos em {caminho_saida_final}.")

except Exception as e:
    print(f"  ✗ ERRO CRÍTICO ao gerar o cenário desnormalizado: {e}")

# --- NOVA FUNÇÃO: Apenas transformar geometria_logradouro em geometry_wkt ---
def transformar_geometria_para_wkt(caminho_csv):
    print(f"Transformando geometria_logradouro em geometry_wkt no arquivo: {caminho_csv}")
    df = pd.read_csv(caminho_csv)
    if 'geometria_logradouro' not in df.columns:
        print("Coluna 'geometria_logradouro' não encontrada.")
        return
    df['geometry_wkt'] = df['geometria_logradouro']
    df = df.drop(columns=['geometria_logradouro'])
    novo_caminho = caminho_csv.replace('.csv', '_wkt.csv')
    df.to_csv(novo_caminho, index=False)
    print(f"Arquivo salvo: {novo_caminho}")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "apenas_wkt":
        caminho = os.path.join('data_ingestion', 'treated_dataset', 'desnormalizado_tabelao.csv')
        transformar_geometria_para_wkt(caminho)
        sys.exit(0)

# --- CALCULA E EXIBE O TEMPO TOTAL DE EXECUÇÃO ---
end_time = time.time()  # <-- ADICIONADO
total_time = end_time - start_time  # <-- ADICIONADO
# Formata o tempo para uma leitura mais fácil (minutos e segundos)
minutes, seconds = divmod(total_time, 60) # <-- ADICIONADO

print("\n--- Processo concluído! ---")
# <-- ADICIONADO: Exibe o tempo total formatado -->
print(f"Tempo total de execução: {int(minutes)} minutos e {seconds:.2f} segundos.")