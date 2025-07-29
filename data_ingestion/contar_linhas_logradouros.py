import geopandas as gpd
import glob
import os

# Caminho base dos shapefiles de logradouros
pasta_base_logradouros = os.path.join('data_ingestion', 'raw_dataset', 'lougradores')
padrao_shp_logradouros = os.path.join(pasta_base_logradouros, '**', '*.shp')

# Encontra todos os arquivos .shp de logradouros
lista_arquivos_logradouros = glob.glob(padrao_shp_logradouros, recursive=True)

if not lista_arquivos_logradouros:
    print('Nenhum arquivo de logradouro (.shp) encontrado.')
    exit(1)

total_linhas = 0
for arquivo in lista_arquivos_logradouros:
    try:
        gdf = gpd.read_file(arquivo)
        n = len(gdf)
        print(f"Arquivo: {arquivo} - {n} linhas")
        total_linhas += n
    except Exception as e:
        print(f"Erro ao ler {arquivo}: {e}")

print(f"\nQuantidade total de linhas (feições) em todos os shapefiles de logradouros: {total_linhas}") 