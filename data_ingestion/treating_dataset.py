import geopandas as gpd
import pandas as pd
import numpy as np
import os

print("--- Iniciando o script de preparação de dados (ESQUEMA NORMALIZADO) ---")

# --- 1. CONFIGURAÇÃO INICIAL ---
caminho_shapefile = 'data_ingestion/raw_dataset/SP_setores_CD2022/SP_setores_CD2022.shp'  # Ajuste conforme o nome real do arquivo
pasta_saida = 'data_ingestion/treated_dataset'

# Verificação explícita do caminho e conteúdo da pasta
import os
print('Arquivo existe?', os.path.exists(caminho_shapefile))
pasta_shapefile = os.path.dirname(caminho_shapefile)
if os.path.exists(pasta_shapefile):
    print('Conteúdo da pasta:', os.listdir(pasta_shapefile))
else:
    print('A pasta do shapefile não existe:', pasta_shapefile)

os.makedirs(pasta_saida, exist_ok=True)
print(f"Pasta de saída '{pasta_saida}' garantida.")

try:
    gdf = gpd.read_file(caminho_shapefile)
    print(f"Shapefile '{caminho_shapefile}' lido com sucesso. Total de {len(gdf)} setores.")
except Exception as e:
    print(f"ERRO: Não foi possível ler o shapefile. Verifique o caminho: {e}")
    exit()

# --- 2. TRATAMENTO DE NULOS E AJUSTES INICIAIS ---
# Substituir strings vazias por NaN para facilitar merges
for col in gdf.columns:
    if gdf[col].dtype == object:
        gdf[col] = gdf[col].replace('', np.nan)

def criar_dimensao(df, col_cd, col_nm, nome_id, nome_csv):
    """Cria uma tabela de dimensão genérica."""
    dim = df[[col_cd, col_nm]].drop_duplicates().reset_index(drop=True)
    dim = dim[dim[col_cd].notnull()]
    dim[nome_id] = range(1, len(dim) + 1)
    dim = dim[[nome_id, col_cd, col_nm]]
    dim.to_csv(os.path.join(pasta_saida, nome_csv), index=False)
    return dim

# --- 0. GERAR TABELA NÃO NORMALIZADA DE REFERÊNCIA ---
print("\n--- Gerando tabela não normalizada de referência (cenario_nao_normalizado) ---")
colunas_nao_normalizado = [
    'CD_SETOR', 'NM_SITUACAO', 'CD_SIT', 'CD_TIPO', 'AREA_KM2', 'CD_REGIAO', 'NM_REGIAO',
    'CD_UF', 'NM_UF', 'CD_MUN', 'NM_MUN', 'CD_DIST', 'NM_DIST', 'CD_SUBDIST',
    'CD_BAIRRO', 'NM_BAIRRO', 'CD_RGINT', 'NM_RGINT', 'CD_RGI', 'NM_RGI', 'CD_CONCURB', 'NM_CONCURB', 'geometry'
]
df_nao_normalizado = gdf[colunas_nao_normalizado].copy()
df_nao_normalizado['geometry_wkt'] = df_nao_normalizado['geometry'].apply(lambda x: x.wkt if pd.notnull(x) else None)
df_nao_normalizado = df_nao_normalizado.drop(columns=['geometry'])
caminho_saida_nao_normalizado = os.path.join(pasta_saida, 'cenario_nao_normalizado.csv')
df_nao_normalizado.to_csv(caminho_saida_nao_normalizado, index=False)
print(f"SUCESSO: Arquivo salvo em '{caminho_saida_nao_normalizado}'")

# --- 3. CRIAÇÃO DAS TABELAS DE DIMENSÃO ---
print("\n--- Gerando tabelas de dimensão ---")

# 1. DIM_LOCALIZACAO
print("Gerando DIM_LOCALIZACAO...")
dim_localizacao_cols = [
    'CD_MUN', 'NM_MUN', 'CD_DIST', 'NM_DIST', 'CD_SUBDIST',
    'CD_RGI', 'NM_RGI', 'CD_RGINT', 'NM_RGINT', 'CD_UF', 'NM_UF', 'CD_REGIAO', 'NM_REGIAO'
]
dim_localizacao = gdf[dim_localizacao_cols].drop_duplicates().reset_index(drop=True)
dim_localizacao['ID_LOCALIZACAO'] = range(1, len(dim_localizacao) + 1)
dim_localizacao = dim_localizacao[['ID_LOCALIZACAO'] + dim_localizacao_cols]
dim_localizacao.to_csv(os.path.join(pasta_saida, 'DIM_LOCALIZACAO.csv'), index=False)

# 2. DIM_SITUACAO
print("Gerando DIM_SITUACAO...")
dim_situacao = gdf[['CD_SIT', 'NM_SITUACAO']].drop_duplicates().reset_index(drop=True)
dim_situacao = dim_situacao[dim_situacao['CD_SIT'].notnull()]
dim_situacao['ID_SITUACAO'] = range(1, len(dim_situacao) + 1)
dim_situacao = dim_situacao[['ID_SITUACAO', 'CD_SIT', 'NM_SITUACAO']]
dim_situacao.to_csv(os.path.join(pasta_saida, 'DIM_SITUACAO.csv'), index=False)

# 3. DIM_TIPO
print("Gerando DIM_TIPO...")
dim_tipo = gdf[['CD_TIPO']].drop_duplicates().reset_index(drop=True)
dim_tipo = dim_tipo[dim_tipo['CD_TIPO'].notnull()]
dim_tipo['NM_TIPO'] = np.nan  # Preencha manualmente se souber o significado
dim_tipo['ID_TIPO'] = range(1, len(dim_tipo) + 1)
dim_tipo = dim_tipo[['ID_TIPO', 'CD_TIPO', 'NM_TIPO']]
dim_tipo.to_csv(os.path.join(pasta_saida, 'DIM_TIPO.csv'), index=False)

# 4. DIM_BAIRRO
print("Gerando DIM_BAIRRO...")
dim_bairro = gdf[['CD_BAIRRO', 'NM_BAIRRO']].drop_duplicates().reset_index(drop=True)
dim_bairro = dim_bairro[dim_bairro['CD_BAIRRO'].notnull()]
dim_bairro['ID_BAIRRO'] = range(1, len(dim_bairro) + 1)
dim_bairro = dim_bairro[['ID_BAIRRO', 'CD_BAIRRO', 'NM_BAIRRO']]
dim_bairro.to_csv(os.path.join(pasta_saida, 'DIM_BAIRRO.csv'), index=False)

# 5. DIM_CONCURB
print("Gerando DIM_CONCURB...")
dim_concurb = gdf[['CD_CONCURB', 'NM_CONCURB']].drop_duplicates().reset_index(drop=True)
dim_concurb = dim_concurb[dim_concurb['CD_CONCURB'].notnull()]
dim_concurb['ID_CONCURB'] = range(1, len(dim_concurb) + 1)
dim_concurb = dim_concurb[['ID_CONCURB', 'CD_CONCURB', 'NM_CONCURB']]
dim_concurb.to_csv(os.path.join(pasta_saida, 'DIM_CONCURB.csv'), index=False)

# --- 4. CRIAÇÃO DA TABELA FATO ---
print("\n--- Gerando tabela FATO_SETORES_CENSITARIOS ---")

def get_fk(df_fato, dim, col_cd, id_col, fk_col):
    m = dim[[col_cd, id_col]].set_index(col_cd)
    return df_fato[col_cd].map(m[id_col])

# Monta a tabela fato
fato_cols = [
    'CD_SETOR', 'AREA_KM2', 'geometry',
    'CD_MUN', 'NM_MUN', 'CD_DIST', 'NM_DIST', 'CD_SUBDIST',
    'CD_RGI', 'NM_RGI', 'CD_RGINT', 'NM_RGINT', 'CD_UF', 'NM_UF', 'CD_REGIAO', 'NM_REGIAO',
    'CD_SIT', 'CD_TIPO', 'CD_BAIRRO', 'CD_CONCURB'
]
df_fato = gdf[fato_cols].copy()

# FK Localização
df_fato = df_fato.merge(dim_localizacao, on=[
    'CD_MUN', 'NM_MUN', 'CD_DIST', 'NM_DIST', 'CD_SUBDIST',
    'CD_RGI', 'NM_RGI', 'CD_RGINT', 'NM_RGINT', 'CD_UF', 'NM_UF', 'CD_REGIAO', 'NM_REGIAO'
], how='left', suffixes=('', '_dim'))
df_fato['ID_LOCALIZACAO_FK'] = df_fato['ID_LOCALIZACAO']
df_fato = df_fato.drop(columns=['ID_LOCALIZACAO'])

# Outras FKs
df_fato['ID_SITUACAO_FK'] = get_fk(df_fato, dim_situacao, 'CD_SIT', 'ID_SITUACAO', 'ID_SITUACAO_FK')
df_fato['ID_TIPO_FK'] = get_fk(df_fato, dim_tipo, 'CD_TIPO', 'ID_TIPO', 'ID_TIPO_FK')
df_fato['ID_BAIRRO_FK'] = get_fk(df_fato, dim_bairro, 'CD_BAIRRO', 'ID_BAIRRO', 'ID_BAIRRO_FK')
df_fato['ID_CONCURB_FK'] = get_fk(df_fato, dim_concurb, 'CD_CONCURB', 'ID_CONCURB', 'ID_CONCURB_FK')

# Seleciona colunas finais da fato
fato_final_cols = [
    'CD_SETOR', 'AREA_KM2', 'geometry',
    'ID_LOCALIZACAO_FK', 'ID_SITUACAO_FK', 'ID_TIPO_FK', 'ID_BAIRRO_FK',
    'ID_CONCURB_FK'
]
df_fato_final = df_fato[fato_final_cols].copy()
df_fato_final['ID_FATO_SETOR'] = range(1, len(df_fato_final) + 1)

# Reordena colunas
fato_final_cols = ['ID_FATO_SETOR'] + fato_final_cols

# Converte geometria para WKT
if 'geometry' in df_fato_final.columns:
    df_fato_final['geometry_wkt'] = df_fato_final['geometry'].apply(lambda x: x.wkt if pd.notnull(x) else None)
    df_fato_final = df_fato_final.drop(columns=['geometry'])
    # Atualiza a lista de colunas finais
    fato_final_cols = [col for col in fato_final_cols if col != 'geometry'] + ['geometry_wkt']

# Salva CSV
df_fato_final[fato_final_cols].to_csv(os.path.join(pasta_saida, 'FATO_SETORES_CENSITARIOS.csv'), index=False)
print(f"SUCESSO: Arquivo FATO_SETORES_CENSITARIOS.csv salvo em '{pasta_saida}'")

print("\n--- Processo de preparação de dados concluído! ---")