import pandas as pd
import os

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
    caminho = os.path.join('data_ingestion', 'treated_dataset', 'desnormalizado_tabelao.csv')
    transformar_geometria_para_wkt(caminho) 