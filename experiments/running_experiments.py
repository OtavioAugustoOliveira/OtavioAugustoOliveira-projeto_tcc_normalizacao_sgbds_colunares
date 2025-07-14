import pandas as pd
import time
import psycopg2
import pyodbc
import snowflake.connector
import re
import sys
import os

# Adiciona o diretório raiz ao path para importar config_loader
try:
    sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
    from config_loader import obter_configuracoes, imprimir_configuracoes
except ImportError:
    print("ERRO: Não foi possível encontrar 'config_loader'. Certifique-se de que o script está na estrutura de pastas correta.")
    sys.exit(1)

# --- CONFIGURAÇÃO GLOBAL ---
config = obter_configuracoes()

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

# --- CONFIGURAÇÕES DE CONEXÃO ---
CONEXOES = {
    'PostgreSQL': { 'host': PG_HOST, 'port': PG_PORT, 'database': PG_DBNAME, 'user': PG_USER, 'password': PG_PASSWORD },
    'SQL Server': { 'server': SQL_SERVER_NAME, 'database': SQL_DBNAME, 'user': SQL_USER, 'password': SQL_PASSWORD, 'driver': '{ODBC Driver 17 for SQL Server}' },
    'Snowflake': { 'user': SNOW_USER, 'password': SNOW_PASSWORD, 'account': SNOW_ACCOUNT, 'warehouse': SNOW_WAREHOUSE, 'database': SNOW_DATABASE, 'schema': SNOW_SCHEMA }
}

# --- BIBLIOTECA DE QUERIES ---
QUERIES = {
    'PostgreSQL': {
        'Desnormalizado': {
            'E1': "SELECT NM_MUN, COUNT(*) AS contagem_setores, SUM(AREA_KM2) AS area_total_km2, AVG(AREA_KM2) AS area_media_km2, RANK() OVER (ORDER BY COUNT(*) DESC) AS ranking_por_contagem FROM cenario_nao_normalizado GROUP BY NM_MUN ORDER BY ranking_por_contagem;",
            'E2': "WITH PontosDeTeste (ponto) AS (SELECT * FROM (VALUES (ST_SetSRID(ST_MakePoint(-46.63, -23.55), 4326)), (ST_SetSRID(ST_MakePoint(-46.87, -23.38), 4326)), (ST_SetSRID(ST_MakePoint(-47.93, -23.49), 4326)), (ST_SetSRID(ST_MakePoint(-47.06, -22.90), 4326)), (ST_SetSRID(ST_MakePoint(-51.37, -20.77), 4326)), (ST_SetSRID(ST_MakePoint(-46.65, -23.56), 4326)), (ST_SetSRID(ST_MakePoint(-46.70, -23.52), 4326)), (ST_SetSRID(ST_MakePoint(-46.75, -23.48), 4326)), (ST_SetSRID(ST_MakePoint(-46.80, -23.44), 4326)), (ST_SetSRID(ST_MakePoint(-46.85, -23.40), 4326))) AS v) SELECT ST_AsText(p.ponto), t.CD_SETOR, t.NM_MUN FROM PontosDeTeste p JOIN cenario_nao_normalizado t ON ST_Contains(t.geometry, p.ponto);",
            'E3': "WITH MunicipioAlvo AS (SELECT ST_Union(geometry) AS geom_mun FROM cenario_nao_normalizado WHERE NM_MUN = 'Campinas') SELECT t.NM_SITUACAO, SUM(t.AREA_KM2) AS area_total_km2 FROM cenario_nao_normalizado t, MunicipioAlvo m WHERE ST_Intersects(t.geometry, m.geom_mun) GROUP BY t.NM_SITUACAO;",
            'E4': "SELECT a.CD_SETOR, COUNT(b.CD_SETOR) AS contagem_vizinhos FROM cenario_nao_normalizado a JOIN cenario_nao_normalizado b ON ST_Touches(a.geometry, b.geometry) WHERE a.NM_MUN = 'Sorocaba' AND b.NM_MUN = 'Sorocaba' AND a.CD_SETOR <> b.CD_SETOR GROUP BY a.CD_SETOR ORDER BY contagem_vizinhos DESC;"
        }, 'Normalizado': {
            'E1': "SELECT d.NM_MUN, COUNT(*) AS contagem_setores, SUM(f.AREA_KM2) AS area_total_km2, AVG(f.AREA_KM2) AS area_media_km2, RANK() OVER (ORDER BY COUNT(*) DESC) AS ranking_por_contagem FROM fato_setores_censitarios AS f JOIN dim_localizacao AS d ON f.ID_LOCALIZACAO_FK = d.ID_LOCALIZACAO GROUP BY d.NM_MUN ORDER BY ranking_por_contagem;",
            'E2': "WITH PontosDeTeste (ponto) AS (SELECT * FROM (VALUES (ST_SetSRID(ST_MakePoint(-46.63, -23.55), 4326)), (ST_SetSRID(ST_MakePoint(-46.87, -23.38), 4326)), (ST_SetSRID(ST_MakePoint(-47.93, -23.49), 4326)), (ST_SetSRID(ST_MakePoint(-47.06, -22.90), 4326)), (ST_SetSRID(ST_MakePoint(-51.37, -20.77), 4326)), (ST_SetSRID(ST_MakePoint(-46.65, -23.56), 4326)), (ST_SetSRID(ST_MakePoint(-46.70, -23.52), 4326)), (ST_SetSRID(ST_MakePoint(-46.75, -23.48), 4326)), (ST_SetSRID(ST_MakePoint(-46.80, -23.44), 4326)), (ST_SetSRID(ST_MakePoint(-46.85, -23.40), 4326))) AS v) SELECT ST_AsText(p.ponto), t.CD_SETOR, d.NM_MUN FROM PontosDeTeste p JOIN fato_setores_censitarios t ON ST_Contains(t.geometry, p.ponto) JOIN dim_localizacao d ON t.ID_LOCALIZACAO_FK = d.ID_LOCALIZACAO;",
            'E3': "WITH MunicipioAlvo AS (SELECT ST_Union(f.geometry) AS geom_mun FROM fato_setores_censitarios f JOIN dim_localizacao d ON f.ID_LOCALIZACAO_FK = d.ID_LOCALIZACAO WHERE d.NM_MUN = 'Campinas') SELECT s.NM_SITUACAO, SUM(f.AREA_KM2) AS area_total_km2 FROM fato_setores_censitarios f JOIN dim_situacao s ON f.ID_SITUACAO_FK = s.ID_SITUACAO, MunicipioAlvo m WHERE ST_Intersects(f.geometry, m.geom_mun) GROUP BY s.NM_SITUACAO;",
            'E4': "SELECT a.CD_SETOR, COUNT(b.CD_SETOR) AS contagem_vizinhos FROM fato_setores_censitarios a JOIN fato_setores_censitarios b ON ST_Touches(a.geometry, b.geometry) JOIN dim_localizacao da ON a.ID_LOCALIZACAO_FK = da.ID_LOCALIZACAO JOIN dim_localizacao db ON b.ID_LOCALIZACAO_FK = db.ID_LOCALIZACAO WHERE da.NM_MUN = 'Sorocaba' AND db.NM_MUN = 'Sorocaba' AND a.CD_SETOR <> b.CD_SETOR GROUP BY a.CD_SETOR ORDER BY contagem_vizinhos DESC;"
        }}, 'SQL Server': {
        'Desnormalizado': {
            'E1': "SELECT NM_MUN, COUNT(*) AS contagem_setores, SUM(AREA_KM2) AS area_total_km2, AVG(AREA_KM2) AS area_media_km2, RANK() OVER (ORDER BY COUNT(*) DESC) AS ranking_por_contagem FROM cenario_nao_normalizado GROUP BY NM_MUN ORDER BY ranking_por_contagem;",
            'E2': "WITH PontosDeTeste (ponto) AS (SELECT GEOMETRY::Point(-46.63, -23.55, 4326) UNION ALL SELECT GEOMETRY::Point(-46.87, -23.38, 4326) UNION ALL SELECT GEOMETRY::Point(-47.93, -23.49, 4326) UNION ALL SELECT GEOMETRY::Point(-47.06, -22.90, 4326) UNION ALL SELECT GEOMETRY::Point(-51.37, -20.77, 4326) UNION ALL SELECT GEOMETRY::Point(-46.65, -23.56, 4326) UNION ALL SELECT GEOMETRY::Point(-46.70, -23.52, 4326) UNION ALL SELECT GEOMETRY::Point(-46.75, -23.48, 4326) UNION ALL SELECT GEOMETRY::Point(-46.80, -23.44, 4326) UNION ALL SELECT GEOMETRY::Point(-46.85, -23.40, 4326)) SELECT p.ponto.ToString(), t.CD_SETOR, t.NM_MUN FROM PontosDeTeste p JOIN cenario_nao_normalizado t ON t.geometry.STContains(p.ponto) = 1;",
            'E3': "WITH MunicipioAlvo AS (SELECT GEOMETRY::UnionAggregate(geometry) AS geom_mun FROM cenario_nao_normalizado WHERE NM_MUN = 'Campinas') SELECT t.NM_SITUACAO, SUM(t.AREA_KM2) AS area_total_km2 FROM cenario_nao_normalizado t, MunicipioAlvo m WHERE t.geometry.STIntersects(m.geom_mun) = 1 GROUP BY t.NM_SITUACAO;",
            'E4': "SELECT a.CD_SETOR, COUNT(b.CD_SETOR) AS contagem_vizinhos FROM cenario_nao_normalizado a JOIN cenario_nao_normalizado b ON a.geometry.STTouches(b.geometry) = 1 WHERE a.NM_MUN = 'Sorocaba' AND b.NM_MUN = 'Sorocaba' AND a.CD_SETOR <> b.CD_SETOR GROUP BY a.CD_SETOR ORDER BY contagem_vizinhos DESC;"
        }, 'Normalizado': {
            'E1': "SELECT d.NM_MUN, COUNT(*) AS contagem_setores, SUM(f.AREA_KM2) AS area_total_km2, AVG(f.AREA_KM2) AS area_media_km2, RANK() OVER (ORDER BY COUNT(*) DESC) AS ranking_por_contagem FROM fato_setores_censitarios AS f JOIN dim_localizacao AS d ON f.ID_LOCALIZACAO_FK = d.ID_LOCALIZACAO GROUP BY d.NM_MUN ORDER BY ranking_por_contagem;",
            'E2': "WITH PontosDeTeste (ponto) AS (SELECT GEOMETRY::Point(-46.63, -23.55, 4326) UNION ALL SELECT GEOMETRY::Point(-46.87, -23.38, 4326) UNION ALL SELECT GEOMETRY::Point(-47.93, -23.49, 4326) UNION ALL SELECT GEOMETRY::Point(-47.06, -22.90, 4326) UNION ALL SELECT GEOMETRY::Point(-51.37, -20.77, 4326) UNION ALL SELECT GEOMETRY::Point(-46.65, -23.56, 4326) UNION ALL SELECT GEOMETRY::Point(-46.70, -23.52, 4326) UNION ALL SELECT GEOMETRY::Point(-46.75, -23.48, 4326) UNION ALL SELECT GEOMETRY::Point(-46.80, -23.44, 4326) UNION ALL SELECT GEOMETRY::Point(-46.85, -23.40, 4326)) SELECT p.ponto.ToString(), t.CD_SETOR, d.NM_MUN FROM PontosDeTeste p JOIN fato_setores_censitarios t ON t.geometry.STContains(p.ponto) = 1 JOIN dim_localizacao d ON t.ID_LOCALIZACAO_FK = d.ID_LOCALIZACAO;",
            'E3': "WITH MunicipioAlvo AS (SELECT GEOMETRY::UnionAggregate(f.geometry) AS geom_mun FROM fato_setores_censitarios f JOIN dim_localizacao d ON f.ID_LOCALIZACAO_FK = d.ID_LOCALIZACAO WHERE d.NM_MUN = 'Campinas') SELECT s.NM_SITUACAO, SUM(f.AREA_KM2) AS area_total_km2 FROM fato_setores_censitarios f JOIN dim_situacao s ON f.ID_SITUACAO_FK = s.ID_SITUACAO, MunicipioAlvo m WHERE f.geometry.STIntersects(m.geom_mun) = 1 GROUP BY s.NM_SITUACAO;",
            'E4': "SELECT a.CD_SETOR, COUNT(b.CD_SETOR) AS contagem_vizinhos FROM fato_setores_censitarios a JOIN fato_setores_censitarios b ON a.geometry.STTouches(b.geometry) = 1 JOIN dim_localizacao da ON a.ID_LOCALIZACAO_FK = da.ID_LOCALIZACAO JOIN dim_localizacao db ON b.ID_LOCALIZACAO_FK = db.ID_LOCALIZACAO WHERE da.NM_MUN = 'Sorocaba' AND db.NM_MUN = 'Sorocaba' AND a.CD_SETOR <> b.CD_SETOR GROUP BY a.CD_SETOR ORDER BY contagem_vizinhos DESC;"
        }}, 'Snowflake': {
        'Desnormalizado': {
            'E1': "SELECT NM_MUN, COUNT(*) AS contagem_setores, SUM(AREA_KM2) AS area_total_km2, AVG(AREA_KM2) AS area_media_km2, RANK() OVER (ORDER BY COUNT(*) DESC) AS ranking_por_contagem FROM cenario_nao_normalizado GROUP BY NM_MUN ORDER BY ranking_por_contagem;",
            'E2': "WITH PontosDeTeste (ponto) AS (SELECT * FROM (VALUES (ST_MAKEPOINT(-46.63, -23.55)), (ST_MAKEPOINT(-46.87, -23.38)), (ST_MAKEPOINT(-47.93, -23.49)), (ST_MAKEPOINT(-47.06, -22.90)), (ST_MAKEPOINT(-51.37, -20.77)), (ST_MAKEPOINT(-46.65, -23.56)), (ST_MAKEPOINT(-46.70, -23.52)), (ST_MAKEPOINT(-46.75, -23.48)), (ST_MAKEPOINT(-46.80, -23.44)), (ST_MAKEPOINT(-46.85, -23.40))) AS v(ponto)) SELECT p.ponto, t.CD_SETOR, t.NM_MUN FROM PontosDeTeste p JOIN cenario_nao_normalizado t ON ST_CONTAINS(t.geometry, p.ponto);",
            'E3': "WITH MunicipioAlvo AS (SELECT ST_UNION_AGG(geometry) AS geom_mun FROM cenario_nao_normalizado WHERE NM_MUN = 'Campinas') SELECT t.NM_SITUACAO, SUM(t.AREA_KM2) AS area_total_km2 FROM cenario_nao_normalizado t, MunicipioAlvo m WHERE ST_INTERSECTS(t.geometry, m.geom_mun) GROUP BY t.NM_SITUACAO;",
            'E4': "SELECT a.CD_SETOR, COUNT(b.CD_SETOR) AS contagem_vizinhos FROM cenario_nao_normalizado a JOIN cenario_nao_normalizado b ON ST_TOUCHES(a.geometry, b.geometry) WHERE a.NM_MUN = 'Sorocaba' AND b.NM_MUN = 'Sorocaba' AND a.CD_SETOR <> b.CD_SETOR GROUP BY a.CD_SETOR ORDER BY contagem_vizinhos DESC;"
        }, 'Normalizado': {
            'E1': "SELECT d.NM_MUN, COUNT(*) AS contagem_setores, SUM(f.AREA_KM2) AS area_total_km2, AVG(f.AREA_KM2) AS area_media_km2, RANK() OVER (ORDER BY COUNT(*) DESC) AS ranking_por_contagem FROM fato_setores_censitarios AS f JOIN dim_localizacao AS d ON f.ID_LOCALIZACAO_FK = d.ID_LOCALIZACAO GROUP BY d.NM_MUN ORDER BY ranking_por_contagem;",
            'E2': "WITH PontosDeTeste (ponto) AS (SELECT * FROM (VALUES (ST_MAKEPOINT(-46.63, -23.55)), (ST_MAKEPOINT(-46.87, -23.38)), (ST_MAKEPOINT(-47.93, -23.49)), (ST_MAKEPOINT(-47.06, -22.90)), (ST_MAKEPOINT(-51.37, -20.77)), (ST_MAKEPOINT(-46.65, -23.56)), (ST_MAKEPOINT(-46.70, -23.52)), (ST_MAKEPOINT(-46.75, -23.48)), (ST_MAKEPOINT(-46.80, -23.44)), (ST_MAKEPOINT(-46.85, -23.40))) AS v(ponto)) SELECT p.ponto, t.CD_SETOR, d.NM_MUN FROM PontosDeTeste p JOIN fato_setores_censitarios t ON ST_CONTAINS(t.geometry, p.ponto) JOIN dim_localizacao d ON t.ID_LOCALIZACAO_FK = d.ID_LOCALIZACAO;",
            'E3': "WITH MunicipioAlvo AS (SELECT ST_UNION_AGG(f.geometry) AS geom_mun FROM fato_setores_censitarios f JOIN dim_localizacao d ON f.ID_LOCALIZACAO_FK = d.ID_LOCALIZACAO WHERE d.NM_MUN = 'Campinas') SELECT s.NM_SITUACAO, SUM(f.AREA_KM2) AS area_total_km2 FROM fato_setores_censitarios f JOIN dim_situacao s ON f.ID_SITUACAO_FK = s.ID_SITUACAO, MunicipioAlvo m WHERE ST_INTERSECTS(f.geometry, m.geom_mun) GROUP BY s.NM_SITUACAO;",
            'E4': "SELECT a.CD_SETOR, COUNT(b.CD_SETOR) AS contagem_vizinhos FROM fato_setores_censitarios a JOIN fato_setores_censitarios b ON ST_TOUCHES(a.geometry, b.geometry) JOIN dim_localizacao da ON a.ID_LOCALIZACAO_FK = da.ID_LOCALIZACAO JOIN dim_localizacao db ON b.ID_LOCALIZACAO_FK = db.ID_LOCALIZACAO WHERE da.NM_MUN = 'Sorocaba' AND db.NM_MUN = 'Sorocaba' AND a.CD_SETOR <> b.CD_SETOR GROUP BY a.CD_SETOR ORDER BY contagem_vizinhos DESC;"
        }}}

NUM_EXECUCOES = 11

def get_connection(sgbd_name, config):
    """Cria e retorna uma conexão com o banco de dados apropriado."""
    conn = None
    if sgbd_name == 'PostgreSQL':
        conn = psycopg2.connect(**config)
    elif sgbd_name == 'SQL Server':
        conn_str = f"DRIVER={config['driver']};SERVER={config['server']};DATABASE={config['database']};UID={config['user']};PWD={config['password']};"
        conn = pyodbc.connect(conn_str)
    elif sgbd_name == 'Snowflake':
        conn = snowflake.connector.connect(**config)
    return conn

def get_storage_metrics(sgbd, tabelas):
    """Coleta métricas de armazenamento para as tabelas especificadas e retorna o tamanho total em MB."""
    config = CONEXOES[sgbd]
    tamanho_total_mb = 0
    conn = None
    try:
        conn = get_connection(sgbd, config)
        cursor = conn.cursor()
        total_bytes = 0
        if sgbd == 'PostgreSQL':
            for tabela in tabelas:
                cursor.execute(f"SELECT pg_total_relation_size('{tabela.lower()}');")
                size_bytes = cursor.fetchone()[0]
                if size_bytes: total_bytes += size_bytes
        elif sgbd == 'SQL Server':
            total_kb = 0
            for tabela in tabelas:
                cursor.execute(f"EXEC sp_spaceused '{tabela}';")
                row = cursor.fetchone()
                data_kb = int(re.search(r'(\d+)', row.data).group(1))
                index_kb = int(re.search(r'(\d+)', row.index_size).group(1))
                total_kb += data_kb + index_kb
            total_bytes = total_kb * 1024
        elif sgbd == 'Snowflake':
            for tabela in tabelas:
                db, schema = config['database'], config['schema']
                cursor.execute(f"SELECT BYTES FROM {db}.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{schema.upper()}' AND TABLE_NAME = '{tabela.upper()}'")
                size_bytes = cursor.fetchone()
                if size_bytes and size_bytes[0]: total_bytes += size_bytes[0]
        tamanho_total_mb = total_bytes / (1024 * 1024) if total_bytes > 0 else 0
    except Exception as e:
        print(f"❌ ERRO ao coletar armazenamento para {sgbd}: {e}")
        return None
    finally:
        if conn: conn.close()
    return tamanho_total_mb

def run_experiment(sgbd, modelo, query_id, tamanho_armazenamento_mb):
    """
    Executa o experimento para o cenário e retorna uma lista de dicionários padronizados para o CSV.
    """
    config = CONEXOES[sgbd]
    sql = QUERIES[sgbd][modelo][query_id]
    resultados_experimento = []

    print(f"\n🚀 Iniciando execução: {sgbd} | {modelo} | {query_id}")
    print(f"   📦 Armazenamento do cenário: {tamanho_armazenamento_mb:.3f} MB")
    print("-" * 60)

    try:
        conn = get_connection(sgbd, config)
        cursor = conn.cursor()
        for i in range(NUM_EXECUCOES):
            tempo_total_ms = None
            tempo_servidor_ms = None
            io_reads = None
            io_hits = None
            bytes_scanned_mb = None

            # Medição de tempo (comum a todos)
            start_time = time.time()
            cursor.execute(sql)
            _ = cursor.fetchall()
            end_time = time.time()
            tempo_total_ms = (end_time - start_time) * 1000

            if sgbd == 'PostgreSQL':
                try:
                    cursor.execute(f"EXPLAIN (ANALYZE, BUFFERS) {sql}")
                    explain_output = "\n".join(str(row[0]) for row in cursor.fetchall())
                    exec_time_match = re.search(r"Execution Time: ([\d.]+)", explain_output)
                    if exec_time_match:
                        tempo_servidor_ms = float(exec_time_match.group(1))
                    io_reads = sum([int(val) for val in re.findall(r"shared read=(\d+)", explain_output)])
                    io_hits = sum([int(val) for val in re.findall(r"shared hit=(\d+)", explain_output)])
                except Exception as e:
                    print(f"   ⚠️  Aviso (PG): Não foi possível extrair métricas detalhadas: {e}")
            elif sgbd == 'SQL Server':
                # Tenta capturar mensagens de I/O
                try:
                    while cursor.nextset():
                        pass
                    stats_msgs = "\n".join([msg[1] for msg in getattr(cursor, 'messages', []) if msg[0] != 'DBINFO'])
                    logical_reads_match = re.findall(r"logical reads (\d+)", stats_msgs)
                    if logical_reads_match:
                        io_reads = sum([int(val) for val in logical_reads_match])
                    physical_reads_match = re.findall(r"physical reads (\d+)", stats_msgs)
                    # Se quiser, pode adicionar physical_reads como outra coluna
                except Exception as e:
                    print(f"[AVISO] Erro ao tentar capturar métricas de I/O do SQL Server: {e}")
            elif sgbd == 'Snowflake':
                # Retry para buscar métricas na query_history
                def get_snowflake_query_metrics(cursor, query_id_sf, max_wait=60):
                    waited = 0
                    while waited < max_wait:
                        cursor.execute(f"SELECT EXECUTION_TIME, BYTES_SCANNED FROM snowflake.account_usage.query_history WHERE QUERY_ID = '{query_id_sf}' LIMIT 1;")
                        result = cursor.fetchone()
                        if result and result[0] is not None:
                            return result
                        time.sleep(5)
                        waited += 5
                    return None
                query_id_sf = getattr(cursor, 'sfqid', None)
                if query_id_sf:
                    result = get_snowflake_query_metrics(cursor, query_id_sf, max_wait=60)
                    if result:
                        try:
                            tempo_servidor_ms = float(result[0]) if result[0] is not None else None
                        except Exception:
                            tempo_servidor_ms = None
                        try:
                            bytes_scanned_mb = float(result[1]) / (1024*1024) if result[1] is not None else None
                        except Exception:
                            bytes_scanned_mb = None

            if i == 0:
                print(f"   🔥 Aquecimento: {tempo_total_ms:.2f} ms")
            else:
                print(f"   ✅ Exec {i:2d}/10: {tempo_total_ms:7.2f} ms")
                resultados_experimento.append({
                    'SGBD': sgbd,
                    'Modelo': modelo,
                    'Experimento': query_id,
                    'N_Execucao': i,
                    'Tempo_Total_ms': tempo_total_ms,
                    'Tempo_Servidor_ms': tempo_servidor_ms,
                    'Armazenamento_MB': tamanho_armazenamento_mb,
                    'IO_Reads': io_reads,
                    'IO_Hits': io_hits,
                    'Bytes_Scanned_MB': bytes_scanned_mb
                })
    except Exception as e:
        print(f"❌ ERRO GERAL NO EXPERIMENTO: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()
    return resultados_experimento

def save_postgresql_results(resultados, modelo, sgbd, experimento):
    """Salva os resultados detalhados do PostgreSQL em um arquivo CSV."""
    if not resultados:
        print(f"\nINFO: Nenhum resultado para salvar para {sgbd} | {modelo} | {experimento}.")
        return

    df = pd.DataFrame(resultados)
    RESULTS_DIR = os.path.join(os.path.dirname(__file__), 'results')
    os.makedirs(RESULTS_DIR, exist_ok=True)
    caminho_arquivo = os.path.join(RESULTS_DIR, f"resultado_{sgbd}_{modelo}_{experimento}.csv")
    df.to_csv(caminho_arquivo, index=False, float_format='%.3f')
    
    print("\n" + "=" * 60)
    print(f"📈 RESULTADOS SALVOS PARA - {modelo}")
    print(f"💾 Arquivo: {caminho_arquivo}")
    print("=" * 60)


if __name__ == "__main__":
    print("=" * 60)
    print("|| 🧪 ORQUESTRADOR DE EXPERIMENTOS DE PERFORMANCE 🧪 ||")
    print("=" * 60)
    imprimir_configuracoes()
    
    # --- Definição dos Testes a Serem Executados ---
    SGDB_LIST = ["PostgreSQL", "SQL Server", "Snowflake"]
    MODELO_LIST = ["Desnormalizado", "Normalizado"]
    EXPERIMENTO_LIST = ["E1", "E2", "E3", "E4"]
    
    tabelas_cenarios = {
        'Desnormalizado': ['cenario_nao_normalizado'],
        'Normalizado': ['fato_setores_censitarios', 'dim_localizacao', 'dim_situacao']
    }
    
    # --- Loop Principal de Execução ---
    for sgbd in SGDB_LIST:
        for modelo in MODELO_LIST:
            for experimento in EXPERIMENTO_LIST:
                print("\n" + "#" * 70)
                print(f"## INICIANDO BLOCO DE TESTE: {sgbd} | {modelo} | {experimento}")
                print("#" * 70)

                # Coleta de métricas de armazenamento para o cenário atual
                tabelas_do_modelo = tabelas_cenarios[modelo]
                tamanho_mb = get_storage_metrics(sgbd, tabelas_do_modelo)
                
                if tamanho_mb is None:
                    print(f"AVISO: Não foi possível calcular o armazenamento. Pulando este bloco de teste.")
                    continue

                # Executa o experimento
                resultados = run_experiment(sgbd, modelo, experimento, tamanho_mb)
                
                # Salva o CSV padronizado para todos os bancos
                df = pd.DataFrame(resultados)
                csv_path = f"results/resultado_{sgbd}_{modelo}_{experimento}.csv"
                df.to_csv(csv_path, index=False)
                print(f"💾 CSV salvo: {csv_path}")

    print("\n🎉 Todos os blocos de teste foram concluídos! 🎉")