--Detalhamento do Experimento 4
--Objetivo Analítico: "Para cada município do estado de Pernambuco, listar todos os seus municípios vizinhos."

--Operação Principal: SELF-JOIN da tabela de municípios (... AS A JOIN ... AS B) com um predicado espacial ON ST_Touches(A.geometry, B.geometry).






-- Encontra todos os pares de municípios vizinhos em Pernambuco
SELECT
    A.NM_MUN AS municipio_origem,
    B.NM_MUN AS municipio_vizinho
FROM
    cenario_nao_normalizado AS A
JOIN
    cenario_nao_normalizado AS B ON ST_Touches(A.geometry, B.geometry)
WHERE
    A.SIGLA_UF = 'PE'
    AND A.CD_MUN < B.CD_MUN; -- Condição para evitar duplicatas (ex: Recife-Olinda e Olinda-Recife) e auto-referência.







-- Encontra todos os pares de municípios vizinhos em Pernambuco
SELECT
    A.NM_MUN AS municipio_origem,
    B.NM_MUN AS municipio_vizinho
FROM
    fato_municipios AS A
JOIN
    fato_municipios AS B ON ST_Touches(A.geometry, B.geometry)
WHERE
    A.ID_UF_FK = '26' -- Supondo que '26' é o código de Pernambuco
    AND A.ID_MUNICIPIO < B.ID_MUNICIPIO; -- Mesma condição para evitar duplicatas