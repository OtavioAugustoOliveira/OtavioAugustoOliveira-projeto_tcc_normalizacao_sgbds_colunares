--Experimento 2: Filtro Geográfico Simples (Point-in-Polygon)
--Objetivo Analítico: "Dado um ponto de coordenada específico (ex: o Palácio do Campo das Princesas no Recife), qual é o município que contém este ponto?"

--O que Testa na Prática: A eficiência do índice espacial para uma busca muito seletiva ("seek"). É um dos casos de uso mais comuns em geoprocessamento.



SELECT
    NM_MUN,
    NM_UF
FROM
    cenario_nao_normalizado
WHERE
    geometry.STContains(geography::Point(-8.0578, -34.8779, 4326)) = 1;



-------------

SELECT
    mun.NM_MUN,
    est.NM_UF
FROM
    fato_municipios AS mun
JOIN
    dim_estados AS est ON mun.ID_UF_FK = est.ID_UF
WHERE
    mun.geometry.STContains(geography::Point(-8.0578, -34.8779, 4326)) = 1;