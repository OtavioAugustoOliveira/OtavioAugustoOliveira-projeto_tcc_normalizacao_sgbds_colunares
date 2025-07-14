-- Experimento 3: Filtro com Geometria Complexa e Agregação
-- Objetivo: Aumentar drasticamente a quantidade de dados lidos e processados pelo filtro espacial
-- Pergunta: "Para todos os setores censitários que interceptam a mancha urbana do município de 'Campinas', agrupe-os pela sua situação ('Urbano' ou 'Rural') e some a área de cada grupo."

-- Cenário Desnormalizado
WITH MunicipioAlvo AS (
    SELECT ST_Union(geometry) AS geom_mun 
    FROM cenario_nao_normalizado 
    WHERE NM_MUN = 'Campinas'
)
SELECT t.NM_SITUACAO, SUM(t.AREA_KM2) AS area_total_km2
FROM cenario_nao_normalizado t, MunicipioAlvo m 
WHERE ST_Intersects(t.geometry, m.geom_mun) 
GROUP BY t.NM_SITUACAO;

-- Cenário Normalizado
WITH MunicipioAlvo AS (
    SELECT ST_Union(f.geometry) AS geom_mun
    FROM fato_setores_censitarios f 
    JOIN dim_localizacao d ON f.ID_LOCALIZACAO_FK = d.ID_LOCALIZACAO 
    WHERE d.NM_MUN = 'Campinas'
)
SELECT s.NM_SITUACAO, SUM(f.AREA_KM2) AS area_total_km2
FROM fato_setores_censitarios f
JOIN dim_situacao s ON f.ID_SITUACAO_FK = s.ID_SITUACAO
, MunicipioAlvo m
WHERE ST_Intersects(f.geometry, m.geom_mun)
GROUP BY s.NM_SITUACAO;