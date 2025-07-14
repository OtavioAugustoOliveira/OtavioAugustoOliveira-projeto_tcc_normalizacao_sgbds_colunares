-- Experimento 1: Agregação Analítica Complexa
-- Objetivo: Forçar o banco a processar resultados da agregação com função de janela (RANK)
-- Pergunta: "Para cada município, calcule a contagem de setores, a área total, a área média e crie um ranking dos municípios por quantidade de setores."

-- Cenário Desnormalizado
SELECT
    NM_MUN,
    COUNT(*) AS contagem_setores,
    SUM(AREA_KM2) AS area_total_km2,
    AVG(AREA_KM2) AS area_media_km2,
    RANK() OVER (ORDER BY COUNT(*) DESC) AS ranking_por_contagem
FROM cenario_nao_normalizado
GROUP BY NM_MUN
ORDER BY ranking_por_contagem;

-- Cenário Normalizado
SELECT
    d.NM_MUN,
    COUNT(*) AS contagem_setores,
    SUM(f.AREA_KM2) AS area_total_km2,
    AVG(f.AREA_KM2) AS area_media_km2,
    RANK() OVER (ORDER BY COUNT(*) DESC) AS ranking_por_contagem
FROM fato_setores_censitarios AS f
JOIN dim_localizacao AS d ON f.ID_LOCALIZACAO_FK = d.ID_LOCALIZACAO
GROUP BY d.NM_MUN
ORDER BY ranking_por_contagem;
