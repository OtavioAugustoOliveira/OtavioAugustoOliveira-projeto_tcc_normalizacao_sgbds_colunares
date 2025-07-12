--Experimento 1: Agregação Analítica Ampla
--Objetivo Analítico: "Qual a área total (em km²) e a população total de todos os municípios, agrupadas por cada estado (UF)?"

--O que Testa na Prática: A eficiência do SGBD em varrer uma grande quantidade de dados (SUM, GROUP BY). É o teste ideal para verificar a vantagem do armazenamento colunar, que só precisará ler as colunas AREA_KM2, populacao e as colunas de agrupamento, ignorando a geometria pesada.





SELECT
    SIGLA_UF,
    SUM(AREA_KM2) AS area_total_km2
FROM
    cenario_nao_normalizado
GROUP BY
    SIGLA_UF
ORDER BY
    SIGLA_UF;



------------

SELECT
    est.SIGLA_UF,
    SUM(mun.AREA_KM2) AS area_total_km2
    -- Adicione SUM(mun.populacao) se tiver esse dado
FROM
    fato_municipios AS mun
JOIN
    dim_estados AS est ON mun.ID_UF_FK = est.ID_UF
GROUP BY
    est.SIGLA_UF
ORDER BY
    est.SIGLA_UF;
