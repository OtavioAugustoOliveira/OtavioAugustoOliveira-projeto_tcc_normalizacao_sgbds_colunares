--Experimento 3: Filtro Composto com JOIN
--Objetivo Analítico: "Quais são os nomes e as áreas de todos os municípios da região 'Nordeste' que possuem uma área superior a 5.000 km²?"

--O que Testa na Prática: A capacidade do otimizador de consultas em lidar com múltiplos filtros (um na tabela de dimensão e outro na tabela fato) e executar o JOIN de forma eficiente.



SELECT
    NM_MUN,
    AREA_KM2
FROM
    cenario_nao_normalizado
WHERE
    NM_REGIAO = 'Nordeste' AND AREA_KM2 > 5000;



-----------

SELECT
    mun.NM_MUN,
    mun.AREA_KM2
FROM
    fato_municipios AS mun
JOIN
    dim_estados AS est ON mun.ID_UF_FK = est.ID_UF
JOIN
    dim_regioes AS reg ON est.ID_REGIAO_FK = reg.ID_REGIAO
WHERE
    reg.NM_REGIAO = 'Nordeste' AND mun.AREA_KM2 > 5000;