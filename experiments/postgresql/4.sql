-- Experimento 4: Contagem Massiva de Vizinhos
-- Objetivo: Executar a operação mais cara: um JOIN espacial da tabela contra ela mesma em escopo maior
-- Pergunta: "Para cada setor censitário do município de 'Sorocaba', conte quantos outros setores o tocam (são vizinhos diretos)."

-- Cenário Desnormalizado
SELECT a.CD_SETOR, COUNT(b.CD_SETOR) AS contagem_vizinhos
FROM cenario_nao_normalizado a
JOIN cenario_nao_normalizado b ON ST_Touches(a.geometry, b.geometry)
WHERE a.NM_MUN = 'Sorocaba' AND b.NM_MUN = 'Sorocaba' AND a.CD_SETOR <> b.CD_SETOR
GROUP BY a.CD_SETOR
ORDER BY contagem_vizinhos DESC;

-- Cenário Normalizado
SELECT a.CD_SETOR, COUNT(b.CD_SETOR) AS contagem_vizinhos
FROM fato_setores_censitarios a
JOIN fato_setores_censitarios b ON ST_Touches(a.geometry, b.geometry)
JOIN dim_localizacao da ON a.ID_LOCALIZACAO_FK = da.ID_LOCALIZACAO
JOIN dim_localizacao db ON b.ID_LOCALIZACAO_FK = db.ID_LOCALIZACAO
WHERE da.NM_MUN = 'Sorocaba' AND db.NM_MUN = 'Sorocaba' AND a.CD_SETOR <> b.CD_SETOR
GROUP BY a.CD_SETOR
ORDER BY contagem_vizinhos DESC;