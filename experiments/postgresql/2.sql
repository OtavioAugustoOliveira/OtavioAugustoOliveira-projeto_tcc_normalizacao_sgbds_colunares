-- Experimento 2: Múltiplos Pontos em Polígonos
-- Objetivo: Forçar centenas de buscas em uma única consulta, testando a capacidade do banco de lidar com JOINs baseados em predicados espaciais
-- Pergunta: "Para um conjunto de pontos aleatórios em São Paulo, determine em qual setor censitário cada ponto está localizado."

WITH PontosDeTeste (ponto) AS (
    SELECT * FROM (VALUES
        (ST_SetSRID(ST_MakePoint(-46.63, -23.55), 4326)),
        (ST_SetSRID(ST_MakePoint(-46.87, -23.38), 4326)),
        (ST_SetSRID(ST_MakePoint(-47.93, -23.49), 4326)),
        (ST_SetSRID(ST_MakePoint(-47.06, -22.90), 4326)),
        (ST_SetSRID(ST_MakePoint(-51.37, -20.77), 4326)),
        (ST_SetSRID(ST_MakePoint(-46.65, -23.56), 4326)),
        (ST_SetSRID(ST_MakePoint(-46.70, -23.52), 4326)),
        (ST_SetSRID(ST_MakePoint(-46.75, -23.48), 4326)),
        (ST_SetSRID(ST_MakePoint(-46.80, -23.44), 4326)),
        (ST_SetSRID(ST_MakePoint(-46.85, -23.40), 4326))
    ) AS v
)
-- Cenário Desnormalizado
SELECT ST_AsText(p.ponto), t.CD_SETOR, t.NM_MUN 
FROM PontosDeTeste p 
JOIN cenario_nao_normalizado t ON ST_Contains(t.geometry, p.ponto);

-- Cenário Normalizado
SELECT ST_AsText(p.ponto), t.CD_SETOR, d.NM_MUN 
FROM PontosDeTeste p 
JOIN fato_setores_censitarios t ON ST_Contains(t.geometry, p.ponto)
JOIN dim_localizacao d ON t.ID_LOCALIZACAO_FK = d.ID_LOCALIZACAO;