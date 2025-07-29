SELECT
    nome_regiao AS regiao,
    nome_estado AS estado,
    COUNT(DISTINCT nome_municipio) AS quantidade_cidades
FROM
    desnormalizado_tabelao
GROUP BY
    nome_regiao, nome_estado
ORDER BY
    quantidade_cidades DESC; 