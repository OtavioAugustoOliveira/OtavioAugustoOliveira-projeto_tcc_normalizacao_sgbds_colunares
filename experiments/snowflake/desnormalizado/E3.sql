SELECT
    nome_regiao AS regiao,
    COUNT(DISTINCT nome_estado) AS quantidade_estados
FROM
    desnormalizado_tabelao
GROUP BY
    nome_regiao
ORDER BY
    quantidade_estados DESC; 