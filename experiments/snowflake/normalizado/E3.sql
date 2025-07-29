SELECT
    r.nm_regiao AS regiao,
    COUNT(e.cd_uf) AS quantidade_estados
FROM
    normalizado_estados AS e
JOIN
    normalizado_regioes AS r ON ST_Contains(r.geometry, e.geometry)
GROUP BY
    r.nm_regiao
ORDER BY
    quantidade_estados DESC; 