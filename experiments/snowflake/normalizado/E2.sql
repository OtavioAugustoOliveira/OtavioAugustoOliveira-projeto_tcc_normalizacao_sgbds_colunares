SELECT
    e.nm_uf AS estado,
    COUNT(m.cd_mun) AS quantidade_cidades
FROM
    normalizado_municipios AS m
JOIN
    normalizado_estados AS e ON ST_Contains(e.geometry, m.geometry)
GROUP BY
    e.nm_uf
ORDER BY
    quantidade_cidades DESC; 