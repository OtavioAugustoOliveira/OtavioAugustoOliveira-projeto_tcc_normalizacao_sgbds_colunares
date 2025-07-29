SELECT
    r.nm_regiao AS regiao,
    e.nm_uf AS estado,
    m.nm_mun AS cidade,
    COUNT(l.id_face_logradouro) AS quantidade_logradouros
FROM
    normalizado_logradouros AS l
JOIN
    normalizado_municipios AS m ON ST_Contains(m.geometry, l.geometry)
JOIN
    normalizado_estados AS e ON ST_Contains(e.geometry, m.geometry)
JOIN
    normalizado_regioes AS r ON ST_Contains(r.geometry, m.geometry)
GROUP BY
    r.nm_regiao, e.nm_uf, m.nm_mun
ORDER BY
    quantidade_logradouros DESC; 