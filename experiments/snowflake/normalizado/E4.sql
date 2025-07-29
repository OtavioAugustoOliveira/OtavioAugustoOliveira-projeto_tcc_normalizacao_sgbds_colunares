SELECT
    e.nm_uf AS estado,
    m.nm_mun AS cidade,
    COUNT(l.id_face_logradouro) AS quantidade_logradouros
FROM
    normalizado_logradouros AS l
JOIN
    normalizado_municipios AS m ON ST_Contains(m.geometry, l.geometry)
JOIN
    normalizado_estados AS e ON ST_Contains(e.geometry, m.geometry)
GROUP BY
    e.nm_uf, m.nm_mun
ORDER BY
    quantidade_logradouros DESC; 