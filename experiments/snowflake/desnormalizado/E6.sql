SELECT
    nome_regiao AS regiao,
    nome_estado AS estado,
    nome_municipio AS cidade,
    COUNT(id_face_logradouro) AS quantidade_logradouros
FROM
    desnormalizado_tabelao
GROUP BY
    nome_regiao, nome_estado, nome_municipio
ORDER BY
    quantidade_logradouros DESC; 