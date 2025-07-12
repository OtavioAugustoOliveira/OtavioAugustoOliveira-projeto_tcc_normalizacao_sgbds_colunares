-- Tabela Desnormalizada
CREATE TABLE cenario_nao_normalizado (
    ts_updated DATETIMEOFFSET NOT NULL DEFAULT SYSDATETIMEOFFSET(),
    cd_regiao VARCHAR(1),
    nm_regiao VARCHAR(255),
    sigla_rg VARCHAR(2),
    cd_uf VARCHAR(2),
    nm_uf VARCHAR(255),
    sigla_uf VARCHAR(2),
    cd_mun VARCHAR(7) NOT NULL PRIMARY KEY,
    nm_mun VARCHAR(255),
    area_km2 FLOAT,
    geometry_wkt VARCHAR(MAX),
    geometry GEOMETRY
);

-- Tabelas de Dimensão
CREATE TABLE dim_regioes (
    id_regiao VARCHAR(1) NOT NULL PRIMARY KEY,
    nm_regiao VARCHAR(255),
    sigla_rg VARCHAR(2)
);

CREATE TABLE dim_estados (
    id_uf VARCHAR(2) NOT NULL PRIMARY KEY,
    nm_uf VARCHAR(255),
    sigla_uf VARCHAR(2),
    id_regiao_fk VARCHAR(1)
);

-- Tabela Fato
CREATE TABLE fato_municipios (
    ts_updated DATETIMEOFFSET NOT NULL DEFAULT SYSDATETIMEOFFSET(),
    id_municipio VARCHAR(7) NOT NULL PRIMARY KEY,
    nm_mun VARCHAR(255),
    area_km2 FLOAT,
    id_uf_fk VARCHAR(2),
    geometry_wkt VARCHAR(MAX),
    geometry GEOMETRY
);

-- Converter Geometria
UPDATE cenario_nao_normalizado SET geometry = geometry::STGeomFromText(geometry_wkt, 4326);
UPDATE fato_municipios SET geometry = geometry::STGeomFromText(geometry_wkt, 4326);

-- Adicionar Índices de Performance
CREATE NONCLUSTERED COLUMNSTORE INDEX ncci_cenario_analitico ON cenario_nao_normalizado (area_km2, nm_regiao, nm_uf, nm_mun);
CREATE NONCLUSTERED COLUMNSTORE INDEX ncci_fato_analitico ON fato_municipios (area_km2, nm_mun, id_uf_fk);

CREATE SPATIAL INDEX si_cenario_geometria ON cenario_nao_normalizado(geometry) WITH ( BOUNDING_BOX = (XMIN = -74, YMIN = -34, XMAX = -34, YMAX = 6) );
CREATE SPATIAL INDEX si_fato_geometria ON fato_municipios(geometry) WITH ( BOUNDING_BOX = (XMIN = -74, YMIN = -34, XMAX = -34, YMAX = 6) );

-- Adicionar Chaves Estrangeiras
ALTER TABLE dim_estados ADD CONSTRAINT fk_estado_regiao FOREIGN KEY (id_regiao_fk) REFERENCES dim_regioes(id_regiao);
ALTER TABLE fato_municipios ADD CONSTRAINT fk_municipio_estado FOREIGN KEY (id_uf_fk) REFERENCES dim_estados(id_uf);

-- Remover a coluna de texto redundante
ALTER TABLE cenario_nao_normalizado DROP COLUMN geometry_wkt;
ALTER TABLE fato_municipios DROP COLUMN geometry_wkt;


-- Para o cenário não normalizado
ALTER TABLE cenario_nao_normalizado
ADD SEARCH OPTIMIZATION ON GEO(geometry);

-- Para o cenário normalizado (na tabela fato)
ALTER TABLE fato_municipios
ADD SEARCH OPTIMIZATION ON GEO(geometry);
