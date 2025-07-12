
--DROP TABLE cenario_nao_normalizado;
--DROP TABLE dim_regioes;
--DROP TABLE dim_estados;
--DROP TABLE fato_municipios;
CREATE EXTENSION IF NOT EXISTS postgis;

-- Tabela Desnormalizada
CREATE TABLE cenario_nao_normalizado (
    TS_UPDATED TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CD_REGIAO VARCHAR(1),
    NM_REGIAO VARCHAR(255),
    SIGLA_RG VARCHAR(2),
    CD_UF VARCHAR(2),
    NM_UF VARCHAR(255),
    SIGLA_UF VARCHAR(2),
    CD_MUN VARCHAR(7),
    NM_MUN VARCHAR(255),
    AREA_KM2 DOUBLE PRECISION,
    GEOMETRY_WKT TEXT,
    GEOMETRY GEOMETRY(GEOMETRY, 4326)
);

-- Tabelas de Dimensão
CREATE TABLE dim_regioes (
    ID_REGIAO VARCHAR(1) PRIMARY KEY,
    NM_REGIAO VARCHAR(255),
    SIGLA_RG VARCHAR(2)
);

CREATE TABLE dim_estados (
    ID_UF VARCHAR(2) PRIMARY KEY,
    NM_UF VARCHAR(255),
    SIGLA_UF VARCHAR(2),
    ID_REGIAO_FK VARCHAR(1)
);

-- Tabela Fato
CREATE TABLE fato_municipios (
    TS_UPDATED TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ID_MUNICIPIO VARCHAR(7) PRIMARY KEY,
    NM_MUN VARCHAR(255),
    AREA_KM2 DOUBLE PRECISION,
    ID_UF_FK VARCHAR(2),
    GEOMETRY_WKT TEXT,
    GEOMETRY GEOMETRY(GEOMETRY, 4326)
);

-- 1. Converter Geometria WKT para o tipo nativo

-- Converter Geometria
UPDATE cenario_nao_normalizado SET geometry = ST_GeomFromText(geometry_wkt, 4326);
UPDATE fato_municipios SET geometry = ST_GeomFromText(geometry_wkt, 4326);

-- Adicionar Chaves Estrangeiras
ALTER TABLE dim_estados ADD CONSTRAINT fk_estado_regiao FOREIGN KEY (id_regiao_fk) REFERENCES dim_regioes(id_regiao);
ALTER TABLE fato_municipios ADD CONSTRAINT fk_municipio_estado FOREIGN KEY (id_uf_fk) REFERENCES dim_estados(id_uf);

-- Remover a coluna de texto redundante
ALTER TABLE cenario_nao_normalizado DROP COLUMN geometry_wkt;
ALTER TABLE fato_municipios DROP COLUMN geometry_wkt;


-- Para o cenário não normalizado
CREATE INDEX idx_nao_normalizado_geom
ON cenario_nao_normalizado
USING GIST (geometry);

-- Para o cenário normalizado (na tabela fato)
CREATE INDEX idx_fato_municipios_geom
ON fato_municipios
USING GIST (geometry);