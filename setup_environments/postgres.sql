-- Novo modelo estrela para setores censitários
CREATE EXTENSION IF NOT EXISTS postgis;

-- Tabela de Dimensão: Localização
DROP TABLE IF EXISTS dim_localizacao CASCADE;
CREATE TABLE dim_localizacao (
    id_localizacao SERIAL PRIMARY KEY,
    cd_mun VARCHAR(50),
    nm_mun VARCHAR(100),
    cd_dist VARCHAR(50),
    nm_dist VARCHAR(100),
    cd_subdist VARCHAR(50),
    cd_rgi VARCHAR(50),
    nm_rgi VARCHAR(100),
    cd_rgint VARCHAR(50),
    nm_rgint VARCHAR(100),
    cd_uf VARCHAR(10),
    nm_uf VARCHAR(100),
    cd_regiao VARCHAR(10),
    nm_regiao VARCHAR(100)
);

-- Tabela de Dimensão: Situação
DROP TABLE IF EXISTS dim_situacao CASCADE;
CREATE TABLE dim_situacao (
    id_situacao SERIAL PRIMARY KEY,
    cd_sit VARCHAR(50),
    nm_situacao VARCHAR(100)
);

-- Tabela de Dimensão: Tipo
DROP TABLE IF EXISTS dim_tipo CASCADE;
CREATE TABLE dim_tipo (
    id_tipo SERIAL PRIMARY KEY,
    cd_tipo VARCHAR(50),
    nm_tipo VARCHAR(100)
);

-- Tabela de Dimensão: Bairro
DROP TABLE IF EXISTS dim_bairro CASCADE;
CREATE TABLE dim_bairro (
    id_bairro SERIAL PRIMARY KEY,
    cd_bairro VARCHAR(50),
    nm_bairro VARCHAR(100)
);

-- Tabela de Dimensão: Conurbação
DROP TABLE IF EXISTS dim_concurb CASCADE;
CREATE TABLE dim_concurb (
    id_concurb SERIAL PRIMARY KEY,
    cd_concurb VARCHAR(50),
    nm_concurb VARCHAR(100)
);

-- Tabela Fato: Setores Censitários
DROP TABLE IF EXISTS fato_setores_censitarios CASCADE;
CREATE TABLE fato_setores_censitarios (
    id_fato_setor SERIAL PRIMARY KEY,
    cd_setor VARCHAR(50),
    area_km2 DOUBLE PRECISION,
    geometry_wkt TEXT,
    id_localizacao_fk INTEGER REFERENCES dim_localizacao(id_localizacao),
    id_situacao_fk INTEGER REFERENCES dim_situacao(id_situacao),
    id_tipo_fk INTEGER REFERENCES dim_tipo(id_tipo),
    id_bairro_fk INTEGER REFERENCES dim_bairro(id_bairro),
    id_concurb_fk INTEGER REFERENCES dim_concurb(id_concurb),
    geometry GEOMETRY(GEOMETRY, 4326)
);

-- Tabela Não Normalizada de Referência
DROP TABLE IF EXISTS cenario_nao_normalizado CASCADE;
CREATE TABLE cenario_nao_normalizado (
    cd_setor VARCHAR(50),
    nm_situacao VARCHAR(100),
    cd_sit VARCHAR(50),
    cd_tipo VARCHAR(50),
    area_km2 DOUBLE PRECISION,
    cd_regiao VARCHAR(10),
    nm_regiao VARCHAR(100),
    cd_uf VARCHAR(10),
    nm_uf VARCHAR(100),
    cd_mun VARCHAR(50),
    nm_mun VARCHAR(100),
    cd_dist VARCHAR(50),
    nm_dist VARCHAR(100),
    cd_subdist VARCHAR(50),
    cd_bairro VARCHAR(50),
    nm_bairro VARCHAR(100),
    cd_rgint VARCHAR(50),
    nm_rgint VARCHAR(100),
    cd_rgi VARCHAR(50),
    nm_rgi VARCHAR(100),
    cd_concurb VARCHAR(50),
    nm_concurb VARCHAR(100),
    geometry_wkt TEXT,
    geometry GEOMETRY(GEOMETRY, 4326)
);

-- Conversão de geometria e índice espacial (deixe no final absoluto)
UPDATE fato_setores_censitarios SET geometry = ST_GeomFromText(geometry_wkt, 4326);
ALTER TABLE fato_setores_censitarios DROP COLUMN geometry_wkt;
CREATE INDEX idx_fato_setores_geom ON fato_setores_censitarios USING GIST (geometry);

UPDATE cenario_nao_normalizado SET geometry = ST_GeomFromText(geometry_wkt, 4326);
ALTER TABLE cenario_nao_normalizado DROP COLUMN geometry_wkt;
CREATE INDEX idx_cenario_nao_normalizado_geom ON cenario_nao_normalizado USING GIST (geometry);
