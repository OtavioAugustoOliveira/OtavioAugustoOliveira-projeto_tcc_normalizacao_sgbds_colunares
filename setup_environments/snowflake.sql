-- DROP de todas as tabelas do modelo (garante ambiente limpo)
DROP TABLE IF EXISTS normalizado_municipios;
DROP TABLE IF EXISTS normalizado_estados;
DROP TABLE IF EXISTS normalizado_regioes;
DROP TABLE IF EXISTS normalizado_logradouros;
DROP TABLE IF EXISTS desnormalizado_tabelao;

CREATE TABLE normalizado_municipios (
    cd_mun STRING,
    nm_mun STRING,
    sigla_uf STRING,
    geometry_wkt STRING
);

CREATE TABLE normalizado_estados (
    cd_uf STRING,
    nm_uf STRING,
    sigla_uf STRING,
    geometry_wkt STRING
);

CREATE TABLE normalizado_regioes (
    nm_regiao STRING,
    sigla_rg STRING,
    geometry_wkt STRING
);

CREATE TABLE normalizado_logradouros (
    id_face_logradouro STRING,
    nm_log STRING,
    geometry_wkt STRING
);

CREATE TABLE desnormalizado_tabelao (
    id_face_logradouro STRING,
    nome_logradouro STRING,
    geometry_wkt STRING,
    id_municipio_logradouro STRING,
    nome_municipio STRING,
    sigla_estado STRING,
    nome_estado STRING,
    sigla_regiao STRING,
    nome_regiao STRING
);

-- Conversão de geometria para tabelas normalizadas e desnormalizada
ALTER TABLE normalizado_municipios ADD COLUMN geometry GEOGRAPHY;
UPDATE normalizado_municipios SET geometry = TO_GEOGRAPHY(geometry_wkt);
ALTER TABLE normalizado_municipios DROP COLUMN geometry_wkt;

ALTER TABLE normalizado_estados ADD COLUMN geometry GEOGRAPHY;
UPDATE normalizado_estados SET geometry = TO_GEOGRAPHY(geometry_wkt);
ALTER TABLE normalizado_estados DROP COLUMN geometry_wkt;

ALTER TABLE normalizado_regioes ADD COLUMN geometry GEOGRAPHY;
UPDATE normalizado_regioes SET geometry = TO_GEOGRAPHY(geometry_wkt);
ALTER TABLE normalizado_regioes DROP COLUMN geometry_wkt;

ALTER TABLE normalizado_logradouros ADD COLUMN geometry GEOGRAPHY;
UPDATE normalizado_logradouros SET geometry = TO_GEOGRAPHY(geometry_wkt);
ALTER TABLE normalizado_logradouros DROP COLUMN geometry_wkt;

-- Conversão de geometria para a tabela DESNORMALIZADO_TABELAO
ALTER TABLE desnormalizado_tabelao ADD COLUMN geometry GEOGRAPHY;
UPDATE desnormalizado_tabelao SET geometry = TO_GEOGRAPHY(geometry_wkt);
ALTER TABLE desnormalizado_tabelao DROP COLUMN geometry_wkt;

-- Chaves primárias
ALTER TABLE normalizado_municipios ADD CONSTRAINT pk_normalizado_municipios PRIMARY KEY (cd_mun);
ALTER TABLE normalizado_estados ADD CONSTRAINT pk_normalizado_estados PRIMARY KEY (cd_uf);
ALTER TABLE normalizado_regioes ADD CONSTRAINT pk_normalizado_regioes PRIMARY KEY (sigla_rg);
ALTER TABLE normalizado_logradouros ADD CONSTRAINT pk_normalizado_logradouros PRIMARY KEY (id_face_logradouro);

-- Uniques necessários para as FKs
ALTER TABLE normalizado_estados ADD CONSTRAINT uq_normalizado_estados_sigla_uf UNIQUE (sigla_uf);
ALTER TABLE normalizado_regioes ADD CONSTRAINT uq_normalizado_regioes_sigla_rg UNIQUE (sigla_rg);

-- Chaves estrangeiras
ALTER TABLE normalizado_municipios ADD CONSTRAINT fk_municipios_estados FOREIGN KEY (sigla_uf) REFERENCES normalizado_estados(sigla_uf);