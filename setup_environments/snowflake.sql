-- Novo modelo estrela para setores censitários

-- Tabela de Dimensão: Localização
CREATE OR REPLACE TABLE DIM_LOCALIZACAO (
    id_localizacao INTEGER AUTOINCREMENT PRIMARY KEY,
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
CREATE OR REPLACE TABLE DIM_SITUACAO (
    id_situacao INTEGER AUTOINCREMENT PRIMARY KEY,
    cd_sit VARCHAR(50),
    nm_situacao VARCHAR(100)
);

-- Tabela de Dimensão: Tipo
CREATE OR REPLACE TABLE DIM_TIPO (
    id_tipo INTEGER AUTOINCREMENT PRIMARY KEY,
    cd_tipo VARCHAR(50),
    nm_tipo VARCHAR(100)
);

-- Tabela de Dimensão: Bairro
CREATE OR REPLACE TABLE DIM_BAIRRO (
    id_bairro INTEGER AUTOINCREMENT PRIMARY KEY,
    cd_bairro VARCHAR(50),
    nm_bairro VARCHAR(100)
);

-- Tabela de Dimensão: Conurbação
CREATE OR REPLACE TABLE DIM_CONCURB (
    id_concurb INTEGER AUTOINCREMENT PRIMARY KEY,
    cd_concurb VARCHAR(50),
    nm_concurb VARCHAR(100)
);

-- Tabela Fato: Setores Censitários
CREATE OR REPLACE TABLE FATO_SETORES_CENSITARIOS (
    id_fato_setor INTEGER AUTOINCREMENT PRIMARY KEY,
    cd_setor VARCHAR(50),
    area_km2 FLOAT,
    geometry_wkt VARCHAR,
    id_localizacao_fk INTEGER REFERENCES DIM_LOCALIZACAO(id_localizacao),
    id_situacao_fk INTEGER REFERENCES DIM_SITUACAO(id_situacao),
    id_tipo_fk INTEGER REFERENCES DIM_TIPO(id_tipo),
    id_bairro_fk INTEGER REFERENCES DIM_BAIRRO(id_bairro),
    id_concurb_fk INTEGER REFERENCES DIM_CONCURB(id_concurb),
    geometry GEOGRAPHY
);

-- Tabela Não Normalizada de Referência
CREATE OR REPLACE TABLE CENARIO_NAO_NORMALIZADO (
    cd_setor VARCHAR(50),
    nm_situacao VARCHAR(100),
    cd_sit VARCHAR(50),
    cd_tipo VARCHAR(50),
    area_km2 FLOAT,
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
    geometry_wkt VARCHAR,
    geometry GEOGRAPHY
);

-- Conversão de geometria (deixe no final absoluto)
UPDATE FATO_SETORES_CENSITARIOS SET geometry = TO_GEOGRAPHY(geometry_wkt);
ALTER TABLE FATO_SETORES_CENSITARIOS DROP COLUMN geometry_wkt;

UPDATE CENARIO_NAO_NORMALIZADO SET geometry = TO_GEOGRAPHY(geometry_wkt);
ALTER TABLE CENARIO_NAO_NORMALIZADO DROP COLUMN geometry_wkt;

-- Nota: Snowflake não suporta índices espaciais como PostgreSQL/SQL Server
-- O Snowflake otimiza automaticamente consultas espaciais em colunas GEOGRAPHY