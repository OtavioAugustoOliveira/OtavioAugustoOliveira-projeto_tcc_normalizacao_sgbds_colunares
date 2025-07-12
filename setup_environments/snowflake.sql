-- Tabela Desnormalizada
CREATE OR REPLACE TABLE "cenario_nao_normalizado" (
    "ts_updated" TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP(),
    "cd_regiao" VARCHAR(1),
    "nm_regiao" VARCHAR(255),
    "sigla_rg" VARCHAR(2),
    "cd_uf" VARCHAR(2),
    "nm_uf" VARCHAR(255),
    "sigla_uf" VARCHAR(2),
    "cd_mun" VARCHAR(7),
    "nm_mun" VARCHAR(255),
    "area_km2" FLOAT,
    "geometry_wkt" VARCHAR,
    "geometry" GEOGRAPHY
);

-- Tabelas de Dimensão
CREATE OR REPLACE TABLE "dim_regioes" (
    "id_regiao" VARCHAR(1) PRIMARY KEY,
    "nm_regiao" VARCHAR(255),
    "sigla_rg" VARCHAR(2)
);

CREATE OR REPLACE TABLE "dim_estados" (
    "id_uf" VARCHAR(2) PRIMARY KEY,
    "nm_uf" VARCHAR(255),
    "sigla_uf" VARCHAR(2),
    "id_regiao_fk" VARCHAR(1)
);

-- Tabela Fato
CREATE OR REPLACE TABLE "fato_municipios" (
    "ts_updated" TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP(),
    "id_municipio" VARCHAR(7) PRIMARY KEY,
    "nm_mun" VARCHAR(255),
    "area_km2" FLOAT,
    "id_uf_fk" VARCHAR(2),
    "geometry_wkt" VARCHAR,
    "geometry" GEOGRAPHY
);



-- Converter Geometria
UPDATE "cenario_nao_normalizado" SET "geometry" = TO_GEOGRAPHY("geometry_wkt");
UPDATE "fato_municipios" SET "geometry" = TO_GEOGRAPHY("geometry_wkt");

-- Adicionar Chaves Estrangeiras
ALTER TABLE "dim_estados" ADD CONSTRAINT "fk_estado_regiao" FOREIGN KEY ("id_regiao_fk") REFERENCES "dim_regioes"("id_regiao");
ALTER TABLE "fato_municipios" ADD CONSTRAINT "fk_municipio_estado" FOREIGN KEY ("id_uf_fk") REFERENCES "dim_estados"("id_uf");

-- Remover a coluna de texto redundante
ALTER TABLE cenario_nao_normalizado DROP COLUMN geometry_wkt;
ALTER TABLE fato_municipios DROP COLUMN geometry_wkt;

-- Para o cenário não normalizado
CREATE SPATIAL INDEX idx_nao_normalizado_geom
ON cenario_nao_normalizado(geometry);

-- Para o cenário normalizado (na tabela fato)
CREATE SPATIAL INDEX idx_fato_municipios_geom
ON fato_municipios(geometry);