-- Novo modelo estrela para setores censitários

-- Tabela Fato: Setores Censitários (drop primeiro por dependências)
DROP TABLE IF EXISTS fato_setores_censitarios;

-- Tabelas de Dimensão (drop em ordem reversa de dependência)
DROP TABLE IF EXISTS dim_concurb;
DROP TABLE IF EXISTS dim_bairro;
DROP TABLE IF EXISTS dim_tipo;
DROP TABLE IF EXISTS dim_situacao;
DROP TABLE IF EXISTS dim_localizacao;

-- Tabela Não Normalizada de Referência
DROP TABLE IF EXISTS cenario_nao_normalizado;

-- Agora recrie as tabelas normalmente

-- Tabela de Dimensão: Localização
CREATE TABLE dim_localizacao (
    id_localizacao INT IDENTITY(1,1) PRIMARY KEY,
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
CREATE TABLE dim_situacao (
    id_situacao INT IDENTITY(1,1) PRIMARY KEY,
    cd_sit VARCHAR(50),
    nm_situacao VARCHAR(100)
);

-- Tabela de Dimensão: Tipo
CREATE TABLE dim_tipo (
    id_tipo INT IDENTITY(1,1) PRIMARY KEY,
    cd_tipo VARCHAR(50),
    nm_tipo VARCHAR(100)
);

-- Tabela de Dimensão: Bairro
CREATE TABLE dim_bairro (
    id_bairro INT IDENTITY(1,1) PRIMARY KEY,
    cd_bairro VARCHAR(50),
    nm_bairro VARCHAR(100)
);

-- Tabela de Dimensão: Conurbação
CREATE TABLE dim_concurb (
    id_concurb INT IDENTITY(1,1) PRIMARY KEY,
    cd_concurb VARCHAR(50),
    nm_concurb VARCHAR(100)
);

-- Tabela Fato: Setores Censitários
CREATE TABLE fato_setores_censitarios (
    id_fato_setor INT IDENTITY(1,1) PRIMARY KEY,
    cd_setor VARCHAR(50),
    area_km2 FLOAT,
    geometry_wkt VARCHAR(MAX),
    id_localizacao_fk INT FOREIGN KEY REFERENCES dim_localizacao(id_localizacao),
    id_situacao_fk INT FOREIGN KEY REFERENCES dim_situacao(id_situacao),
    id_tipo_fk INT FOREIGN KEY REFERENCES dim_tipo(id_tipo),
    id_bairro_fk INT FOREIGN KEY REFERENCES dim_bairro(id_bairro),
    id_concurb_fk INT FOREIGN KEY REFERENCES dim_concurb(id_concurb),
    geometry GEOMETRY
);

-- Tabela Não Normalizada de Referência
CREATE TABLE cenario_nao_normalizado (
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
    geometry_wkt VARCHAR(MAX),
    geometry GEOMETRY
);




PRINT '--- Início do Processo Completo e Definitivo para [cenario_nao_normalizado] ---';
GO

-- PASSO 1: Garantir que a coluna da chave primária seja NOT NULL.
PRINT '1. Alterando a coluna CD_SETOR para NOT NULL...';
-- Ajuste o tipo de dado (VARCHAR(15)) para corresponder exatamente ao da sua tabela.
ALTER TABLE dbo.cenario_nao_normalizado
ALTER COLUMN CD_SETOR VARCHAR(15) NOT NULL;
PRINT '   Coluna alterada com sucesso.';
GO


-- PASSO 2: Adicionar a Chave Primária Clusterizada.
PRINT '2. Adicionando Chave Primária Clusterizada...';
IF NOT EXISTS (SELECT * FROM sys.key_constraints WHERE type = 'PK' AND parent_object_id = OBJECT_ID('dbo.cenario_nao_normalizado'))
BEGIN
    ALTER TABLE dbo.cenario_nao_normalizado
    ADD CONSTRAINT PK_cenario_nao_normalizado
    PRIMARY KEY CLUSTERED (CD_SETOR);
    PRINT '   Chave Primária [PK_cenario_nao_normalizado] criada.';
END
ELSE
BEGIN
    PRINT '   Chave Primária já existe.';
END
GO


-- PASSO 3: Converter o WKT para o tipo GEOMETRY.
PRINT '3. Convertendo dados de WKT para GEOMETRY...';
UPDATE dbo.cenario_nao_normalizado
SET geometry = GEOMETRY::STGeomFromText(geometry_wkt, 4326)
WHERE geometry_wkt IS NOT NULL AND geometry IS NULL;
PRINT '   Conversão concluída.';
GO


-- PASSO 4: Remover a coluna de texto WKT que não é mais necessária.
PRINT '4. Removendo a coluna [geometry_wkt]...';
IF COL_LENGTH('dbo.cenario_nao_normalizado', 'geometry_wkt') IS NOT NULL
BEGIN
    ALTER TABLE dbo.cenario_nao_normalizado
    DROP COLUMN geometry_wkt;
    PRINT '   Coluna removida com sucesso.';
END
ELSE
BEGIN
    PRINT '   Coluna [geometry_wkt] não encontrada ou já removida.';
END
GO


-- PASSO 5: Criar o Índice Espacial.
PRINT '5. Criando o Índice Espacial com SQL Dinâmico...';

DECLARE @xmin_desn FLOAT, @ymin_desn FLOAT, @xmax_desn FLOAT, @ymax_desn FLOAT;
DECLARE @envelope_desn GEOMETRY;
DECLARE @sqlCommand_desn NVARCHAR(MAX);

SELECT @envelope_desn = GEOMETRY::CollectionAggregate(geometry).STEnvelope() FROM dbo.cenario_nao_normalizado;

SELECT
    @xmin_desn = @envelope_desn.STPointN(1).STX,
    @ymin_desn = @envelope_desn.STPointN(1).STY,
    @xmax_desn = @envelope_desn.STPointN(3).STX,
    @ymax_desn = @envelope_desn.STPointN(3).STY;

SET @sqlCommand_desn =
    N'CREATE SPATIAL INDEX si_cenario_nao_normalizado_geom ON dbo.cenario_nao_normalizado(geometry) ' +
    N'WITH ( BOUNDING_BOX = ( ' +
    N'XMIN = ' + CAST(@xmin_desn AS NVARCHAR(30)) + N', ' +
    N'YMIN = ' + CAST(@ymin_desn AS NVARCHAR(30)) + N', ' +
    N'XMAX = ' + CAST(@xmax_desn AS NVARCHAR(30)) + N', ' +
    N'YMAX = ' + CAST(@ymax_desn AS NVARCHAR(30)) + N') );';

EXEC sp_executesql @sqlCommand_desn;
PRINT '   Índice Espacial [si_cenario_nao_normalizado_geom] criado com sucesso!';
GO




PRINT '--- Processo para [fato_setores_censitarios] concluído. ---';


PRINT '--- Início do Processo Completo e Definitivo para [fato_setores_censitarios] ---';
GO

-- PASSO 1: Garantir que a coluna da chave primária seja NOT NULL.
PRINT '1. Alterando a coluna CD_SETOR para NOT NULL...';
-- Ajuste o tipo de dado (VARCHAR(15)) para corresponder exatamente ao da sua tabela.
ALTER TABLE dbo.fato_setores_censitarios
ALTER COLUMN CD_SETOR VARCHAR(15) NOT NULL;
PRINT '   Coluna alterada com sucesso.';
GO


-- PASSO 2: Adicionar a Chave Primária Clusterizada.
PRINT '2. Adicionando Chave Primária Clusterizada...';
IF NOT EXISTS (SELECT * FROM sys.key_constraints WHERE type = 'PK' AND parent_object_id = OBJECT_ID('dbo.fato_setores_censitarios'))
BEGIN
    ALTER TABLE dbo.fato_setores_censitarios
    ADD CONSTRAINT PK_fato_setores_censitarios
    PRIMARY KEY CLUSTERED (CD_SETOR);
    PRINT '   Chave Primária [PK_fato_setores_censitarios] criada.';
END
ELSE
BEGIN
    PRINT '   Chave Primária já existe.';
END
GO


-- PASSO 3: Converter o WKT para o tipo GEOMETRY.
PRINT '3. Convertendo dados de WKT para GEOMETRY...';
UPDATE dbo.fato_setores_censitarios
SET geometry = GEOMETRY::STGeomFromText(geometry_wkt, 4326)
WHERE geometry_wkt IS NOT NULL AND geometry IS NULL;
PRINT '   Conversão concluída.';
GO


-- PASSO 4: Remover a coluna de texto WKT que não é mais necessária.
PRINT '4. Removendo a coluna [geometry_wkt]...';
IF COL_LENGTH('dbo.fato_setores_censitarios', 'geometry_wkt') IS NOT NULL
BEGIN
    ALTER TABLE dbo.fato_setores_censitarios
    DROP COLUMN geometry_wkt;
    PRINT '   Coluna removida com sucesso.';
END
ELSE
BEGIN
    PRINT '   Coluna [geometry_wkt] não encontrada ou já removida.';
END
GO


-- PASSO 5: Criar o Índice Espacial.
PRINT '5. Criando o Índice Espacial com SQL Dinâmico...';

DECLARE @xmin_desn FLOAT, @ymin_desn FLOAT, @xmax_desn FLOAT, @ymax_desn FLOAT;
DECLARE @envelope_desn GEOMETRY;
DECLARE @sqlCommand_desn NVARCHAR(MAX);

SELECT @envelope_desn = GEOMETRY::CollectionAggregate(geometry).STEnvelope() FROM dbo.fato_setores_censitarios;

SELECT
    @xmin_desn = @envelope_desn.STPointN(1).STX,
    @ymin_desn = @envelope_desn.STPointN(1).STY,
    @xmax_desn = @envelope_desn.STPointN(3).STX,
    @ymax_desn = @envelope_desn.STPointN(3).STY;

SET @sqlCommand_desn =
    N'CREATE SPATIAL INDEX si_fato_setores_censitarios_geom ON dbo.fato_setores_censitarios(geometry) ' +
    N'WITH ( BOUNDING_BOX = ( ' +
    N'XMIN = ' + CAST(@xmin_desn AS NVARCHAR(30)) + N', ' +
    N'YMIN = ' + CAST(@ymin_desn AS NVARCHAR(30)) + N', ' +
    N'XMAX = ' + CAST(@xmax_desn AS NVARCHAR(30)) + N', ' +
    N'YMAX = ' + CAST(@ymax_desn AS NVARCHAR(30)) + N') );';

EXEC sp_executesql @sqlCommand_desn;
PRINT '   Índice Espacial [si_fato_setores_censitarios_geom] criado com sucesso!';
GO

PRINT '--- Processo para [fato_setores_censitarios] concluído. ---';