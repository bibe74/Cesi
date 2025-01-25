USE CesiDW;
GO

/*
SET NOEXEC OFF;
--*/ SET NOEXEC ON;
GO

/*
    SCHEMA_NAME > COMETA
    TABLE_NAME > Articolo
    STAGING_TABLE_NAME > Articolo
*/

/**
 * @table Staging.ArticoloCategoriaMaster
 * @description Mappatura Articolo / Categoria Master
*/

CREATE OR ALTER VIEW Staging.ArticoloCategoriaMasterView
AS
SELECT
    T.id_articolo,
    T.codice AS Codice,
    COALESCE(T.descrizione, N'') AS Descrizione,
    CASE
        WHEN COALESCE(T.descrizione, N'') LIKE N'%Master MySolution On-line%' THEN N'Master MySolution'
        WHEN COALESCE(T.descrizione, N'') LIKE N'%Master MySolution Plus%' THEN N'Master MySolution'
        WHEN COALESCE(T.descrizione, N'') LIKE N'%Mini Master Revisione Legale%' THEN N'Mini Master Revisione'
        WHEN COALESCE(T.descrizione, N'') LIKE N'%Master MySolution 202%' THEN N'Master MySolution'
        ELSE N''
    END AS CategoriaMaster,
    CASE
        WHEN COALESCE(T.descrizione, N'') LIKE N'%Master MySolution On-line%' OR COALESCE(T.descrizione, N'') LIKE N'%Master MySolution Plus%' OR COALESCE(T.descrizione, N'') LIKE N'%Mini Master Revisione Legale%' OR COALESCE(T.descrizione, N'') LIKE N'%Master MySolution 202%'
        THEN
        CASE
            WHEN COALESCE(T.descrizione, N'') LIKE N'%2012_2013%' THEN N'2012/2013'
            WHEN COALESCE(T.descrizione, N'') LIKE N'%12_13%' THEN N'2012/2013'
            WHEN COALESCE(T.descrizione, N'') LIKE N'%2012%' THEN N'2012/2013'
            WHEN COALESCE(T.descrizione, N'') LIKE N'%2013_2014%' THEN N'2013/2014'
            WHEN COALESCE(T.descrizione, N'') LIKE N'%13_14%' THEN N'2013/2014'
            WHEN COALESCE(T.descrizione, N'') LIKE N'%2013%' THEN N'2013/2014'
            WHEN COALESCE(T.descrizione, N'') LIKE N'%2014_2015%' THEN N'2014/2015'
            WHEN COALESCE(T.descrizione, N'') LIKE N'%14_15%' THEN N'2014/2015'
            WHEN COALESCE(T.descrizione, N'') LIKE N'%2014%' THEN N'2014/2015'
            WHEN COALESCE(T.descrizione, N'') LIKE N'%2015_2016%' THEN N'2015/2016'
            WHEN COALESCE(T.descrizione, N'') LIKE N'%15_16%' THEN N'2015/2016'
            WHEN COALESCE(T.descrizione, N'') LIKE N'%2015%' THEN N'2015/2016'
            WHEN COALESCE(T.descrizione, N'') LIKE N'%2016_2017%' THEN N'2016/2017'
            WHEN COALESCE(T.descrizione, N'') LIKE N'%16_17%' THEN N'2016/2017'
            WHEN COALESCE(T.descrizione, N'') LIKE N'%2016%' THEN N'2016/2017'
            WHEN COALESCE(T.descrizione, N'') LIKE N'%2017_2018%' THEN N'2017/2018'
            WHEN COALESCE(T.descrizione, N'') LIKE N'%17_18%' THEN N'2017/2018'
            WHEN COALESCE(T.descrizione, N'') LIKE N'%2017%' THEN N'2017/2018'
            WHEN COALESCE(T.descrizione, N'') LIKE N'%2018_2019%' THEN N'2018/2019'
            WHEN COALESCE(T.descrizione, N'') LIKE N'%18_19%' THEN N'2018/2019'
            WHEN COALESCE(T.descrizione, N'') LIKE N'%2018%' THEN N'2018/2019'
            WHEN COALESCE(T.descrizione, N'') LIKE N'%2019_2020%' THEN N'2019/2020'
            WHEN COALESCE(T.descrizione, N'') LIKE N'%19_20%' THEN N'2019/2020'
            WHEN COALESCE(T.descrizione, N'') LIKE N'%2019%' THEN N'2019/2020'
            WHEN COALESCE(T.descrizione, N'') LIKE N'%2020_2021%' THEN N'2020/2021'
            WHEN COALESCE(T.descrizione, N'') LIKE N'%20_21%' THEN N'2020/2021'
            WHEN COALESCE(T.descrizione, N'') LIKE N'%2020%' THEN N'2020/2021'
            WHEN COALESCE(T.descrizione, N'') LIKE N'%2021_2022%' THEN N'2021/2022'
            WHEN COALESCE(T.descrizione, N'') LIKE N'%21_22%' THEN N'2021/2022'
            WHEN COALESCE(T.descrizione, N'') LIKE N'%2021%' THEN N'2021/2022'
            WHEN COALESCE(T.descrizione, N'') LIKE N'%2022_2023%' THEN N'2022/2023'
            WHEN COALESCE(T.descrizione, N'') LIKE N'%22_23%' THEN N'2022/2023'
            WHEN COALESCE(T.descrizione, N'') LIKE N'%2022%' THEN N'2022/2023'
            WHEN COALESCE(T.descrizione, N'') LIKE N'%2023_2024%' THEN N'2023/2024'
            WHEN COALESCE(T.descrizione, N'') LIKE N'%23_24%' THEN N'2023/2024'
            WHEN COALESCE(T.descrizione, N'') LIKE N'%2023%' THEN N'2023/2024'
            WHEN COALESCE(T.descrizione, N'') LIKE N'%2024_2025%' THEN N'2024/2025'
            WHEN COALESCE(T.descrizione, N'') LIKE N'%24_25%' THEN N'2024/2025'
            WHEN COALESCE(T.descrizione, N'') LIKE N'%2024%' THEN N'2024/2025'
            ELSE N''
        END
        ELSE N''
    END AS CodiceEsercizioMaster,
    CAST(1.0 AS DECIMAL(5,2)) AS Percentuale

FROM Landing.COMETA_Articolo T
WHERE COALESCE(T.descrizione, N'') LIKE N'%Master MySolution On-line%'
    OR COALESCE(T.descrizione, N'') LIKE N'%Master MySolution Plus%'
    OR COALESCE(T.descrizione, N'') LIKE N'%Mini Master Revisione Legale%'
    OR COALESCE(T.descrizione, N'') LIKE N'%Master MySolution%'
    OR COALESCE(T.descrizione, N'') LIKE N'%Master MySolution 202%';
GO

--DROP TABLE IF EXISTS Staging.ArticoloCategoriaMaster;
GO

IF OBJECT_ID('Staging.ArticoloCategoriaMaster', 'U') IS NULL
BEGIN

    SELECT TOP (0) * INTO Staging.ArticoloCategoriaMaster FROM Staging.ArticoloCategoriaMasterView;

    ALTER TABLE Staging.ArticoloCategoriaMaster ADD CONSTRAINT PK_Staging_ArticoloCategoriaMaster PRIMARY KEY CLUSTERED (id_articolo, CategoriaMaster);

    ALTER TABLE Staging.ArticoloCategoriaMaster ALTER COLUMN CategoriaMaster NVARCHAR(40) NOT NULL;
    ALTER TABLE Staging.ArticoloCategoriaMaster ALTER COLUMN CodiceEsercizioMaster NVARCHAR(10) NOT NULL;
    ALTER TABLE Staging.ArticoloCategoriaMaster ALTER COLUMN Percentuale DECIMAL(5,2) NOT NULL;

END;
GO

CREATE OR ALTER PROCEDURE Staging.usp_Reload_ArticoloCategoriaMaster
AS
BEGIN

    SET NOCOUNT ON;

    TRUNCATE TABLE Staging.ArticoloCategoriaMaster;

    INSERT INTO Staging.ArticoloCategoriaMaster SELECT * FROM Staging.ArticoloCategoriaMasterView;

    DELETE FROM Staging.ArticoloCategoriaMaster WHERE Codice IN (N'MS2010', N'MS2011');

    INSERT INTO Staging.ArticoloCategoriaMaster (
        id_articolo,
        Codice,
        Descrizione,
        CategoriaMaster,
        CodiceEsercizioMaster,
        Percentuale
    )
    SELECT
        A.id_articolo,
        A.Codice,
        A.Descrizione,
        CEM.CategoriaMaster,
        CEM.CodiceEsercizionMaster,
        CASE A.Codice
          WHEN N'MS2010' THEN CASE WHEN CEM.CategoriaMaster = N'Master MySolution' THEN .75 ELSE .25 END
          WHEN N'MS2011' THEN CASE WHEN CEM.CategoriaMaster = N'Master MySolution' THEN .80 ELSE .20 END
          ELSE NULL
        END

    FROM Dim.Articolo A
    CROSS JOIN (
        SELECT
            N'Master MySolution' AS CategoriaMaster,
            N'2022/2023' AS CodiceEsercizionMaster

        UNION ALL SELECT N'Mini Master Revisione', N'2022/2023'
    ) CEM
    WHERE A.Codice IN (N'MS2010', N'MS2011');

    -- Richiesta del 14/6/2023
    UPDATE Staging.ArticoloCategoriaMaster SET CodiceEsercizioMaster = N'2022/2023' WHERE Codice = N'FO2286';

END;
GO

--EXEC Staging.usp_Reload_ArticoloCategoriaMaster;
GO

/**
 * @table Staging.Articolo
 * @description

 * @depends Landing.COMETA_Articolo

SELECT TOP 1 * FROM Landing.COMETA_Articolo;
*/

--DROP TABLE IF EXISTS Staging.Articolo; DELETE FROM audit.tables WHERE provider_name = N'MyDatamartReporting' AND full_table_name = N'Landing.COMETA_Articolo';
GO

IF NOT EXISTS (SELECT 1 FROM audit.tables WHERE provider_name = N'MyDatamartReporting' AND full_table_name = N'Landing.COMETA_Articolo')
BEGIN

    INSERT INTO audit.tables (
        provider_name,
        full_table_name,
        staging_table_name,
        datawarehouse_table_name,
        lastupdated_staging,
        lastupdated_local
    )
    VALUES
    (   N'MyDatamartReporting',       -- provider_name - nvarchar(60)
        N'Landing.COMETA_Articolo',      -- full_table_name - sysname
        N'Staging.Articolo',      -- staging_table_name - sysname
        N'Dim.Articolo',      -- datawarehouse_table_name - sysname
        NULL, -- lastupdated_staging - datetime
        NULL  -- lastupdated_local - datetime
    );

END;
GO

IF OBJECT_ID(N'Staging.ArticoloView', N'V') IS NULL EXEC('CREATE VIEW Staging.ArticoloView AS SELECT 1 AS fld;');
GO

ALTER VIEW Staging.ArticoloView
AS
WITH ArticoloCategoriaMasterDettaglio
AS (
    SELECT
        ACM.id_articolo,
        ACM.CategoriaMaster,
        ACM.CodiceEsercizioMaster,
        ROW_NUMBER() OVER (PARTITION BY ACM.id_articolo ORDER BY ACM.Percentuale DESC) AS rn

    FROM Staging.ArticoloCategoriaMaster ACM
),
DatiArticolo
AS (
    SELECT
        T.id_articolo,
        T.codice AS Codice,
        COALESCE(T.descrizione, N'') AS Descrizione,
        --T.id_cat_com_articolo,
        COALESCE(CCA.codice, CASE WHEN COALESCE(T.id_cat_com_articolo, 0) = 0 THEN N'' ELSE N'???' END) AS CodiceCategoriaCommerciale,
        COALESCE(CCA.descrizione, CASE WHEN COALESCE(T.id_cat_com_articolo, 0) = 0 THEN N'' ELSE N'???' END) AS CategoriaCommerciale,
        --T.id_cat_merceologica,
        COALESCE(CM.codice, CASE WHEN COALESCE(T.id_cat_merceologica, 0) = 0 THEN N'' ELSE N'???' END) AS CodiceCategoriaMerceologica,
        COALESCE(CM.descrizione, CASE WHEN COALESCE(T.id_cat_merceologica, 0) = 0 THEN N'' ELSE N'???' END) AS CategoriaMerceologica,
        COALESCE(T.des_breve, N'') AS DescrizioneBreve,
        COALESCE(ACMD.CategoriaMaster, N'') AS CategoriaMaster,
        COALESCE(ACMD.CodiceEsercizioMaster, N'') AS CodiceEsercizioMaster,
        COALESCE(MST.tipo, N'') AS Tipo

    FROM Landing.COMETA_Articolo T
    LEFT JOIN Landing.COMETA_CategoriaCommercialeArticolo CCA ON CCA.id_cat_com_articolo = T.id_cat_com_articolo
    LEFT JOIN Landing.COMETA_CategoriaMerceologica CM ON CM.id_cat_merceologica = T.id_cat_merceologica
    LEFT JOIN ArticoloCategoriaMasterDettaglio ACMD ON ACMD.id_articolo = T.id_articolo
        AND ACMD.rn = 1
    LEFT JOIN Landing.COMETA_MySolutionTrascodifica MST ON MST.codice = T.codice
),
TableData
AS (
    SELECT
        DA.id_articolo,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            DA.id_articolo,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            DA.Codice,
            DA.Descrizione,
            DA.CodiceCategoriaCommerciale,
            DA.CategoriaCommerciale,
            DA.CodiceCategoriaMerceologica,
            DA.CategoriaMerceologica,
            DA.DescrizioneBreve,
            DA.CategoriaMaster,
            DA.CodiceEsercizioMaster,
            DA.Tipo,
            ABID.Data1,
            ABID.Data2,
            ABID.Data3,
            ABID.Data4,
            ABID.Data5,
            ABID.Data6,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        DA.Codice,
        DA.Descrizione,
        DA.CodiceCategoriaCommerciale,
        DA.CategoriaCommerciale,
        DA.CodiceCategoriaMerceologica,
        DA.CategoriaMerceologica,
        DA.DescrizioneBreve,
        DA.CategoriaMaster,
        DA.CodiceEsercizioMaster,
        CASE RIGHT(DA.Codice, 2)
          WHEN N'A1' THEN N'Annuale'
          WHEN N'A2' THEN N'Biennale'
          WHEN N'A3' THEN N'Triennale'
          WHEN N'A4' THEN N'Quadriennale'
          WHEN N'A5' THEN N'Quinquennale'
          WHEN N'A6' THEN N'6 anni'
          ELSE CASE RIGHT(DA.Codice, 3)
              WHEN N'A1F' THEN N'Annuale'
              WHEN N'A2F' THEN N'Biennale'
              WHEN N'A3F' THEN N'Triennale'
              WHEN N'A4F' THEN N'Quadriennale'
              WHEN N'A5F' THEN N'Quinquennale'
              WHEN N'A6F' THEN N'6 anni'
              ELSE N''
            END
        END AS Fatturazione,
        DA.Tipo,
        COALESCE(ABID.Data1, N'') AS Data1,
        COALESCE(ABID.Data2, N'') AS Data2,
        COALESCE(ABID.Data3, N'') AS Data3,
        COALESCE(ABID.Data4, N'') AS Data4,
        COALESCE(ABID.Data5, N'') AS Data5,
        COALESCE(ABID.Data6, N'') AS Data6 

    FROM DatiArticolo DA
    LEFT JOIN Landing.COMETAINTEGRATION_ArticleBIData ABID ON ABID.ArticleID = DA.id_articolo
)
SELECT
    -- Chiavi
    TD.id_articolo,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Altri campi
    TD.Codice,
    TD.Descrizione,
    TD.CodiceCategoriaCommerciale,
    TD.CategoriaCommerciale,
    TD.CodiceCategoriaMerceologica,
    TD.CategoriaMerceologica,
    TD.DescrizioneBreve,
    TD.CategoriaMaster,
    TD.CodiceEsercizioMaster,
    TD.Fatturazione,
    TD.Tipo,
    TD.Data1,
    TD.Data2,
    TD.Data3,
    TD.Data4,
    TD.Data5,
    TD.Data6

FROM TableData TD;
GO

--IF OBJECT_ID(N'Staging.Articolo', N'U') IS NOT NULL DROP TABLE Staging.Articolo;
GO

IF OBJECT_ID(N'Staging.Articolo', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Staging.Articolo FROM Staging.ArticoloView;

    ALTER TABLE Staging.Articolo ADD CONSTRAINT PK_Landing_COMETA_Articolo PRIMARY KEY CLUSTERED (UpdateDatetime, id_articolo);

    ALTER TABLE Staging.Articolo ALTER COLUMN Descrizione NVARCHAR(80) NOT NULL;
    ALTER TABLE Staging.Articolo ALTER COLUMN CodiceCategoriaCommerciale NVARCHAR(10) NOT NULL;
    ALTER TABLE Staging.Articolo ALTER COLUMN CategoriaCommerciale NVARCHAR(40) NOT NULL;
    ALTER TABLE Staging.Articolo ALTER COLUMN CodiceCategoriaMerceologica NVARCHAR(10) NOT NULL;
    ALTER TABLE Staging.Articolo ALTER COLUMN CategoriaMerceologica NVARCHAR(40) NOT NULL;
    ALTER TABLE Staging.Articolo ALTER COLUMN DescrizioneBreve NVARCHAR(80) NOT NULL;
    ALTER TABLE Staging.Articolo ALTER COLUMN CategoriaMaster NVARCHAR(40) NOT NULL;
    ALTER TABLE Staging.Articolo ALTER COLUMN CodiceEsercizioMaster NVARCHAR(10) NOT NULL;
    ALTER TABLE Staging.Articolo ALTER COLUMN Fatturazione NVARCHAR(20) NOT NULL;
    ALTER TABLE Staging.Articolo ALTER COLUMN Tipo NVARCHAR(20) NOT NULL;
    ALTER TABLE Staging.Articolo ALTER COLUMN Data1 NVARCHAR(40) NOT NULL;
    ALTER TABLE Staging.Articolo ALTER COLUMN Data2 NVARCHAR(40) NOT NULL;
    ALTER TABLE Staging.Articolo ALTER COLUMN Data3 NVARCHAR(40) NOT NULL;
    ALTER TABLE Staging.Articolo ALTER COLUMN Data4 NVARCHAR(40) NOT NULL;
    ALTER TABLE Staging.Articolo ALTER COLUMN Data5 NVARCHAR(40) NOT NULL;
    ALTER TABLE Staging.Articolo ALTER COLUMN Data6 NVARCHAR(40) NOT NULL;

    CREATE UNIQUE NONCLUSTERED INDEX IX_COMETA_Articolo_BusinessKey ON Staging.Articolo (id_articolo);
END;
GO

IF OBJECT_ID(N'Staging.usp_Reload_Articolo', N'P') IS NULL EXEC('CREATE PROCEDURE Staging.usp_Reload_Articolo AS RETURN 0;');
GO

ALTER PROCEDURE Staging.usp_Reload_Articolo
AS
BEGIN

    SET NOCOUNT ON;

    DECLARE @lastupdated_staging DATETIME;
    DECLARE @provider_name NVARCHAR(60) = N'MyDatamartReporting';
    DECLARE @full_table_name sysname = N'Landing.COMETA_Articolo';

    SELECT TOP 1 @lastupdated_staging = lastupdated_staging
    FROM audit.tables
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    IF (@lastupdated_staging IS NULL) SET @lastupdated_staging = CAST('19000101' AS DATETIME);

    BEGIN TRANSACTION

    TRUNCATE TABLE Staging.Articolo;

    INSERT INTO Staging.Articolo
    SELECT * FROM Staging.ArticoloView
    --WHERE UpdateDatetime > @lastupdated_staging;

    SELECT @lastupdated_staging = MAX(UpdateDatetime) FROM Staging.Articolo;

    IF (@lastupdated_staging IS NOT NULL)
    BEGIN

    UPDATE audit.tables
    SET lastupdated_staging = @lastupdated_staging
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    END;

    COMMIT

END;
GO

EXEC Staging.usp_Reload_Articolo;
GO

--DROP TABLE IF EXISTS Fact.Documenti; DROP TABLE IF EXISTS Dim.Articolo; DROP SEQUENCE IF EXISTS dbo.seq_Dim_Articolo;
GO

IF OBJECT_ID('dbo.seq_Dim_Articolo', 'SO') IS NULL
BEGIN

    CREATE SEQUENCE dbo.seq_Dim_Articolo START WITH 1;

END;
GO

IF OBJECT_ID('Dim.Articolo', 'U') IS NULL
BEGIN

    CREATE TABLE Dim.Articolo (
        PKArticolo INT NOT NULL CONSTRAINT PK_Dim_Articolo PRIMARY KEY CLUSTERED CONSTRAINT DFT_Dim_Articolo_PKArticolo DEFAULT (NEXT VALUE FOR dbo.seq_Dim_Articolo),
        id_articolo INT NOT NULL,

	    HistoricalHashKey VARBINARY(20) NULL,
	    ChangeHashKey VARBINARY(20) NULL,
	    HistoricalHashKeyASCII VARCHAR(34) NULL,
	    ChangeHashKeyASCII VARCHAR(34) NULL,
	    InsertDatetime DATETIME NOT NULL,
	    UpdateDatetime DATETIME NOT NULL,
	    IsDeleted BIT NOT NULL,

        Codice NVARCHAR(80) NOT NULL,
        Descrizione NVARCHAR(80) NOT NULL,
        CodiceCategoriaCommerciale NVARCHAR(10) NOT NULL,
        CategoriaCommerciale NVARCHAR(40) NOT NULL,
        CodiceCategoriaMerceologica NVARCHAR(10) NOT NULL,
        CategoriaMerceologica NVARCHAR(40) NOT NULL,
        DescrizioneBreve NVARCHAR(80) NOT NULL,
        CodiceEsercizioMaster NVARCHAR(10) NOT NULL,
        CategoriaMaster NVARCHAR(40) NOT NULL,
        Fatturazione NVARCHAR(20) NOT NULL,
        Tipo NVARCHAR(20) NOT NULL,
        Data1 NVARCHAR(40) NOT NULL,
        Data2 NVARCHAR(40) NOT NULL,
        Data3 NVARCHAR(40) NOT NULL,
        Data4 NVARCHAR(40) NOT NULL,
        Data5 NVARCHAR(40) NOT NULL,
        Data6 NVARCHAR(40) NOT NULL
    );

    CREATE UNIQUE NONCLUSTERED INDEX IX_Dim_Articolo_id_articolo ON Dim.Articolo (id_articolo);
    
    ALTER TABLE Dim.Articolo ADD CONSTRAINT DFT_Dim_Articolo_InsertDatetime DEFAULT (CURRENT_TIMESTAMP) FOR InsertDatetime;
    ALTER TABLE Dim.Articolo ADD CONSTRAINT DFT_Dim_Articolo_UpdateDatetime DEFAULT (CURRENT_TIMESTAMP) FOR UpdateDatetime;
    ALTER TABLE Dim.Articolo ADD CONSTRAINT DFT_Dim_Articolo_IsDeleted DEFAULT (0) FOR IsDeleted;

    INSERT INTO Dim.Articolo (
        PKArticolo,
        id_articolo,
        Codice,
        Descrizione,
        CodiceCategoriaCommerciale,
        CategoriaCommerciale,
        CodiceCategoriaMerceologica,
        CategoriaMerceologica,
        DescrizioneBreve,
        CategoriaMaster,
        CodiceEsercizioMaster,
        Fatturazione,
        Tipo,
        Data1,
        Data2,
        Data3,
        Data4,
        Data5,
        Data6
    )
    VALUES
    (   -1,         -- PKArticolo - int
        -1,         -- id_articolo - int
        N'',       -- Codice - nvarchar(40)
        N'',       -- Descrizione - nvarchar(80)
        N'',       -- CodiceCategoriaCommerciale - nvarchar(10)
        N'',       -- CategoriaCommerciale - nvarchar(40)
        N'',       -- CodiceCategoriaMerceologica - nvarchar(10)
        N'',       -- CategoriaMerceologica - nvarchar(40)
        N'',       -- DescrizioneBreve - nvarchar(80)
        N'',       -- CategoriaMaster - nvarchar(40)
        N'',       -- CodiceEsercizioMaster - nvarchar(10)
        N'',       -- Fatturazione - nvarchar(20)
        N'',       -- Tipo - nvarchar(20)
        N'',       -- Data1 - nvarchar(40)
        N'',       -- Data2 - nvarchar(40)
        N'',       -- Data3 - nvarchar(40)
        N'',       -- Data4 - nvarchar(40)
        N'',       -- Data5 - nvarchar(40)
        N''        -- Data6 - nvarchar(40)
    ),
    (   -101,         -- PKArticolo - int
        -101,         -- id_articolo - int
        N'???',       -- Codice - nvarchar(40)
        N'<???>',       -- Descrizione - nvarchar(80)
        N'',       -- CodiceCategoriaCommerciale - nvarchar(10)
        N'',       -- CategoriaCommerciale - nvarchar(40)
        N'',       -- CodiceCategoriaMerceologica - nvarchar(10)
        N'',       -- CategoriaMerceologica - nvarchar(40)
        N'<???>',       -- DescrizioneBreve - nvarchar(80)
        N'',       -- CategoriaMaster - nvarchar(40)
        N'',       -- CodiceEsercizioMaster - nvarchar(10)
        N'',       -- Fatturazione - nvarchar(20)
        N'',       -- Tipo - nvarchar(20)
        N'',       -- Data1 - nvarchar(40)
        N'',       -- Data2 - nvarchar(40)
        N'',       -- Data3 - nvarchar(40)
        N'',       -- Data4 - nvarchar(40)
        N'',       -- Data5 - nvarchar(40)
        N''        -- Data6 - nvarchar(40)
    );

    ALTER SEQUENCE dbo.seq_Dim_Articolo RESTART WITH 1;

END;
GO

IF OBJECT_ID(N'Dim.usp_Merge_Articolo', N'P') IS NULL EXEC('CREATE PROCEDURE Dim.usp_Merge_Articolo AS RETURN 0;');
GO

ALTER PROCEDURE Dim.usp_Merge_Articolo
AS
BEGIN

    SET NOCOUNT ON;

    BEGIN TRANSACTION 

    DECLARE @provider_name NVARCHAR(60) = N'MyDatamartReporting';
    DECLARE @full_table_name sysname = N'Landing.COMETA_Articolo';

    MERGE INTO Dim.Articolo AS TGT
    USING Staging.Articolo (nolock) AS SRC
    ON SRC.id_articolo = TGT.id_articolo

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.IsDeleted = SRC.IsDeleted,
        
        TGT.Codice = SRC.Codice,
        TGT.Descrizione = SRC.Descrizione,
        TGT.CodiceCategoriaCommerciale = SRC.CodiceCategoriaCommerciale,
        TGT.CategoriaCommerciale = SRC.CategoriaCommerciale,
        TGT.CodiceCategoriaMerceologica = SRC.CodiceCategoriaMerceologica,
        TGT.CategoriaMerceologica = SRC.CategoriaMerceologica,
        TGT.DescrizioneBreve = SRC.DescrizioneBreve,
        TGT.CategoriaMaster = SRC.CategoriaMaster,
        TGT.CodiceEsercizioMaster = SRC.CodiceEsercizioMaster,
        TGT.Fatturazione = SRC.Fatturazione,
        TGT.Tipo = SRC.Tipo,
        TGT.Data1 = SRC.Data1,
        TGT.Data2 = SRC.Data2,
        TGT.Data3 = SRC.Data3,
        TGT.Data4 = SRC.Data4,
        TGT.Data5 = SRC.Data5,
        TGT.Data6 = SRC.Data6

    WHEN NOT MATCHED
      THEN INSERT (
        id_articolo,
        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
        Codice,
        Descrizione,
        CodiceCategoriaCommerciale,
        CategoriaCommerciale,
        CodiceCategoriaMerceologica,
        CategoriaMerceologica,
        DescrizioneBreve,
        CategoriaMaster,
        CodiceEsercizioMaster,
        Fatturazione,
        Tipo,
        Data1,
        Data2,
        Data3,
        Data4,
        Data5,
        Data6
      )
      VALUES (
        SRC.id_articolo,
        SRC.HistoricalHashKey,
        SRC.ChangeHashKey,
        SRC.HistoricalHashKeyASCII,
        SRC.ChangeHashKeyASCII,
        SRC.InsertDatetime,
        SRC.UpdateDatetime,
        SRC.IsDeleted,
        SRC.Codice,
        SRC.Descrizione,
        SRC.CodiceCategoriaCommerciale,
        SRC.CategoriaCommerciale,
        SRC.CodiceCategoriaMerceologica,
        SRC.CategoriaMerceologica,
        SRC.DescrizioneBreve,
        SRC.CategoriaMaster,
        SRC.CodiceEsercizioMaster,
        SRC.Fatturazione,
        SRC.Tipo,
        SRC.Data1,
        SRC.Data2,
        SRC.Data3,
        SRC.Data4,
        SRC.Data5,
        SRC.Data6
      )

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        $action AS merge_action,
        'Staging.Articolo' AS full_olap_table_name,
        'id_articolo = ' + CAST(COALESCE(inserted.id_articolo, deleted.id_articolo) AS NVARCHAR(1000)) AS primary_key_description
    INTO audit.merge_log_details;

    --DELETE FROM Dim.Articolo
    --WHERE IsDeleted = CAST(1 AS BIT);

    UPDATE audit.tables
    SET lastupdated_local = lastupdated_staging
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    COMMIT TRANSACTION;

END;
GO

EXEC Dim.usp_Merge_Articolo;
GO
