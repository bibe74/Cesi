USE CesiDW;
GO

/*
SET NOEXEC OFF;
--*/ SET NOEXEC ON;
GO

/*
    SCHEMA_NAME > COMETA
    TABLE_NAME > Anagrafica
*/

/**
 * @table Landing.COMETA_Anagrafica
 * @description 

 * @depends COMETA.Anagrafica

SELECT TOP 100 * FROM COMETA.Anagrafica;
*/

IF OBJECT_ID('Landing.COMETA_AnagraficaView', 'V') IS NULL EXEC('CREATE VIEW Landing.COMETA_AnagraficaView AS SELECT 1 AS fld;');
GO

ALTER VIEW Landing.COMETA_AnagraficaView
AS
WITH TableData
AS (
    SELECT
        id_anagrafica,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            id_anagrafica,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            rag_soc_1,
            rag_soc_2,
            indirizzo,
            cap,
            localita,
            provincia,
            nazione,
            cod_fiscale,
            par_iva,
            indirizzo2,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        rag_soc_1,
        rag_soc_2,
        indirizzo,
        cap,
        localita,
        provincia,
        nazione,
        cod_fiscale,
        par_iva,
        indirizzo2

    FROM COMETA.Anagrafica
)
SELECT
    -- Chiavi
    TD.id_anagrafica,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.rag_soc_1 COLLATE DATABASE_DEFAULT AS rag_soc_1,
    TD.rag_soc_2 COLLATE DATABASE_DEFAULT AS rag_soc_2,
    TD.indirizzo COLLATE DATABASE_DEFAULT AS indirizzo,
    TD.cap COLLATE DATABASE_DEFAULT AS cap,
    TD.localita COLLATE DATABASE_DEFAULT AS localita,
    TD.provincia COLLATE DATABASE_DEFAULT AS provincia,
    TD.nazione COLLATE DATABASE_DEFAULT AS nazione,
    TD.cod_fiscale COLLATE DATABASE_DEFAULT AS cod_fiscale,
    TD.par_iva COLLATE DATABASE_DEFAULT AS par_iva,
    TD.indirizzo2 COLLATE DATABASE_DEFAULT AS indirizzo2

FROM TableData TD;
GO

--DROP TABLE IF EXISTS Landing.COMETA_Anagrafica;
GO

IF OBJECT_ID(N'Landing.COMETA_Anagrafica', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Landing.COMETA_Anagrafica FROM Landing.COMETA_AnagraficaView;

    ALTER TABLE Landing.COMETA_Anagrafica ALTER COLUMN id_anagrafica INT NOT NULL;

    ALTER TABLE Landing.COMETA_Anagrafica ADD CONSTRAINT PK_Landing_COMETA_Anagrafica PRIMARY KEY CLUSTERED (UpdateDatetime, id_anagrafica);

    ALTER TABLE Landing.COMETA_Anagrafica ALTER COLUMN rag_soc_1 NVARCHAR(60) NULL;
    ALTER TABLE Landing.COMETA_Anagrafica ALTER COLUMN rag_soc_2 NVARCHAR(60) NULL;
    ALTER TABLE Landing.COMETA_Anagrafica ALTER COLUMN indirizzo NVARCHAR(60) NULL;
    ALTER TABLE Landing.COMETA_Anagrafica ALTER COLUMN cap NVARCHAR(10) NULL;
    ALTER TABLE Landing.COMETA_Anagrafica ALTER COLUMN localita NVARCHAR(60) NULL;
    ALTER TABLE Landing.COMETA_Anagrafica ALTER COLUMN provincia NVARCHAR(10) NULL;
    ALTER TABLE Landing.COMETA_Anagrafica ALTER COLUMN nazione NVARCHAR(60) NULL;
    ALTER TABLE Landing.COMETA_Anagrafica ALTER COLUMN cod_fiscale NVARCHAR(60) NULL;
    ALTER TABLE Landing.COMETA_Anagrafica ALTER COLUMN par_iva NVARCHAR(60) NULL;
    ALTER TABLE Landing.COMETA_Anagrafica ALTER COLUMN indirizzo2 NVARCHAR(60) NULL;

    CREATE UNIQUE NONCLUSTERED INDEX IX_COMETA_Anagrafica_BusinessKey ON Landing.COMETA_Anagrafica (id_anagrafica);
END;
GO

IF OBJECT_ID('COMETA.usp_Merge_Anagrafica', 'P') IS NULL EXEC('CREATE PROCEDURE COMETA.usp_Merge_Anagrafica AS RETURN 0;');
GO

ALTER PROCEDURE COMETA.usp_Merge_Anagrafica
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @IsCometaExportRunning BIT = 1;

    SELECT TOP (1) @IsCometaExportRunning = IsCometaExportRunning FROM COMETA.Semaforo;

    IF (COALESCE(@IsCometaExportRunning, 1) = 1) RETURN -1;

    MERGE INTO Landing.COMETA_Anagrafica AS TGT
    USING Landing.COMETA_AnagraficaView (nolock) AS SRC
    ON SRC.id_anagrafica = TGT.id_anagrafica

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.IsDeleted = SRC.IsDeleted,
        TGT.rag_soc_1 = SRC.rag_soc_1,
        TGT.rag_soc_2 = SRC.rag_soc_2,
        TGT.indirizzo = SRC.indirizzo,
        TGT.cap = SRC.cap,
        TGT.localita = SRC.localita,
        TGT.provincia = SRC.provincia,
        TGT.nazione = SRC.nazione,
        TGT.cod_fiscale = SRC.cod_fiscale,
        TGT.par_iva = SRC.par_iva,
        TGT.indirizzo2 = SRC.indirizzo2

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        id_anagrafica,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        rag_soc_1,
        rag_soc_2,
        indirizzo,
        cap,
        localita,
        provincia,
        nazione,
        cod_fiscale,
        par_iva,
        indirizzo2
      )

    WHEN NOT MATCHED BY SOURCE
        AND TGT.IsDeleted = CAST(0 AS BIT)
      THEN UPDATE
        SET TGT.IsDeleted = CAST(1 AS BIT),
        TGT.UpdateDatetime = CURRENT_TIMESTAMP,
        TGT.ChangeHashKey = CONVERT(VARBINARY(20), ''),
        TGT.ChangeHashKeyASCII = ''

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        CASE WHEN Inserted.IsDeleted = CAST(1 AS BIT) THEN N'DELETE' ELSE $action END AS merge_action,
        'Landing.COMETA_Anagrafica' AS full_olap_table_name,
        'id_anagrafica = ' + CAST(COALESCE(inserted.id_anagrafica, deleted.id_anagrafica) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;
GO

EXEC COMETA.usp_Merge_Anagrafica;
GO

/*
    SCHEMA_NAME > COMETA
    TABLE_NAME > Articolo
*/

/**
 * @table Landing.COMETA_Articolo
 * @description 

 * @depends COMETA.Articolo

SELECT TOP 100 * FROM COMETA.Articolo;
*/

IF OBJECT_ID('Landing.COMETA_ArticoloView', 'V') IS NULL EXEC('CREATE VIEW Landing.COMETA_ArticoloView AS SELECT 1 AS fld;');
GO

ALTER VIEW Landing.COMETA_ArticoloView
AS
WITH TableData
AS (
    SELECT
        id_articolo,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            id_articolo,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            codice,
            descrizione,
            des_breve,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        codice,
        descrizione,
        id_cat_com_articolo,
        id_cat_merceologica,
        des_breve

    FROM COMETA.Articolo
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

    -- Attributi
    TD.codice,
    TD.descrizione,
    TD.id_cat_com_articolo,
    TD.id_cat_merceologica,
    TD.des_breve

FROM TableData TD;
GO

--DROP TABLE IF EXISTS Landing.COMETA_Articolo;
GO

IF OBJECT_ID(N'Landing.COMETA_Articolo', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Landing.COMETA_Articolo FROM Landing.COMETA_ArticoloView;

    ALTER TABLE Landing.COMETA_Articolo ALTER COLUMN id_articolo INT NOT NULL;

    ALTER TABLE Landing.COMETA_Articolo ADD CONSTRAINT PK_Landing_COMETA_Articolo PRIMARY KEY CLUSTERED (UpdateDatetime, id_articolo);

    ALTER TABLE Landing.COMETA_Articolo ALTER COLUMN codice NVARCHAR(40) NOT NULL;
    ALTER TABLE Landing.COMETA_Articolo ALTER COLUMN descrizione NVARCHAR(80) NULL;
    ALTER TABLE Landing.COMETA_Articolo ALTER COLUMN id_cat_com_articolo INT NULL;
    ALTER TABLE Landing.COMETA_Articolo ALTER COLUMN id_cat_merceologica INT NULL;
    ALTER TABLE Landing.COMETA_Articolo ALTER COLUMN des_breve NVARCHAR(80) NULL;

    CREATE UNIQUE NONCLUSTERED INDEX IX_COMETA_Articolo_BusinessKey ON Landing.COMETA_Articolo (id_articolo);
END;
GO

IF OBJECT_ID('COMETA.usp_Merge_Articolo', 'P') IS NULL EXEC('CREATE PROCEDURE COMETA.usp_Merge_Articolo AS RETURN 0;');
GO

ALTER PROCEDURE COMETA.usp_Merge_Articolo
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @IsCometaExportRunning BIT = 1;

    SELECT TOP (1) @IsCometaExportRunning = IsCometaExportRunning FROM COMETA.Semaforo;

    IF (COALESCE(@IsCometaExportRunning, 1) = 1) RETURN -1;

    MERGE INTO Landing.COMETA_Articolo AS TGT
    USING Landing.COMETA_ArticoloView (nolock) AS SRC
    ON SRC.id_articolo = TGT.id_articolo

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.codice = SRC.codice,
        TGT.descrizione = SRC.descrizione,
        TGT.id_cat_com_articolo = SRC.id_cat_com_articolo,
        TGT.id_cat_merceologica = SRC.id_cat_merceologica,
        TGT.des_breve = SRC.des_breve

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        id_Articolo,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        codice,
        descrizione,
        id_cat_com_articolo,
        id_cat_merceologica,
        des_breve
      )

    WHEN NOT MATCHED BY SOURCE
        AND TGT.IsDeleted = CAST(0 AS BIT)
      THEN UPDATE
        SET TGT.IsDeleted = CAST(1 AS BIT),
        TGT.UpdateDatetime = CURRENT_TIMESTAMP,
        TGT.ChangeHashKey = CONVERT(VARBINARY(20), ''),
        TGT.ChangeHashKeyASCII = ''

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        CASE WHEN Inserted.IsDeleted = CAST(1 AS BIT) THEN N'DELETE' ELSE $action END AS merge_action,
        'Landing.COMETA_Articolo' AS full_olap_table_name,
        'id_articolo = ' + CAST(COALESCE(inserted.id_articolo, deleted.id_articolo) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;
GO

EXEC COMETA.usp_Merge_Articolo;
GO

/*
    SCHEMA_NAME > COMETA
    TABLE_NAME > CategoriaCommercialeArticolo
*/

/**
 * @table Landing.COMETA_CategoriaCommercialeArticolo
 * @description 

 * @depends COMETA.CategoriaCommercialeArticolo

SELECT TOP 100 * FROM COMETA.CategoriaCommercialeArticolo;
*/

IF OBJECT_ID('Landing.COMETA_CategoriaCommercialeArticoloView', 'V') IS NULL EXEC('CREATE VIEW Landing.COMETA_CategoriaCommercialeArticoloView AS SELECT 1 AS fld;');
GO

ALTER VIEW Landing.COMETA_CategoriaCommercialeArticoloView
AS
WITH TableData
AS (
    SELECT
        id_cat_com_articolo,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            id_cat_com_articolo,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            codice,
            descrizione,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        codice,
        descrizione

    FROM COMETA.CategoriaCommercialeArticolo
)
SELECT
    -- Chiavi
    TD.id_cat_com_articolo,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.codice,
    TD.descrizione

FROM TableData TD;
GO

--DROP TABLE IF EXISTS Landing.COMETA_CategoriaCommercialeArticolo;
GO

IF OBJECT_ID(N'Landing.COMETA_CategoriaCommercialeArticolo', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Landing.COMETA_CategoriaCommercialeArticolo FROM Landing.COMETA_CategoriaCommercialeArticoloView;

    ALTER TABLE Landing.COMETA_CategoriaCommercialeArticolo ALTER COLUMN id_cat_com_articolo INT NOT NULL;

    ALTER TABLE Landing.COMETA_CategoriaCommercialeArticolo ADD CONSTRAINT PK_Landing_COMETA_CategoriaCommercialeArticolo PRIMARY KEY CLUSTERED (UpdateDatetime, id_cat_com_articolo);

    ALTER TABLE Landing.COMETA_CategoriaCommercialeArticolo ALTER COLUMN codice NVARCHAR(10) NOT NULL;
    ALTER TABLE Landing.COMETA_CategoriaCommercialeArticolo ALTER COLUMN descrizione NVARCHAR(40) NOT NULL;

    CREATE UNIQUE NONCLUSTERED INDEX IX_COMETA_CategoriaCommercialeArticolo_BusinessKey ON Landing.COMETA_CategoriaCommercialeArticolo (id_cat_com_articolo);
END;
GO

IF OBJECT_ID('COMETA.usp_Merge_CategoriaCommercialeArticolo', 'P') IS NULL EXEC('CREATE PROCEDURE COMETA.usp_Merge_CategoriaCommercialeArticolo AS RETURN 0;');
GO

ALTER PROCEDURE COMETA.usp_Merge_CategoriaCommercialeArticolo
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @IsCometaExportRunning BIT = 1;

    SELECT TOP (1) @IsCometaExportRunning = IsCometaExportRunning FROM COMETA.Semaforo;

    IF (COALESCE(@IsCometaExportRunning, 1) = 1) RETURN -1;

    MERGE INTO Landing.COMETA_CategoriaCommercialeArticolo AS TGT
    USING Landing.COMETA_CategoriaCommercialeArticoloView (nolock) AS SRC
    ON SRC.id_cat_com_articolo = TGT.id_cat_com_articolo

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.codice = SRC.codice,
        TGT.descrizione = SRC.descrizione

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        id_cat_com_articolo,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        codice,
        descrizione
      )

    WHEN NOT MATCHED BY SOURCE
        AND TGT.IsDeleted = CAST(0 AS BIT)
      THEN UPDATE
        SET TGT.IsDeleted = CAST(1 AS BIT),
        TGT.UpdateDatetime = CURRENT_TIMESTAMP,
        TGT.ChangeHashKey = CONVERT(VARBINARY(20), ''),
        TGT.ChangeHashKeyASCII = ''

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        CASE WHEN Inserted.IsDeleted = CAST(1 AS BIT) THEN N'DELETE' ELSE $action END AS merge_action,
        'Landing.COMETA_CategoriaCommercialeArticolo' AS full_olap_table_name,
        'id_cat_com_articolo = ' + CAST(COALESCE(inserted.id_cat_com_articolo, deleted.id_cat_com_articolo) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;
GO

EXEC COMETA.usp_Merge_CategoriaCommercialeArticolo;
GO

/*
    SCHEMA_NAME > COMETA
    TABLE_NAME > CategoriaMerceologica
*/

/**
 * @table Landing.COMETA_CategoriaMerceologica
 * @description 

 * @depends COMETA.CategoriaMerceologica

SELECT TOP 100 * FROM COMETA.CategoriaMerceologica;
*/

IF OBJECT_ID('Landing.COMETA_CategoriaMerceologicaView', 'V') IS NULL EXEC('CREATE VIEW Landing.COMETA_CategoriaMerceologicaView AS SELECT 1 AS fld;');
GO

ALTER VIEW Landing.COMETA_CategoriaMerceologicaView
AS
WITH TableData
AS (
    SELECT
        id_cat_merceologica,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            id_cat_merceologica,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            codice,
            descrizione,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        codice,
        descrizione

    FROM COMETA.CategoriaMerceologica
)
SELECT
    -- Chiavi
    TD.id_cat_merceologica,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.codice,
    TD.descrizione

FROM TableData TD;
GO

--DROP TABLE IF EXISTS Landing.COMETA_CategoriaMerceologica;
GO

IF OBJECT_ID(N'Landing.COMETA_CategoriaMerceologica', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Landing.COMETA_CategoriaMerceologica FROM Landing.COMETA_CategoriaMerceologicaView;

    ALTER TABLE Landing.COMETA_CategoriaMerceologica ALTER COLUMN id_cat_merceologica INT NOT NULL;

    ALTER TABLE Landing.COMETA_CategoriaMerceologica ADD CONSTRAINT PK_Landing_COMETA_CategoriaMerceologica PRIMARY KEY CLUSTERED (UpdateDatetime, id_cat_merceologica);

    ALTER TABLE Landing.COMETA_CategoriaMerceologica ALTER COLUMN codice NVARCHAR(10) NOT NULL;
    ALTER TABLE Landing.COMETA_CategoriaMerceologica ALTER COLUMN descrizione NVARCHAR(40) NULL;

    CREATE UNIQUE NONCLUSTERED INDEX IX_COMETA_CategoriaMerceologica_BusinessKey ON Landing.COMETA_CategoriaMerceologica (id_cat_merceologica);
END;
GO

IF OBJECT_ID('COMETA.usp_Merge_CategoriaMerceologica', 'P') IS NULL EXEC('CREATE PROCEDURE COMETA.usp_Merge_CategoriaMerceologica AS RETURN 0;');
GO

ALTER PROCEDURE COMETA.usp_Merge_CategoriaMerceologica
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @IsCometaExportRunning BIT = 1;

    SELECT TOP (1) @IsCometaExportRunning = IsCometaExportRunning FROM COMETA.Semaforo;

    IF (COALESCE(@IsCometaExportRunning, 1) = 1) RETURN -1;

    MERGE INTO Landing.COMETA_CategoriaMerceologica AS TGT
    USING Landing.COMETA_CategoriaMerceologicaView (nolock) AS SRC
    ON SRC.id_cat_merceologica = TGT.id_cat_merceologica

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.codice = SRC.codice,
        TGT.descrizione = SRC.descrizione

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        id_cat_merceologica,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        codice,
        descrizione
      )

    WHEN NOT MATCHED BY SOURCE
        AND TGT.IsDeleted = CAST(0 AS BIT)
      THEN UPDATE
        SET TGT.IsDeleted = CAST(1 AS BIT),
        TGT.UpdateDatetime = CURRENT_TIMESTAMP,
        TGT.ChangeHashKey = CONVERT(VARBINARY(20), ''),
        TGT.ChangeHashKeyASCII = ''

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        CASE WHEN Inserted.IsDeleted = CAST(1 AS BIT) THEN N'DELETE' ELSE $action END AS merge_action,
        'Landing.COMETA_CategoriaMerceologica' AS full_olap_table_name,
        'id_cat_merceologica = ' + CAST(COALESCE(inserted.id_cat_merceologica, deleted.id_cat_merceologica) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;
GO

EXEC COMETA.usp_Merge_CategoriaMerceologica;
GO

/*
    SCHEMA_NAME > COMETA
    TABLE_NAME > CondizioniPagamento
*/

/**
 * @table Landing.COMETA_CondizioniPagamento
 * @description 

 * @depends COMETA.CondizioniPagamento

SELECT TOP 100 * FROM COMETA.CondizioniPagamento;
*/

IF OBJECT_ID('Landing.COMETA_CondizioniPagamentoView', 'V') IS NULL EXEC('CREATE VIEW Landing.COMETA_CondizioniPagamentoView AS SELECT 1 AS fld;');
GO

ALTER VIEW Landing.COMETA_CondizioniPagamentoView
AS
WITH TableData
AS (
    SELECT
        id_con_pagamento,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            id_con_pagamento,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            codice,
            descrizione,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        codice,
        descrizione

    FROM COMETA.CondizioniPagamento
)
SELECT
    -- Chiavi
    TD.id_con_pagamento,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.codice COLLATE DATABASE_DEFAULT AS Codice,
    TD.descrizione COLLATE DATABASE_DEFAULT AS Descrizione

FROM TableData TD;
GO

--DROP TABLE IF EXISTS Landing.COMETA_CondizioniPagamento;
GO

IF OBJECT_ID(N'Landing.COMETA_CondizioniPagamento', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Landing.COMETA_CondizioniPagamento FROM Landing.COMETA_CondizioniPagamentoView;

    ALTER TABLE Landing.COMETA_CondizioniPagamento ALTER COLUMN id_con_pagamento INT NOT NULL;

    ALTER TABLE Landing.COMETA_CondizioniPagamento ADD CONSTRAINT PK_Landing_COMETA_CondizioniPagamento PRIMARY KEY CLUSTERED (UpdateDatetime, id_con_pagamento);

    CREATE UNIQUE NONCLUSTERED INDEX IX_COMETA_CondizioniPagamento_BusinessKey ON Landing.COMETA_CondizioniPagamento (id_con_pagamento);
END;
GO

IF OBJECT_ID('COMETA.usp_Merge_CondizioniPagamento', 'P') IS NULL EXEC('CREATE PROCEDURE COMETA.usp_Merge_CondizioniPagamento AS RETURN 0;');
GO

ALTER PROCEDURE COMETA.usp_Merge_CondizioniPagamento
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @IsCometaExportRunning BIT = 1;

    SELECT TOP (1) @IsCometaExportRunning = IsCometaExportRunning FROM COMETA.Semaforo;

    IF (COALESCE(@IsCometaExportRunning, 1) = 1) RETURN -1;

    MERGE INTO Landing.COMETA_CondizioniPagamento AS TGT
    USING Landing.COMETA_CondizioniPagamentoView (nolock) AS SRC
    ON SRC.id_con_pagamento = TGT.id_con_pagamento

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.codice = SRC.codice,
        TGT.descrizione = SRC.descrizione

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        id_con_pagamento,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        codice,
        descrizione
      )

    WHEN NOT MATCHED BY SOURCE
        AND TGT.IsDeleted = CAST(0 AS BIT)
      THEN UPDATE
        SET TGT.IsDeleted = CAST(1 AS BIT),
        TGT.UpdateDatetime = CURRENT_TIMESTAMP,
        TGT.ChangeHashKey = CONVERT(VARBINARY(20), ''),
        TGT.ChangeHashKeyASCII = ''

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        CASE WHEN Inserted.IsDeleted = CAST(1 AS BIT) THEN N'DELETE' ELSE $action END AS merge_action,
        'Landing.COMETA_CondizioniPagamento' AS full_olap_table_name,
        'id_con_pagamento = ' + CAST(COALESCE(inserted.id_con_pagamento, deleted.id_con_pagamento) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;
GO

EXEC COMETA.usp_Merge_CondizioniPagamento;
GO

/*
    SCHEMA_NAME > COMETA
    TABLE_NAME > Documento
*/

/**
 * @table Landing.COMETA_Documento
 * @description 

 * @depends COMETA.Documento

SELECT TOP 100 * FROM COMETA.Documento;
*/

IF OBJECT_ID('Landing.COMETA_DocumentoView', 'V') IS NULL EXEC('CREATE VIEW Landing.COMETA_DocumentoView AS SELECT 1 AS fld;');
GO

ALTER VIEW Landing.COMETA_DocumentoView
AS
WITH TableData
AS (
    SELECT
        id_documento,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            id_documento,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            data_documento,
            num_documento,
            id_prof_documento,
            id_sog_commerciale,
            id_sog_commerciale_fattura,
            data_inizio_contratto,
            data_fine_contratto,
            id_gruppo_agenti,
            id_libero_1,
            id_libero_2,
            id_libero_3,
            libero_4,
            id_tipo_fatturazione,
            id_registro,
            data_competenza,
            data_registrazione,
            data_disdetta,
            motivo_disdetta,
            id_con_pagamento,
            rinnovo_automatico,
            note_intestazione,
            note_decisionali,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        id_prof_documento,
        id_registro,
        data_registrazione,
        num_documento,
        data_documento,
        data_competenza,
        id_sog_commerciale,
        id_sog_commerciale_fattura,
        id_gruppo_agenti,
        data_fine_contratto,
        libero_4,
        data_inizio_contratto,
        id_libero_1,
        id_libero_2,
        id_libero_3,
        id_tipo_fatturazione,
        data_disdetta,
        motivo_disdetta,
        id_con_pagamento,
        rinnovo_automatico,
        note_intestazione,
        note_decisionali

    FROM COMETA.Documento
    --WHERE id_prof_documento = 1 -- 1: Ordine
)
SELECT
    -- Chiavi
    TD.id_documento,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.id_prof_documento,
    TD.id_registro,
    TD.data_registrazione,
    TD.num_documento COLLATE DATABASE_DEFAULT AS num_documento,
    TD.data_documento,
    TD.data_competenza,
    TD.id_sog_commerciale,
    TD.id_sog_commerciale_fattura,
    TD.id_gruppo_agenti,
    TD.data_fine_contratto,
    TD.libero_4 COLLATE DATABASE_DEFAULT AS libero_4,
    TD.data_inizio_contratto,
    TD.id_libero_1,
    TD.id_libero_2,
    TD.id_libero_3,
    TD.id_tipo_fatturazione,
    TD.data_disdetta,
    TD.motivo_disdetta,
    TD.id_con_pagamento,
    TD.rinnovo_automatico COLLATE DATABASE_DEFAULT AS rinnovo_automatico,
    LEFT(TD.note_intestazione, 1000) COLLATE DATABASE_DEFAULT AS note_intestazione,
    LEFT(TD.note_decisionali, 1000) COLLATE DATABASE_DEFAULT AS note_decisionali

FROM TableData TD;
GO

--DROP TABLE IF EXISTS Landing.COMETA_Documento;
GO

IF OBJECT_ID(N'Landing.COMETA_Documento', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Landing.COMETA_Documento FROM Landing.COMETA_DocumentoView;

    ALTER TABLE Landing.COMETA_Documento ALTER COLUMN id_documento INT NOT NULL;

    ALTER TABLE Landing.COMETA_Documento ADD CONSTRAINT PK_Landing_COMETA_Documento PRIMARY KEY CLUSTERED (UpdateDatetime, id_documento);

    ALTER TABLE Landing.COMETA_Documento ALTER COLUMN id_prof_documento INT NULL;
    ALTER TABLE Landing.COMETA_Documento ALTER COLUMN id_registro INT NULL;
    ALTER TABLE Landing.COMETA_Documento ALTER COLUMN data_registrazione DATE NULL;
    ALTER TABLE Landing.COMETA_Documento ALTER COLUMN num_documento NVARCHAR(20) NULL;
    ALTER TABLE Landing.COMETA_Documento ALTER COLUMN data_documento DATE NULL;
    ALTER TABLE Landing.COMETA_Documento ALTER COLUMN data_competenza DATE NULL;
    ALTER TABLE Landing.COMETA_Documento ALTER COLUMN id_sog_commerciale INT NULL;
    ALTER TABLE Landing.COMETA_Documento ALTER COLUMN id_sog_commerciale_fattura INT NULL;
    ALTER TABLE Landing.COMETA_Documento ALTER COLUMN id_gruppo_agenti INT NULL;
    ALTER TABLE Landing.COMETA_Documento ALTER COLUMN data_fine_contratto DATE NULL;
    ALTER TABLE Landing.COMETA_Documento ALTER COLUMN libero_4 NVARCHAR(200) NULL;
    ALTER TABLE Landing.COMETA_Documento ALTER COLUMN data_inizio_contratto DATE NULL;
    ALTER TABLE Landing.COMETA_Documento ALTER COLUMN id_libero_1 INT NULL;
    ALTER TABLE Landing.COMETA_Documento ALTER COLUMN id_libero_2 INT NULL;
    ALTER TABLE Landing.COMETA_Documento ALTER COLUMN id_libero_3 INT NULL;
    ALTER TABLE Landing.COMETA_Documento ALTER COLUMN id_tipo_fatturazione INT NULL;
    ALTER TABLE Landing.COMETA_Documento ALTER COLUMN data_disdetta DATE NULL;
    ALTER TABLE Landing.COMETA_Documento ALTER COLUMN motivo_disdetta NVARCHAR(120) NULL;
    ALTER TABLE Landing.COMETA_Documento ALTER COLUMN rinnovo_automatico CHAR(1) NULL;
    ALTER TABLE Landing.COMETA_Documento ALTER COLUMN note_intestazione NVARCHAR(1000) NULL;
    ALTER TABLE Landing.COMETA_Documento ALTER COLUMN note_decisionali NVARCHAR(1000) NULL;

    CREATE UNIQUE NONCLUSTERED INDEX IX_COMETA_Documento_BusinessKey ON Landing.COMETA_Documento (id_documento);
END;
GO

IF OBJECT_ID('COMETA.usp_Merge_Documento', 'P') IS NULL EXEC('CREATE PROCEDURE COMETA.usp_Merge_Documento AS RETURN 0;');
GO

ALTER PROCEDURE COMETA.usp_Merge_Documento
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @IsCometaExportRunning BIT = 1;

    SELECT TOP (1) @IsCometaExportRunning = IsCometaExportRunning FROM COMETA.Semaforo;

    IF (COALESCE(@IsCometaExportRunning, 1) = 1) RETURN -1;

    MERGE INTO Landing.COMETA_Documento AS TGT
    USING Landing.COMETA_DocumentoView (NOLOCK) AS SRC
    ON SRC.id_documento = TGT.id_documento

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.id_prof_documento = SRC.id_prof_documento,
        TGT.id_registro = SRC.id_registro,
        TGT.data_registrazione = SRC.data_registrazione,
        TGT.num_documento = SRC.num_documento,
        TGT.data_documento = SRC.data_documento,
        TGT.data_competenza = SRC.data_competenza,
        TGT.id_sog_commerciale = SRC.id_sog_commerciale,
        TGT.id_sog_commerciale_fattura = SRC.id_sog_commerciale_fattura,
        TGT.id_gruppo_agenti = SRC.id_gruppo_agenti,
        TGT.data_fine_contratto = SRC.data_fine_contratto,
        TGT.libero_4 = SRC.libero_4,
        TGT.data_inizio_contratto = SRC.data_inizio_contratto,
        TGT.id_libero_1 = SRC.id_libero_1,
        TGT.id_libero_2 = SRC.id_libero_2,
        TGT.id_libero_3 = SRC.id_libero_3,
        TGT.id_tipo_fatturazione = SRC.id_tipo_fatturazione,
        TGT.id_con_pagamento = SRC.id_con_pagamento,
        TGT.rinnovo_automatico = SRC.rinnovo_automatico,
        TGT.note_intestazione = SRC.note_intestazione,
        TGT.note_decisionali = SRC.note_decisionali

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        id_Documento,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
        
        id_prof_documento,
        id_registro,
        data_registrazione,
        num_documento,
        data_documento,
        data_competenza,
        id_sog_commerciale,
        id_sog_commerciale_fattura,
        id_gruppo_agenti,
        data_fine_contratto,
        libero_4,
        data_inizio_contratto,
        id_libero_1,
        id_libero_2,
        id_libero_3,
        id_tipo_fatturazione,
        data_disdetta,
        motivo_disdetta,
        id_con_pagamento,
        rinnovo_automatico,
        note_intestazione,
        note_decisionali
      )

    WHEN NOT MATCHED BY SOURCE
        AND TGT.IsDeleted = CAST(0 AS BIT)
      THEN UPDATE
        SET TGT.IsDeleted = CAST(1 AS BIT),
        TGT.UpdateDatetime = CURRENT_TIMESTAMP,
        TGT.ChangeHashKey = CONVERT(VARBINARY(20), ''),
        TGT.ChangeHashKeyASCII = ''

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        CASE WHEN Inserted.IsDeleted = CAST(1 AS BIT) THEN N'DELETE' ELSE $action END AS merge_action,
        'Landing.COMETA_Documento' AS full_olap_table_name,
        'id_documento = ' + CAST(COALESCE(inserted.id_documento, deleted.id_documento) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;
GO

EXEC COMETA.usp_Merge_Documento;
GO

/*
    SCHEMA_NAME > COMETA
    TABLE_NAME > Documento_Riga
*/

/**
 * @table Landing.COMETA_Documento_Riga
 * @description 

 * @depends COMETA.Documento_Riga

SELECT TOP 100 * FROM COMETA.Documento_Riga;
SELECT TOP 100 * FROM COMETA.Documento_Riga_qlv;
*/

IF OBJECT_ID('Landing.COMETA_Documento_RigaView', 'V') IS NULL EXEC('CREATE VIEW Landing.COMETA_Documento_RigaView AS SELECT 1 AS fld;');
GO

ALTER VIEW Landing.COMETA_Documento_RigaView
AS
WITH TableData
AS (
    SELECT
        DR.id_riga_documento,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            DR.id_riga_documento,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            DR.id_documento,
            DR.id_gruppo_agenti,
            DR.num_riga,
            DR.id_articolo,
            DR.descrizione,
            DR.totale_riga,
            DR.provv_calcolata_carea,
            DR.provv_calcolata_agente,
            DR.provv_calcolata_subagente,
            DR.id_riga_doc_provenienza,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        DR.id_documento,
        DR.id_gruppo_agenti,
        DR.num_riga,
        DR.id_articolo,
        DR.descrizione,
        DR.totale_riga,
        DR.provv_calcolata_carea,
        DR.provv_calcolata_agente,
        DR.provv_calcolata_subagente,
        DR.id_riga_doc_provenienza

    FROM COMETA.Documento_Riga_qlv DR
    INNER JOIN COMETA.Documento D ON D.id_documento = DR.id_documento
        --AND D.id_prof_documento = 1 -- 1: Ordine
)
SELECT
    -- Chiavi
    TD.id_riga_documento,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.id_documento,
    TD.id_gruppo_agenti,
    TD.num_riga,
    TD.id_articolo,
    TD.descrizione COLLATE DATABASE_DEFAULT AS descrizione,

    -- Misure
    TD.totale_riga,
    TD.provv_calcolata_carea,
    TD.provv_calcolata_agente,
    TD.provv_calcolata_subagente,
    
    TD.id_riga_doc_provenienza

FROM TableData TD;
GO

--DROP TABLE IF EXISTS Landing.COMETA_Documento_Riga;
GO

IF OBJECT_ID(N'Landing.COMETA_Documento_Riga', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Landing.COMETA_Documento_Riga FROM Landing.COMETA_Documento_RigaView;

    ALTER TABLE Landing.COMETA_Documento_Riga ALTER COLUMN id_riga_documento INT NOT NULL;

    ALTER TABLE Landing.COMETA_Documento_Riga ADD CONSTRAINT PK_Landing_COMETA_Documento_Riga PRIMARY KEY CLUSTERED (UpdateDatetime, id_riga_documento);

    ALTER TABLE Landing.COMETA_Documento_Riga ALTER COLUMN id_documento INT NULL;
    ALTER TABLE Landing.COMETA_Documento_Riga ALTER COLUMN id_gruppo_agenti INT NULL;
    ALTER TABLE Landing.COMETA_Documento_Riga ALTER COLUMN num_riga INT NULL;
    ALTER TABLE Landing.COMETA_Documento_Riga ALTER COLUMN id_articolo INT NULL;
    ALTER TABLE Landing.COMETA_Documento_Riga ALTER COLUMN descrizione NVARCHAR(MAX) NULL;
    ALTER TABLE Landing.COMETA_Documento_Riga ALTER COLUMN totale_riga DECIMAL(10, 2) NULL;
    ALTER TABLE Landing.COMETA_Documento_Riga ALTER COLUMN provv_calcolata_carea DECIMAL(10, 2) NULL;
    ALTER TABLE Landing.COMETA_Documento_Riga ALTER COLUMN provv_calcolata_agente DECIMAL(10, 2) NULL;
    ALTER TABLE Landing.COMETA_Documento_Riga ALTER COLUMN provv_calcolata_subagente DECIMAL(10, 2) NULL;

    CREATE UNIQUE NONCLUSTERED INDEX IX_COMETA_Documento_Riga_BusinessKey ON Landing.COMETA_Documento_Riga (id_riga_documento);
END;
GO

IF OBJECT_ID('COMETA.usp_Merge_Documento_Riga', 'P') IS NULL EXEC('CREATE PROCEDURE COMETA.usp_Merge_Documento_Riga AS RETURN 0;');
GO

ALTER PROCEDURE COMETA.usp_Merge_Documento_Riga
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @IsCometaExportRunning BIT = 1;

    SELECT TOP (1) @IsCometaExportRunning = IsCometaExportRunning FROM COMETA.Semaforo;

    IF (COALESCE(@IsCometaExportRunning, 1) = 1) RETURN -1;

    MERGE INTO Landing.COMETA_Documento_Riga AS TGT
    USING Landing.COMETA_Documento_RigaView (nolock) AS SRC
    ON SRC.id_riga_documento = TGT.id_riga_documento

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.id_documento = SRC.id_documento,
        TGT.id_gruppo_agenti = SRC.id_gruppo_agenti,
        TGT.num_riga = SRC.num_riga,
        TGT.id_articolo = SRC.id_articolo,
        TGT.descrizione = SRC.descrizione,
        TGT.totale_riga = SRC.totale_riga,
        TGT.provv_calcolata_carea = SRC.provv_calcolata_carea,
        TGT.provv_calcolata_agente = SRC.provv_calcolata_agente,
        TGT.provv_calcolata_subagente = SRC.provv_calcolata_subagente,
        TGT.id_riga_doc_provenienza = SRC.id_riga_doc_provenienza

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        id_riga_documento,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        id_documento,
        id_gruppo_agenti,
        num_riga,
        id_articolo,
        descrizione,
        totale_riga,
        provv_calcolata_carea,
        provv_calcolata_agente,
        provv_calcolata_subagente,
        id_riga_doc_provenienza
      )

    WHEN NOT MATCHED BY SOURCE
        AND TGT.IsDeleted = CAST(0 AS BIT)
      THEN UPDATE
        SET TGT.IsDeleted = CAST(1 AS BIT),
        TGT.UpdateDatetime = CURRENT_TIMESTAMP,
        TGT.ChangeHashKey = CONVERT(VARBINARY(20), ''),
        TGT.ChangeHashKeyASCII = ''

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        CASE WHEN Inserted.IsDeleted = CAST(1 AS BIT) THEN N'DELETE' ELSE $action END AS merge_action,
        'Landing.COMETA_Documento_Riga' AS full_olap_table_name,
        'id_riga_documento = ' + CAST(COALESCE(inserted.id_riga_documento, deleted.id_riga_documento) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;
GO

EXEC COMETA.usp_Merge_Documento_Riga;
GO

/*
    SCHEMA_NAME > COMETA
    TABLE_NAME > Esercizio
*/

/**
 * @table Landing.COMETA_Esercizio
 * @description 

 * @depends COMETA.Esercizio

SELECT TOP 100 * FROM COMETA.Esercizio;
*/

IF OBJECT_ID('Landing.COMETA_EsercizioView', 'V') IS NULL EXEC('CREATE VIEW Landing.COMETA_EsercizioView AS SELECT 1 AS fld;');
GO

ALTER VIEW Landing.COMETA_EsercizioView
AS
WITH TableData
AS (
    SELECT
        id_esercizio,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            id_esercizio,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            codice,
            data_inizio,
            data_fine,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        codice,
        data_inizio,
        data_fine

    FROM COMETA.Esercizio
)
SELECT
    -- Chiavi
    TD.id_esercizio,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.codice COLLATE DATABASE_DEFAULT AS codice,
    TD.data_inizio,
    TD.data_fine

FROM TableData TD;
GO

--DROP TABLE IF EXISTS Landing.COMETA_Esercizio;
GO

IF OBJECT_ID(N'Landing.COMETA_Esercizio', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Landing.COMETA_Esercizio FROM Landing.COMETA_EsercizioView;

    ALTER TABLE Landing.COMETA_Esercizio ALTER COLUMN id_esercizio INT NOT NULL;

    ALTER TABLE Landing.COMETA_Esercizio ADD CONSTRAINT PK_Landing_COMETA_Esercizio PRIMARY KEY CLUSTERED (UpdateDatetime, id_esercizio);

    ALTER TABLE Landing.COMETA_Esercizio ALTER COLUMN codice CHAR(4) NULL;
    ALTER TABLE Landing.COMETA_Esercizio ALTER COLUMN data_inizio DATE NULL;
    ALTER TABLE Landing.COMETA_Esercizio ALTER COLUMN data_fine DATE NULL;

    CREATE UNIQUE NONCLUSTERED INDEX IX_COMETA_Esercizio_BusinessKey ON Landing.COMETA_Esercizio (id_esercizio);
END;
GO

IF OBJECT_ID('COMETA.usp_Merge_Esercizio', 'P') IS NULL EXEC('CREATE PROCEDURE COMETA.usp_Merge_Esercizio AS RETURN 0;');
GO

ALTER PROCEDURE COMETA.usp_Merge_Esercizio
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @IsCometaExportRunning BIT = 1;

    SELECT TOP (1) @IsCometaExportRunning = IsCometaExportRunning FROM COMETA.Semaforo;

    IF (COALESCE(@IsCometaExportRunning, 1) = 1) RETURN -1;

    MERGE INTO Landing.COMETA_Esercizio AS TGT
    USING Landing.COMETA_EsercizioView (nolock) AS SRC
    ON SRC.id_esercizio = TGT.id_esercizio

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.codice = SRC.codice,
        TGT.data_inizio = SRC.data_inizio,
        TGT.data_fine = SRC.data_fine

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        id_esercizio,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        codice,
        data_inizio,
        data_fine
      )

    WHEN NOT MATCHED BY SOURCE
        AND TGT.IsDeleted = CAST(0 AS BIT)
      THEN UPDATE
        SET TGT.IsDeleted = CAST(1 AS BIT),
        TGT.UpdateDatetime = CURRENT_TIMESTAMP,
        TGT.ChangeHashKey = CONVERT(VARBINARY(20), ''),
        TGT.ChangeHashKeyASCII = ''

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        CASE WHEN Inserted.IsDeleted = CAST(1 AS BIT) THEN N'DELETE' ELSE $action END AS merge_action,
        'Landing.COMETA_Esercizio' AS full_olap_table_name,
        'id_esercizio = ' + CAST(COALESCE(inserted.id_esercizio, deleted.id_esercizio) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;
GO

EXEC COMETA.usp_Merge_Esercizio;
GO

/*
    SCHEMA_NAME > COMETA
    TABLE_NAME > Gruppo_Agenti
*/

/**
 * @table Landing.COMETA_Gruppo_Agenti
 * @description 

 * @depends COMETA.Gruppo_Agenti

SELECT TOP 100 * FROM COMETA.Gruppo_Agenti;
*/

IF OBJECT_ID('Landing.COMETA_Gruppo_AgentiView', 'V') IS NULL EXEC('CREATE VIEW Landing.COMETA_Gruppo_AgentiView AS SELECT 1 AS fld;');
GO

ALTER VIEW Landing.COMETA_Gruppo_AgentiView
AS
WITH TableData
AS (
    SELECT
        id_gruppo_agenti,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            id_gruppo_agenti,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            codice,
            descrizione,
            id_sog_com_capo_area,
            id_sog_com_agente,
            id_sog_com_sub_agente,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        codice,
        descrizione,
        id_sog_com_capo_area,
        id_sog_com_agente,
        id_sog_com_sub_agente

    FROM COMETA.Gruppo_Agenti
)
SELECT
    -- Chiavi
    TD.id_gruppo_agenti,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.codice COLLATE DATABASE_DEFAULT AS codice,
    TD.descrizione COLLATE DATABASE_DEFAULT AS descrizione,
    TD.id_sog_com_capo_area,
    TD.id_sog_com_agente,
    TD.id_sog_com_sub_agente

FROM TableData TD;
GO

--DROP TABLE IF EXISTS Landing.COMETA_Gruppo_Agenti;
GO

IF OBJECT_ID(N'Landing.COMETA_Gruppo_Agenti', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Landing.COMETA_Gruppo_Agenti FROM Landing.COMETA_Gruppo_AgentiView;

    ALTER TABLE Landing.COMETA_Gruppo_Agenti ALTER COLUMN id_gruppo_agenti INT NOT NULL;

    ALTER TABLE Landing.COMETA_Gruppo_Agenti ADD CONSTRAINT PK_Landing_COMETA_Gruppo_Agenti PRIMARY KEY CLUSTERED (UpdateDatetime, id_gruppo_agenti);

    ALTER TABLE Landing.COMETA_Gruppo_Agenti ALTER COLUMN codice NVARCHAR(10) NULL;
    ALTER TABLE Landing.COMETA_Gruppo_Agenti ALTER COLUMN descrizione NVARCHAR(60) NULL;
    ALTER TABLE Landing.COMETA_Gruppo_Agenti ALTER COLUMN id_sog_com_capo_area INT NULL;
    ALTER TABLE Landing.COMETA_Gruppo_Agenti ALTER COLUMN id_sog_com_agente INT NULL;
    ALTER TABLE Landing.COMETA_Gruppo_Agenti ALTER COLUMN id_sog_com_sub_agente INT NULL;

    CREATE UNIQUE NONCLUSTERED INDEX IX_COMETA_Gruppo_Agenti_BusinessKey ON Landing.COMETA_Gruppo_Agenti (id_gruppo_agenti);
END;
GO

IF OBJECT_ID('COMETA.usp_Merge_Gruppo_Agenti', 'P') IS NULL EXEC('CREATE PROCEDURE COMETA.usp_Merge_Gruppo_Agenti AS RETURN 0;');
GO

ALTER PROCEDURE COMETA.usp_Merge_Gruppo_Agenti
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @IsCometaExportRunning BIT = 1;

    SELECT TOP (1) @IsCometaExportRunning = IsCometaExportRunning FROM COMETA.Semaforo;

    IF (COALESCE(@IsCometaExportRunning, 1) = 1) RETURN -1;

    MERGE INTO Landing.COMETA_Gruppo_Agenti AS TGT
    USING Landing.COMETA_Gruppo_AgentiView (nolock) AS SRC
    ON SRC.id_gruppo_agenti = TGT.id_gruppo_agenti

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.codice = SRC.codice,
        TGT.descrizione = SRC.descrizione,
        TGT.id_sog_com_capo_area = SRC.id_sog_com_capo_area,
        TGT.id_sog_com_agente = SRC.id_sog_com_agente,
        TGT.id_sog_com_sub_agente = SRC.id_sog_com_sub_agente

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        id_gruppo_agenti,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        codice,
        descrizione,
        id_sog_com_capo_area,
        id_sog_com_agente,
        id_sog_com_sub_agente
      )

    WHEN NOT MATCHED BY SOURCE
        AND TGT.IsDeleted = CAST(0 AS BIT)
      THEN UPDATE
        SET TGT.IsDeleted = CAST(1 AS BIT),
        TGT.UpdateDatetime = CURRENT_TIMESTAMP,
        TGT.ChangeHashKey = CONVERT(VARBINARY(20), ''),
        TGT.ChangeHashKeyASCII = ''

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        CASE WHEN Inserted.IsDeleted = CAST(1 AS BIT) THEN N'DELETE' ELSE $action END AS merge_action,
        'Landing.COMETA_Gruppo_Agenti' AS full_olap_table_name,
        'id_gruppo_agenti = ' + CAST(COALESCE(inserted.id_gruppo_agenti, deleted.id_gruppo_agenti) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;
GO

EXEC COMETA.usp_Merge_Gruppo_Agenti;
GO

/*
    SCHEMA_NAME > COMETA
    TABLE_NAME > Libero_1
*/

/**
 * @table Landing.COMETA_Libero_1
 * @description 

 * @depends COMETA.Libero_1

SELECT TOP 100 * FROM COMETA.Libero_1;
*/

IF OBJECT_ID('Landing.COMETA_Libero_1View', 'V') IS NULL EXEC('CREATE VIEW Landing.COMETA_Libero_1View AS SELECT 1 AS fld;');
GO

ALTER VIEW Landing.COMETA_Libero_1View
AS
WITH TableData
AS (
    SELECT
        id_libero_1,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            id_libero_1,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            codice,
            descrizione,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        codice,
        descrizione

    FROM COMETA.Libero_1
)
SELECT
    -- Chiavi
    TD.id_libero_1,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.codice,
    TD.descrizione

FROM TableData TD;
GO

--DROP TABLE IF EXISTS Landing.COMETA_Libero_1;
GO

IF OBJECT_ID(N'Landing.COMETA_Libero_1', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Landing.COMETA_Libero_1 FROM Landing.COMETA_Libero_1View;

    ALTER TABLE Landing.COMETA_Libero_1 ALTER COLUMN id_libero_1 INT NOT NULL;

    ALTER TABLE Landing.COMETA_Libero_1 ADD CONSTRAINT PK_Landing_COMETA_Libero_1 PRIMARY KEY CLUSTERED (UpdateDatetime, id_libero_1);

    ALTER TABLE Landing.COMETA_Libero_1 ALTER COLUMN codice NVARCHAR(10) NULL;
    ALTER TABLE Landing.COMETA_Libero_1 ALTER COLUMN descrizione NVARCHAR(200) NULL;

    CREATE UNIQUE NONCLUSTERED INDEX IX_COMETA_Libero_1_BusinessKey ON Landing.COMETA_Libero_1 (id_libero_1);
END;
GO

IF OBJECT_ID('COMETA.usp_Merge_Libero_1', 'P') IS NULL EXEC('CREATE PROCEDURE COMETA.usp_Merge_Libero_1 AS RETURN 0;');
GO

ALTER PROCEDURE COMETA.usp_Merge_Libero_1
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @IsCometaExportRunning BIT = 1;

    SELECT TOP (1) @IsCometaExportRunning = IsCometaExportRunning FROM COMETA.Semaforo;

    IF (COALESCE(@IsCometaExportRunning, 1) = 1) RETURN -1;

    MERGE INTO Landing.COMETA_Libero_1 AS TGT
    USING Landing.COMETA_Libero_1View (nolock) AS SRC
    ON SRC.id_libero_1 = TGT.id_libero_1

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.codice = SRC.codice,
        TGT.descrizione = SRC.descrizione

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        id_libero_1,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        codice,
        descrizione
      )

    WHEN NOT MATCHED BY SOURCE
        AND TGT.IsDeleted = CAST(0 AS BIT)
      THEN UPDATE
        SET TGT.IsDeleted = CAST(1 AS BIT),
        TGT.UpdateDatetime = CURRENT_TIMESTAMP,
        TGT.ChangeHashKey = CONVERT(VARBINARY(20), ''),
        TGT.ChangeHashKeyASCII = ''

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        CASE WHEN Inserted.IsDeleted = CAST(1 AS BIT) THEN N'DELETE' ELSE $action END AS merge_action,
        'Landing.COMETA_Libero_1' AS full_olap_table_name,
        'id_libero_1 = ' + CAST(COALESCE(inserted.id_libero_1, deleted.id_libero_1) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;
GO

EXEC COMETA.usp_Merge_Libero_1;
GO

/*
    SCHEMA_NAME > COMETA
    TABLE_NAME > Libero_2
*/

/**
 * @table Landing.COMETA_Libero_2
 * @description 

 * @depends COMETA.Libero_2

SELECT TOP 100 * FROM COMETA.Libero_2;
*/

IF OBJECT_ID('Landing.COMETA_Libero_2View', 'V') IS NULL EXEC('CREATE VIEW Landing.COMETA_Libero_2View AS SELECT 1 AS fld;');
GO

ALTER VIEW Landing.COMETA_Libero_2View
AS
WITH TableData
AS (
    SELECT
        id_libero_2,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            id_libero_2,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            codice,
            descrizione,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        codice,
        descrizione

    FROM COMETA.Libero_2
)
SELECT
    -- Chiavi
    TD.id_libero_2,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.codice,
    TD.descrizione

FROM TableData TD;
GO

--DROP TABLE IF EXISTS Landing.COMETA_Libero_2;
GO

IF OBJECT_ID(N'Landing.COMETA_Libero_2', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Landing.COMETA_Libero_2 FROM Landing.COMETA_Libero_2View;

    ALTER TABLE Landing.COMETA_Libero_2 ALTER COLUMN id_libero_2 INT NOT NULL;

    ALTER TABLE Landing.COMETA_Libero_2 ADD CONSTRAINT PK_Landing_COMETA_Libero_2 PRIMARY KEY CLUSTERED (UpdateDatetime, id_libero_2);

    ALTER TABLE Landing.COMETA_Libero_2 ALTER COLUMN codice NVARCHAR(10) NULL;
    ALTER TABLE Landing.COMETA_Libero_2 ALTER COLUMN descrizione NVARCHAR(200) NULL;

    CREATE UNIQUE NONCLUSTERED INDEX IX_COMETA_Libero_2_BusinessKey ON Landing.COMETA_Libero_2 (id_libero_2);
END;
GO

IF OBJECT_ID('COMETA.usp_Merge_Libero_2', 'P') IS NULL EXEC('CREATE PROCEDURE COMETA.usp_Merge_Libero_2 AS RETURN 0;');
GO

ALTER PROCEDURE COMETA.usp_Merge_Libero_2
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @IsCometaExportRunning BIT = 1;

    SELECT TOP (1) @IsCometaExportRunning = IsCometaExportRunning FROM COMETA.Semaforo;

    IF (COALESCE(@IsCometaExportRunning, 1) = 1) RETURN -1;

    MERGE INTO Landing.COMETA_Libero_2 AS TGT
    USING Landing.COMETA_Libero_2View (nolock) AS SRC
    ON SRC.id_libero_2 = TGT.id_libero_2

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.codice = SRC.codice,
        TGT.descrizione = SRC.descrizione

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        id_libero_2,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        codice,
        descrizione
      )

    WHEN NOT MATCHED BY SOURCE
        AND TGT.IsDeleted = CAST(0 AS BIT)
      THEN UPDATE
        SET TGT.IsDeleted = CAST(1 AS BIT),
        TGT.UpdateDatetime = CURRENT_TIMESTAMP,
        TGT.ChangeHashKey = CONVERT(VARBINARY(20), ''),
        TGT.ChangeHashKeyASCII = ''

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        CASE WHEN Inserted.IsDeleted = CAST(1 AS BIT) THEN N'DELETE' ELSE $action END AS merge_action,
        'Landing.COMETA_Libero_2' AS full_olap_table_name,
        'id_libero_2 = ' + CAST(COALESCE(inserted.id_libero_2, deleted.id_libero_2) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;
GO

EXEC COMETA.usp_Merge_Libero_2;
GO

/*
    SCHEMA_NAME > COMETA
    TABLE_NAME > Libero_3
*/

/**
 * @table Landing.COMETA_Libero_3
 * @description 

 * @depends COMETA.Libero_3

SELECT TOP 100 * FROM COMETA.Libero_3;
*/

IF OBJECT_ID('Landing.COMETA_Libero_3View', 'V') IS NULL EXEC('CREATE VIEW Landing.COMETA_Libero_3View AS SELECT 1 AS fld;');
GO

ALTER VIEW Landing.COMETA_Libero_3View
AS
WITH TableData
AS (
    SELECT
        id_libero_3,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            id_libero_3,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            codice,
            descrizione,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        codice,
        descrizione

    FROM COMETA.Libero_3
)
SELECT
    -- Chiavi
    TD.id_libero_3,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.codice,
    TD.descrizione

FROM TableData TD;
GO

--DROP TABLE IF EXISTS Landing.COMETA_Libero_3;
GO

IF OBJECT_ID(N'Landing.COMETA_Libero_3', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Landing.COMETA_Libero_3 FROM Landing.COMETA_Libero_3View;

    ALTER TABLE Landing.COMETA_Libero_3 ALTER COLUMN id_libero_3 INT NOT NULL;

    ALTER TABLE Landing.COMETA_Libero_3 ADD CONSTRAINT PK_Landing_COMETA_Libero_3 PRIMARY KEY CLUSTERED (UpdateDatetime, id_libero_3);

    ALTER TABLE Landing.COMETA_Libero_3 ALTER COLUMN codice NVARCHAR(10) NULL;
    ALTER TABLE Landing.COMETA_Libero_3 ALTER COLUMN descrizione NVARCHAR(200) NULL;

    CREATE UNIQUE NONCLUSTERED INDEX IX_COMETA_Libero_3_BusinessKey ON Landing.COMETA_Libero_3 (id_libero_3);
END;
GO

IF OBJECT_ID('COMETA.usp_Merge_Libero_3', 'P') IS NULL EXEC('CREATE PROCEDURE COMETA.usp_Merge_Libero_3 AS RETURN 0;');
GO

ALTER PROCEDURE COMETA.usp_Merge_Libero_3
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @IsCometaExportRunning BIT = 1;

    SELECT TOP (1) @IsCometaExportRunning = IsCometaExportRunning FROM COMETA.Semaforo;

    IF (COALESCE(@IsCometaExportRunning, 1) = 1) RETURN -1;

    MERGE INTO Landing.COMETA_Libero_3 AS TGT
    USING Landing.COMETA_Libero_3View (nolock) AS SRC
    ON SRC.id_libero_3 = TGT.id_libero_3

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.codice = SRC.codice,
        TGT.descrizione = SRC.descrizione

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        id_libero_3,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        codice,
        descrizione
      )

    WHEN NOT MATCHED BY SOURCE
        AND TGT.IsDeleted = CAST(0 AS BIT)
      THEN UPDATE
        SET TGT.IsDeleted = CAST(1 AS BIT),
        TGT.UpdateDatetime = CURRENT_TIMESTAMP,
        TGT.ChangeHashKey = CONVERT(VARBINARY(20), ''),
        TGT.ChangeHashKeyASCII = ''

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        CASE WHEN Inserted.IsDeleted = CAST(1 AS BIT) THEN N'DELETE' ELSE $action END AS merge_action,
        'Landing.COMETA_Libero_3' AS full_olap_table_name,
        'id_libero_3 = ' + CAST(COALESCE(inserted.id_libero_3, deleted.id_libero_3) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;
GO

EXEC COMETA.usp_Merge_Libero_3;
GO

/*
    SCHEMA_NAME > COMETA
    TABLE_NAME > MovimentiScadenza
*/

/**
 * @table Landing.COMETA_MovimentiScadenza
 * @description 

 * @depends COMETA.MovimentiScadenza

SELECT TOP 100 * FROM COMETA.MovimentiScadenza;
*/

IF OBJECT_ID('Landing.COMETA_MovimentiScadenzaView', 'V') IS NULL EXEC('CREATE VIEW Landing.COMETA_MovimentiScadenzaView AS SELECT 1 AS fld;');
GO

ALTER VIEW Landing.COMETA_MovimentiScadenzaView
AS
WITH TableData
AS (
    SELECT
        id_mov_scadenza,

        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            id_mov_scadenza,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            id_scadenza,
            data,
            importo,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        id_scadenza,
        data,
        importo

    FROM COMETA.MovimentiScadenza
)
SELECT
    -- Chiavi
    TD.id_mov_scadenza,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.id_scadenza,
    TD.data,
    TD.importo

FROM TableData TD;
GO

--DROP TABLE IF EXISTS Landing.COMETA_MovimentiScadenza;
GO

IF OBJECT_ID(N'Landing.COMETA_MovimentiScadenza', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Landing.COMETA_MovimentiScadenza FROM Landing.COMETA_MovimentiScadenzaView;

    ALTER TABLE Landing.COMETA_MovimentiScadenza ALTER COLUMN id_mov_scadenza INT NOT NULL;

    ALTER TABLE Landing.COMETA_MovimentiScadenza ADD CONSTRAINT PK_Landing_COMETA_MovimentiScadenza PRIMARY KEY CLUSTERED (UpdateDatetime, id_mov_scadenza);

    CREATE UNIQUE NONCLUSTERED INDEX IX_COMETA_MovimentiScadenza_BusinessKey ON Landing.COMETA_MovimentiScadenza (id_mov_scadenza);
END;
GO

IF OBJECT_ID('COMETA.usp_Merge_MovimentiScadenza', 'P') IS NULL EXEC('CREATE PROCEDURE COMETA.usp_Merge_MovimentiScadenza AS RETURN 0;');
GO

ALTER PROCEDURE COMETA.usp_Merge_MovimentiScadenza
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @IsCometaExportRunning BIT = 1;

    SELECT TOP (1) @IsCometaExportRunning = IsCometaExportRunning FROM COMETA.Semaforo;

    IF (COALESCE(@IsCometaExportRunning, 1) = 1) RETURN -1;

    MERGE INTO Landing.COMETA_MovimentiScadenza AS TGT
    USING Landing.COMETA_MovimentiScadenzaView (nolock) AS SRC
    ON SRC.id_mov_scadenza = TGT.id_mov_scadenza

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.id_scadenza = SRC.id_scadenza,
        TGT.data = SRC.data,
        TGT.importo = SRC.importo

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        id_mov_scadenza,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        id_scadenza,
        data,
        importo
      )

    WHEN NOT MATCHED BY SOURCE
        AND TGT.IsDeleted = CAST(0 AS BIT)
      THEN UPDATE
        SET TGT.IsDeleted = CAST(1 AS BIT),
        TGT.UpdateDatetime = CURRENT_TIMESTAMP,
        TGT.ChangeHashKey = CONVERT(VARBINARY(20), ''),
        TGT.ChangeHashKeyASCII = ''

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        CASE WHEN Inserted.IsDeleted = CAST(1 AS BIT) THEN N'DELETE' ELSE $action END AS merge_action,
        'Landing.COMETA_MovimentiScadenza' AS full_olap_table_name,
        'id_mov_scadenza = ' + CAST(COALESCE(inserted.id_mov_scadenza, deleted.id_mov_scadenza) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;
GO

EXEC COMETA.usp_Merge_MovimentiScadenza;
GO

/*
    SCHEMA_NAME > COMETA
    TABLE_NAME > MySolutionContracts
*/

/**
 * @table Landing.COMETA_MySolutionContracts
 * @description 

 * @depends COMETA.MySolutionContracts

SELECT TOP 100 * FROM COMETA.MySolutionContracts;
*/

IF OBJECT_ID('Landing.COMETA_MySolutionContractsView', 'V') IS NULL EXEC('CREATE VIEW Landing.COMETA_MySolutionContractsView AS SELECT 1 AS fld;');
GO

ALTER VIEW Landing.COMETA_MySolutionContractsView
AS
WITH MySolutionContractsDetail
AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY id_riga_documento ORDER BY EMail) AS rn
    FROM COMETA.MySolutionContracts
),
TableData
AS (
    SELECT
        id_riga_documento,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            id_riga_documento,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            id_anagrafica,
            codice,
            RagioneSociale,
            indirizzo,
            cap,
            localita,
            provincia,
            nazione,
            cod_fiscale,
            par_iva,
            EMail,
            num_progressivo,
            num_documento,
            data_documento,
            data_inizio_contratto,
            data_fine_contratto,
            Nome,
            Cognome,
            Quote,
            id_sog_commerciale,
            tipo,
            id_documento,
            descrizione,
            id_articolo,
            prezzo,
            sconto,
            prezzo_netto,
            prezzo_netto_ivato,
            note_intestazione,
            data_disdetta,
            motivo_disdetta,
            pec,
            CodiceArticolo,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        id_anagrafica,
        codice,
        RagioneSociale,
        indirizzo,
        cap,
        localita,
        provincia,
        nazione,
        cod_fiscale,
        par_iva,
        EMail,
        num_progressivo,
        num_documento,
        data_documento,
        data_inizio_contratto,
        data_fine_contratto,
        Nome,
        Cognome,
        Quote,
        id_sog_commerciale,
        tipo,
        id_documento,
        descrizione,
        id_articolo,
        prezzo,
        sconto,
        prezzo_netto,
        prezzo_netto_ivato,
        note_intestazione,
        data_disdetta,
        motivo_disdetta,
        pec,
        CodiceArticolo

    FROM MySolutionContractsDetail
    WHERE rn = 1
)
SELECT
    -- Chiavi
    TD.id_riga_documento,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.id_anagrafica,
    TD.codice,
    TD.RagioneSociale,
    TD.indirizzo,
    TD.cap,
    TD.localita,
    TD.provincia,
    TD.nazione,
    TD.cod_fiscale,
    TD.par_iva,
    TD.EMail,
    TD.num_progressivo,
    TD.num_documento,
    TD.data_documento,
    TD.data_inizio_contratto,
    TD.data_fine_contratto,
    TD.Nome,
    TD.Cognome,
    TD.Quote,
    TD.id_sog_commerciale,
    TD.tipo,
    TD.id_documento,
    TD.descrizione,
    TD.id_articolo,
    TD.prezzo,
    TD.sconto,
    TD.prezzo_netto,
    TD.prezzo_netto_ivato,
    TD.note_intestazione,
    TD.data_disdetta,
    TD.motivo_disdetta,
    TD.pec,
    TD.CodiceArticolo

FROM TableData TD;
GO

--DROP TABLE IF EXISTS Landing.COMETA_MySolutionContracts;
GO

IF OBJECT_ID(N'Landing.COMETA_MySolutionContracts', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Landing.COMETA_MySolutionContracts FROM Landing.COMETA_MySolutionContractsView;

    ALTER TABLE Landing.COMETA_MySolutionContracts ALTER COLUMN id_riga_documento INT NOT NULL;

    ALTER TABLE Landing.COMETA_MySolutionContracts ADD CONSTRAINT PK_Landing_COMETA_MySolutionContracts PRIMARY KEY CLUSTERED (UpdateDatetime, id_riga_documento);

    CREATE UNIQUE NONCLUSTERED INDEX IX_COMETA_MySolutionContracts_BusinessKey ON Landing.COMETA_MySolutionContracts (id_riga_documento);
END;
GO

IF OBJECT_ID('COMETA.usp_Merge_MySolutionContracts', 'P') IS NULL EXEC('CREATE PROCEDURE COMETA.usp_Merge_MySolutionContracts AS RETURN 0;');
GO

ALTER PROCEDURE COMETA.usp_Merge_MySolutionContracts
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @IsCometaExportRunning BIT = 1;

    SELECT TOP (1) @IsCometaExportRunning = IsCometaExportRunning FROM COMETA.Semaforo;

    IF (COALESCE(@IsCometaExportRunning, 1) = 1) RETURN -1;

    MERGE INTO Landing.COMETA_MySolutionContracts AS TGT
    USING Landing.COMETA_MySolutionContractsView (nolock) AS SRC
    ON SRC.id_riga_documento = TGT.id_riga_documento

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.id_anagrafica = SRC.id_anagrafica,
        TGT.codice = SRC.codice,
        TGT.RagioneSociale = SRC.RagioneSociale,
        TGT.indirizzo = SRC.indirizzo,
        TGT.cap = SRC.cap,
        TGT.localita = SRC.localita,
        TGT.provincia = SRC.provincia,
        TGT.nazione = SRC.nazione,
        TGT.cod_fiscale = SRC.cod_fiscale,
        TGT.par_iva = SRC.par_iva,
        TGT.EMail = SRC.EMail,
        TGT.num_progressivo = SRC.num_progressivo,
        TGT.num_documento = SRC.num_documento,
        TGT.data_documento = SRC.data_documento,
        TGT.data_inizio_contratto = SRC.data_inizio_contratto,
        TGT.data_fine_contratto = SRC.data_fine_contratto,
        TGT.Nome = SRC.Nome,
        TGT.Cognome = SRC.Cognome,
        TGT.Quote = SRC.Quote,
        TGT.id_sog_commerciale = SRC.id_sog_commerciale,
        TGT.tipo = SRC.tipo,
        TGT.id_documento = SRC.id_documento,
        TGT.descrizione = SRC.descrizione,
        TGT.id_articolo = SRC.id_articolo,
        TGT.prezzo = SRC.prezzo,
        TGT.sconto = SRC.sconto,
        TGT.prezzo_netto = SRC.prezzo_netto,
        TGT.prezzo_netto_ivato = SRC.prezzo_netto_ivato,
        TGT.note_intestazione = SRC.note_intestazione,
        TGT.data_disdetta = SRC.data_disdetta,
        TGT.motivo_disdetta = SRC.motivo_disdetta,
        TGT.pec = SRC.pec,
        TGT.CodiceArticolo = SRC.CodiceArticolo

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        id_riga_documento,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,

        id_anagrafica,
        codice,
        RagioneSociale,
        indirizzo,
        cap,
        localita,
        provincia,
        nazione,
        cod_fiscale,
        par_iva,
        EMail,
        num_progressivo,
        num_documento,
        data_documento,
        data_inizio_contratto,
        data_fine_contratto,
        Nome,
        Cognome,
        Quote,
        id_sog_commerciale,
        tipo,
        id_documento,
        descrizione,
        id_articolo,
        prezzo,
        sconto,
        prezzo_netto,
        prezzo_netto_ivato,
        note_intestazione,
        data_disdetta,
        motivo_disdetta,
        pec,
        CodiceArticolo
      )

    WHEN NOT MATCHED BY SOURCE
        AND TGT.IsDeleted = CAST(0 AS BIT)
      THEN UPDATE
        SET TGT.IsDeleted = CAST(1 AS BIT),
        TGT.UpdateDatetime = CURRENT_TIMESTAMP,
        TGT.ChangeHashKey = CONVERT(VARBINARY(20), ''),
        TGT.ChangeHashKeyASCII = ''

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        CASE WHEN Inserted.IsDeleted = CAST(1 AS BIT) THEN N'DELETE' ELSE $action END AS merge_action,
        'Landing.COMETA_MySolutionContracts' AS full_olap_table_name,
        'id_riga_documento = ' + CAST(COALESCE(inserted.id_riga_documento, deleted.id_riga_documento) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;
GO

EXEC COMETA.usp_Merge_MySolutionContracts;
GO

/**
 * @table Landing.COMETA_MySolutionUsers
 * @description 

 * @depends COMETA.MySolutionUsers

SELECT TOP 100 * FROM COMETA.MySolutionUsers;
*/

IF OBJECT_ID('Landing.COMETA_MySolutionUsersView', 'V') IS NULL EXEC('CREATE VIEW Landing.COMETA_MySolutionUsersView AS SELECT 1 AS fld;');
GO

ALTER VIEW Landing.COMETA_MySolutionUsersView
AS
WITH TableData
AS (
    SELECT

        LOWER(EMail) AS Email,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            LOWER(EMail),
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            id_anagrafica,
            codice,
            RagioneSociale,
            indirizzo,
            cap,
            localita,
            provincia,
            nazione,
            cod_fiscale,
            par_iva,
            num_progressivo,
            num_documento,
            data_documento,
            data_inizio_contratto,
            data_fine_contratto,
            HaSconto,
            Nome,
            Cognome,
            Quote,
            telefono_descrizione,
            id_telefono,
            id_sog_commerciale,
            tipo,
            id_documento,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        id_anagrafica,
        codice,
        RagioneSociale,
        indirizzo,
        cap,
        localita,
        provincia,
        nazione,
        cod_fiscale,
        par_iva,
        num_progressivo,
        num_documento,
        data_documento,
        data_inizio_contratto,
        data_fine_contratto,
        HaSconto,
        Nome,
        Cognome,
        Quote,
        telefono_descrizione,
        id_telefono,
        id_sog_commerciale,
        tipo,
        id_documento,

        ROW_NUMBER() OVER (PARTITION BY EMail ORDER BY data_inizio_contratto DESC, id_anagrafica DESC) AS rn

    FROM COMETA.MySolutionUsers
    WHERE LOWER(EMail) <> N'[DA INSERIRE]'
)
SELECT
    -- Chiavi
    TD.EMail,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.id_anagrafica,
    TD.codice COLLATE DATABASE_DEFAULT AS codice,
    TD.RagioneSociale COLLATE DATABASE_DEFAULT AS RagioneSociale,
    TD.indirizzo COLLATE DATABASE_DEFAULT AS indirizzo,
    TD.cap COLLATE DATABASE_DEFAULT AS cap,
    TD.localita COLLATE DATABASE_DEFAULT AS localita,
    TD.provincia COLLATE DATABASE_DEFAULT AS provincia,
    TD.nazione COLLATE DATABASE_DEFAULT AS nazione,
    TD.cod_fiscale COLLATE DATABASE_DEFAULT AS cod_fiscale,
    TD.par_iva COLLATE DATABASE_DEFAULT AS par_iva,
    TD.num_progressivo,
    TD.num_documento COLLATE DATABASE_DEFAULT AS num_documento,
    TD.data_documento,
    TD.data_inizio_contratto,
    TD.data_fine_contratto,
    TD.HaSconto,
    TD.Nome COLLATE DATABASE_DEFAULT AS Nome,
    TD.Cognome COLLATE DATABASE_DEFAULT AS Cognome,
    TD.Quote,
    TD.telefono_descrizione COLLATE DATABASE_DEFAULT AS telefono_descrizione,
    TD.id_telefono,
    TD.id_sog_commerciale,
    TD.tipo COLLATE DATABASE_DEFAULT AS tipo,
    TD.id_documento

FROM TableData TD
WHERE TD.rn = 1;
GO

--DROP TABLE IF EXISTS Landing.COMETA_MySolutionUsers;
GO

IF OBJECT_ID(N'Landing.COMETA_MySolutionUsers', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Landing.COMETA_MySolutionUsers FROM Landing.COMETA_MySolutionUsersView;

    ALTER TABLE Landing.COMETA_MySolutionUsers ALTER COLUMN Email NVARCHAR(60) NOT NULL;

    ALTER TABLE Landing.COMETA_MySolutionUsers ADD CONSTRAINT PK_Landing_COMETA_MySolutionUsers PRIMARY KEY CLUSTERED (UpdateDatetime, Email);

    CREATE UNIQUE NONCLUSTERED INDEX IX_COMETA_MySolutionUsers_BusinessKey ON Landing.COMETA_MySolutionUsers (EMail);
END;
GO

IF OBJECT_ID('COMETA.usp_Merge_MySolutionUsers', 'P') IS NULL EXEC('CREATE PROCEDURE COMETA.usp_Merge_MySolutionUsers AS RETURN 0;');
GO

ALTER PROCEDURE COMETA.usp_Merge_MySolutionUsers
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @IsCometaExportRunning BIT = 1;

    SELECT TOP (1) @IsCometaExportRunning = IsCometaExportRunning FROM COMETA.Semaforo;

    IF (COALESCE(@IsCometaExportRunning, 1) = 1) RETURN -1;

    MERGE INTO Landing.COMETA_MySolutionUsers AS TGT
    USING Landing.COMETA_MySolutionUsersView (nolock) AS SRC
    ON SRC.Email = TGT.Email

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.id_anagrafica = SRC.id_anagrafica,
        TGT.codice = SRC.codice,
        TGT.RagioneSociale = SRC.RagioneSociale,
        TGT.indirizzo = SRC.indirizzo,
        TGT.cap = SRC.cap,
        TGT.localita = SRC.localita,
        TGT.provincia = SRC.provincia,
        TGT.nazione = SRC.nazione,
        TGT.cod_fiscale = SRC.cod_fiscale,
        TGT.par_iva = SRC.par_iva,
        TGT.num_progressivo = SRC.num_progressivo,
        TGT.num_documento = SRC.num_documento,
        TGT.data_documento = SRC.data_documento,
        TGT.data_inizio_contratto = SRC.data_inizio_contratto,
        TGT.data_fine_contratto = SRC.data_fine_contratto,
        TGT.HaSconto = SRC.HaSconto,
        TGT.Nome = SRC.Nome,
        TGT.Cognome = SRC.Cognome,
        TGT.Quote = SRC.Quote,
        TGT.telefono_descrizione = SRC.telefono_descrizione,
        TGT.id_telefono = SRC.id_telefono,
        TGT.id_sog_commerciale = SRC.id_sog_commerciale,
        TGT.tipo = SRC.tipo,
        TGT.id_documento = SRC.id_documento

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        EMail,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        id_anagrafica,
        codice,
        RagioneSociale,
        indirizzo,
        cap,
        localita,
        provincia,
        nazione,
        cod_fiscale,
        par_iva,
        num_progressivo,
        num_documento,
        data_documento,
        data_inizio_contratto,
        data_fine_contratto,
        HaSconto,
        Nome,
        Cognome,
        Quote,
        telefono_descrizione,
        id_telefono,
        id_sog_commerciale,
        tipo,
        id_documento
      )

    WHEN NOT MATCHED BY SOURCE
        AND TGT.IsDeleted = CAST(0 AS BIT)
      THEN UPDATE
        SET TGT.IsDeleted = CAST(1 AS BIT),
        TGT.UpdateDatetime = CURRENT_TIMESTAMP,
        TGT.ChangeHashKey = CONVERT(VARBINARY(20), ''),
        TGT.ChangeHashKeyASCII = ''

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        CASE WHEN Inserted.IsDeleted = CAST(1 AS BIT) THEN N'DELETE' ELSE $action END AS merge_action,
        'Landing.COMETA_MySolutionUsers' AS full_olap_table_name,
        'EMail = ' + CAST(COALESCE(inserted.EMail, deleted.EMail) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;
GO

EXEC COMETA.usp_Merge_MySolutionUsers;
GO

/*
    SCHEMA_NAME > COMETA
    TABLE_NAME > Profilo_Documento
*/

/**
 * @table Landing.COMETA_Profilo_Documento
 * @description 

 * @depends COMETA.Profilo_Documento

SELECT TOP 100 * FROM COMETA.Profilo_Documento;
*/

IF OBJECT_ID('Landing.COMETA_Profilo_DocumentoView', 'V') IS NULL EXEC('CREATE VIEW Landing.COMETA_Profilo_DocumentoView AS SELECT 1 AS fld;');
GO

ALTER VIEW Landing.COMETA_Profilo_DocumentoView
AS
WITH TableData
AS (
    SELECT
        id_prof_documento,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            id_prof_documento,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            codice,
            descrizione,
            tipo_registro,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        codice,
        descrizione,
        tipo_registro

    FROM COMETA.Profilo_Documento
)
SELECT
    -- Chiavi
    TD.id_prof_documento,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.codice COLLATE DATABASE_DEFAULT AS codice,
    TD.descrizione COLLATE DATABASE_DEFAULT AS descrizione,
    TD.tipo_registro COLLATE DATABASE_DEFAULT AS tipo_registro

FROM TableData TD;
GO

--DROP TABLE IF EXISTS Landing.COMETA_Profilo_Documento;
GO

IF OBJECT_ID(N'Landing.COMETA_Profilo_Documento', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Landing.COMETA_Profilo_Documento FROM Landing.COMETA_Profilo_DocumentoView;

    ALTER TABLE Landing.COMETA_Profilo_Documento ALTER COLUMN id_prof_documento INT NOT NULL;

    ALTER TABLE Landing.COMETA_Profilo_Documento ADD CONSTRAINT PK_Landing_COMETA_Profilo_Documento PRIMARY KEY CLUSTERED (UpdateDatetime, id_prof_documento);

    ALTER TABLE Landing.COMETA_Profilo_Documento ALTER COLUMN codice NVARCHAR(10) NULL;
    ALTER TABLE Landing.COMETA_Profilo_Documento ALTER COLUMN descrizione NVARCHAR(60) NULL;
    ALTER TABLE Landing.COMETA_Profilo_Documento ALTER COLUMN tipo_registro CHAR(2) NULL;

    CREATE UNIQUE NONCLUSTERED INDEX IX_COMETA_Profilo_Documento_BusinessKey ON Landing.COMETA_Profilo_Documento (id_prof_documento);
END;
GO

IF OBJECT_ID('COMETA.usp_Merge_Profilo_Documento', 'P') IS NULL EXEC('CREATE PROCEDURE COMETA.usp_Merge_Profilo_Documento AS RETURN 0;');
GO

ALTER PROCEDURE COMETA.usp_Merge_Profilo_Documento
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @IsCometaExportRunning BIT = 1;

    SELECT TOP (1) @IsCometaExportRunning = IsCometaExportRunning FROM COMETA.Semaforo;

    IF (COALESCE(@IsCometaExportRunning, 1) = 1) RETURN -1;

    MERGE INTO Landing.COMETA_Profilo_Documento AS TGT
    USING Landing.COMETA_Profilo_DocumentoView (nolock) AS SRC
    ON SRC.id_prof_documento = TGT.id_prof_documento

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.codice = SRC.codice,
        TGT.descrizione = SRC.descrizione,
        TGT.tipo_registro = SRC.tipo_registro

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        id_prof_documento,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        codice,
        descrizione,
        tipo_registro
      )

    WHEN NOT MATCHED BY SOURCE
        AND TGT.IsDeleted = CAST(0 AS BIT)
      THEN UPDATE
        SET TGT.IsDeleted = CAST(1 AS BIT),
        TGT.UpdateDatetime = CURRENT_TIMESTAMP,
        TGT.ChangeHashKey = CONVERT(VARBINARY(20), ''),
        TGT.ChangeHashKeyASCII = ''

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        CASE WHEN Inserted.IsDeleted = CAST(1 AS BIT) THEN N'DELETE' ELSE $action END AS merge_action,
        'Landing.COMETA_Profilo_Documento' AS full_olap_table_name,
        'id_prof_documento = ' + CAST(COALESCE(inserted.id_prof_documento, deleted.id_prof_documento) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;
GO

EXEC COMETA.usp_Merge_Profilo_Documento;
GO

/*
    SCHEMA_NAME > COMETA
    TABLE_NAME > Registro
*/

/**
 * @table Landing.COMETA_Registro
 * @description 

 * @depends COMETA.Registro

SELECT TOP 100 * FROM COMETA.Registro;
*/

IF OBJECT_ID('Landing.COMETA_RegistroView', 'V') IS NULL EXEC('CREATE VIEW Landing.COMETA_RegistroView AS SELECT 1 AS fld;');
GO

ALTER VIEW Landing.COMETA_RegistroView
AS
WITH TableData
AS (
    SELECT
        id_registro,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            id_registro,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            id_esercizio,
            tipo_registro,
            id_mod_registro,
            numero,
            descrizione,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        id_esercizio,
        tipo_registro,
        id_mod_registro,
        numero,
        descrizione

    FROM COMETA.Registro
)
SELECT
    -- Chiavi
    TD.id_registro,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.id_esercizio,
    TD.tipo_registro COLLATE DATABASE_DEFAULT AS tipo_registro,
    TD.id_mod_registro,
    TD.numero,
    TD.descrizione COLLATE DATABASE_DEFAULT AS descrizione

FROM TableData TD;
GO

--DROP TABLE IF EXISTS Landing.COMETA_Registro;
GO

IF OBJECT_ID(N'Landing.COMETA_Registro', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Landing.COMETA_Registro FROM Landing.COMETA_RegistroView;

    ALTER TABLE Landing.COMETA_Registro ALTER COLUMN id_registro INT NOT NULL;

    ALTER TABLE Landing.COMETA_Registro ADD CONSTRAINT PK_Landing_COMETA_Registro PRIMARY KEY CLUSTERED (UpdateDatetime, id_registro);

    CREATE UNIQUE NONCLUSTERED INDEX IX_COMETA_Registro_BusinessKey ON Landing.COMETA_Registro (id_registro);
END;
GO

IF OBJECT_ID('COMETA.usp_Merge_Registro', 'P') IS NULL EXEC('CREATE PROCEDURE COMETA.usp_Merge_Registro AS RETURN 0;');
GO

ALTER PROCEDURE COMETA.usp_Merge_Registro
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @IsCometaExportRunning BIT = 1;

    SELECT TOP (1) @IsCometaExportRunning = IsCometaExportRunning FROM COMETA.Semaforo;

    IF (COALESCE(@IsCometaExportRunning, 1) = 1) RETURN -1;

    MERGE INTO Landing.COMETA_Registro AS TGT
    USING Landing.COMETA_RegistroView (nolock) AS SRC
    ON SRC.id_registro = TGT.id_registro

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.id_esercizio = SRC.id_esercizio,
        TGT.tipo_registro = SRC.tipo_registro,
        TGT.id_mod_registro = SRC.id_mod_registro,
        TGT.numero = SRC.numero,
        TGT.descrizione = SRC.descrizione

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        id_registro,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        id_esercizio,
        tipo_registro,
        id_mod_registro,
        numero,
        descrizione
      )

    WHEN NOT MATCHED BY SOURCE
        AND TGT.IsDeleted = CAST(0 AS BIT)
      THEN UPDATE
        SET TGT.IsDeleted = CAST(1 AS BIT),
        TGT.UpdateDatetime = CURRENT_TIMESTAMP,
        TGT.ChangeHashKey = CONVERT(VARBINARY(20), ''),
        TGT.ChangeHashKeyASCII = ''

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        CASE WHEN Inserted.IsDeleted = CAST(1 AS BIT) THEN N'DELETE' ELSE $action END AS merge_action,
        'Landing.COMETA_Registro' AS full_olap_table_name,
        'id_registro = ' + CAST(COALESCE(inserted.id_registro, deleted.id_registro) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;
GO

EXEC COMETA.usp_Merge_Registro;
GO

/*
    SCHEMA_NAME > COMETA
    TABLE_NAME > Scadenza
*/

/**
 * @table Landing.COMETA_Scadenza
 * @description 

 * @depends COMETA.Scadenza

SELECT TOP 100 * FROM COMETA.Scadenza;
*/

IF OBJECT_ID('Landing.COMETA_ScadenzaView', 'V') IS NULL EXEC('CREATE VIEW Landing.COMETA_ScadenzaView AS SELECT 1 AS fld;');
GO

ALTER VIEW Landing.COMETA_ScadenzaView
AS
WITH TableData
AS (
    SELECT
        id_scadenza,

        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            id_scadenza,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            tipo_scadenza,
            id_sog_commerciale,
            data_scadenza,
            importo,
            stato_scadenza,
            esito_pagamento,
            id_documento,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        tipo_scadenza,
        id_sog_commerciale,
        data_scadenza,
        importo,
        stato_scadenza,
        esito_pagamento,
        id_documento

    FROM COMETA.Scadenza
)
SELECT
    -- Chiavi
    TD.id_scadenza,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.tipo_scadenza,
    TD.id_sog_commerciale,
    TD.data_scadenza,
    TD.importo,
    TD.stato_scadenza,
    TD.esito_pagamento,
    TD.id_documento

FROM TableData TD;
GO

--DROP TABLE IF EXISTS Landing.COMETA_Scadenza;
GO

IF OBJECT_ID(N'Landing.COMETA_Scadenza', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Landing.COMETA_Scadenza FROM Landing.COMETA_ScadenzaView;

    ALTER TABLE Landing.COMETA_Scadenza ALTER COLUMN id_scadenza INT NOT NULL;

    ALTER TABLE Landing.COMETA_Scadenza ADD CONSTRAINT PK_Landing_COMETA_Scadenza PRIMARY KEY CLUSTERED (UpdateDatetime, id_scadenza);

    CREATE UNIQUE NONCLUSTERED INDEX IX_COMETA_Scadenza_BusinessKey ON Landing.COMETA_Scadenza (id_scadenza);
END;
GO

IF OBJECT_ID('COMETA.usp_Merge_Scadenza', 'P') IS NULL EXEC('CREATE PROCEDURE COMETA.usp_Merge_Scadenza AS RETURN 0;');
GO

ALTER PROCEDURE COMETA.usp_Merge_Scadenza
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @IsCometaExportRunning BIT = 1;

    SELECT TOP (1) @IsCometaExportRunning = IsCometaExportRunning FROM COMETA.Semaforo;

    IF (COALESCE(@IsCometaExportRunning, 1) = 1) RETURN -1;

    MERGE INTO Landing.COMETA_Scadenza AS TGT
    USING Landing.COMETA_ScadenzaView (nolock) AS SRC
    ON SRC.id_scadenza = TGT.id_scadenza

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.tipo_scadenza = SRC.tipo_scadenza,
        TGT.id_sog_commerciale = SRC.id_sog_commerciale,
        TGT.data_scadenza = SRC.data_scadenza,
        TGT.importo = SRC.importo,
        TGT.stato_scadenza = SRC.stato_scadenza,
        TGT.esito_pagamento = SRC.esito_pagamento,
        TGT.id_documento = SRC.id_documento

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        id_scadenza,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        tipo_scadenza,
        id_sog_commerciale,
        data_scadenza,
        importo,
        stato_scadenza,
        esito_pagamento,
        id_documento
      )

    WHEN NOT MATCHED BY SOURCE
        AND TGT.IsDeleted = CAST(0 AS BIT)
      THEN UPDATE
        SET TGT.IsDeleted = CAST(1 AS BIT),
        TGT.UpdateDatetime = CURRENT_TIMESTAMP,
        TGT.ChangeHashKey = CONVERT(VARBINARY(20), ''),
        TGT.ChangeHashKeyASCII = ''

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        CASE WHEN Inserted.IsDeleted = CAST(1 AS BIT) THEN N'DELETE' ELSE $action END AS merge_action,
        'Landing.COMETA_Scadenza' AS full_olap_table_name,
        'id_scadenza = ' + CAST(COALESCE(inserted.id_scadenza, deleted.id_scadenza) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;
GO

EXEC COMETA.usp_Merge_Scadenza;
GO

/*
    SCHEMA_NAME > COMETA
    TABLE_NAME > SoggettoCommerciale
*/

/**
 * @table Landing.COMETA_SoggettoCommerciale
 * @description 

 * @depends COMETA.SoggettoCommerciale

SELECT TOP 100 * FROM COMETA.SoggettoCommerciale;
*/

IF OBJECT_ID('Landing.COMETA_SoggettoCommercialeView', 'V') IS NULL EXEC('CREATE VIEW Landing.COMETA_SoggettoCommercialeView AS SELECT 1 AS fld;');
GO

ALTER VIEW Landing.COMETA_SoggettoCommercialeView
AS
WITH TableData
AS (
    SELECT
        id_sog_commerciale,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            id_sog_commerciale,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            codice,
            id_anagrafica,
            tipo,
            id_gruppo_agenti,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        codice,
        id_anagrafica,
        tipo,
        id_gruppo_agenti,
        ROW_NUMBER() OVER (PARTITION BY id_anagrafica ORDER BY id_sog_commerciale DESC) AS rnIDSoggettoCommercialeDESC

    FROM COMETA.SoggettoCommerciale
)
SELECT
    -- Chiavi
    TD.id_sog_commerciale,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.codice COLLATE DATABASE_DEFAULT AS codice,
    TD.id_anagrafica,
    TD.tipo COLLATE DATABASE_DEFAULT AS tipo,
    TD.id_gruppo_agenti,
    TD.rnIDSoggettoCommercialeDESC

FROM TableData TD;
GO

--DROP TABLE IF EXISTS Landing.COMETA_SoggettoCommerciale;
GO

IF OBJECT_ID(N'Landing.COMETA_SoggettoCommerciale', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Landing.COMETA_SoggettoCommerciale FROM Landing.COMETA_SoggettoCommercialeView;

    ALTER TABLE Landing.COMETA_SoggettoCommerciale ALTER COLUMN id_sog_commerciale INT NOT NULL;

    ALTER TABLE Landing.COMETA_SoggettoCommerciale ADD CONSTRAINT PK_Landing_COMETA_SoggettoCommerciale PRIMARY KEY CLUSTERED (UpdateDatetime, id_sog_commerciale);

    ALTER TABLE Landing.COMETA_SoggettoCommerciale ALTER COLUMN codice NVARCHAR(10) NULL;
    ALTER TABLE Landing.COMETA_SoggettoCommerciale ALTER COLUMN id_anagrafica INT NULL;
    ALTER TABLE Landing.COMETA_SoggettoCommerciale ALTER COLUMN tipo CHAR(1) NULL;
    ALTER TABLE Landing.COMETA_SoggettoCommerciale ALTER COLUMN id_gruppo_agenti INT NULL;
    ALTER TABLE Landing.COMETA_SoggettoCommerciale ALTER COLUMN rnIDSoggettoCommercialeDESC INT NOT NULL;

    CREATE UNIQUE NONCLUSTERED INDEX IX_COMETA_SoggettoCommerciale_BusinessKey ON Landing.COMETA_SoggettoCommerciale (id_sog_commerciale);
END;
GO

IF OBJECT_ID('COMETA.usp_Merge_SoggettoCommerciale', 'P') IS NULL EXEC('CREATE PROCEDURE COMETA.usp_Merge_SoggettoCommerciale AS RETURN 0;');
GO

ALTER PROCEDURE COMETA.usp_Merge_SoggettoCommerciale
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @IsCometaExportRunning BIT = 1;

    SELECT TOP (1) @IsCometaExportRunning = IsCometaExportRunning FROM COMETA.Semaforo;

    IF (COALESCE(@IsCometaExportRunning, 1) = 1) RETURN -1;

    MERGE INTO Landing.COMETA_SoggettoCommerciale AS TGT
    USING Landing.COMETA_SoggettoCommercialeView (nolock) AS SRC
    ON SRC.id_sog_commerciale = TGT.id_sog_commerciale

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.codice = SRC.codice,
        TGT.id_anagrafica = SRC.id_anagrafica,
        TGT.tipo = SRC.tipo,
        TGT.id_gruppo_agenti = SRC.id_gruppo_agenti,
        TGT.rnIDSoggettoCommercialeDESC = SRC.rnIDSoggettoCommercialeDESC

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        id_sog_commerciale,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        codice,
        id_anagrafica,
        tipo,
        id_gruppo_agenti,
        rnIDSoggettoCommercialeDESC
      )

    WHEN NOT MATCHED BY SOURCE
        AND TGT.IsDeleted = CAST(0 AS BIT)
      THEN UPDATE
        SET TGT.IsDeleted = CAST(1 AS BIT),
        TGT.UpdateDatetime = CURRENT_TIMESTAMP,
        TGT.ChangeHashKey = CONVERT(VARBINARY(20), ''),
        TGT.ChangeHashKeyASCII = ''

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        CASE WHEN Inserted.IsDeleted = CAST(1 AS BIT) THEN N'DELETE' ELSE $action END AS merge_action,
        'Landing.COMETA_SoggettoCommerciale' AS full_olap_table_name,
        'id_sog_commerciale = ' + CAST(COALESCE(inserted.id_sog_commerciale, deleted.id_sog_commerciale) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;
GO

EXEC COMETA.usp_Merge_SoggettoCommerciale;
GO

/*
    SCHEMA_NAME > COMETA
    TABLE_NAME > Telefono
*/

/**
 * @table Landing.COMETA_Telefono
 * @description 

 * @depends COMETA.Telefono

SELECT TOP 100 * FROM COMETA.Telefono;
*/

IF OBJECT_ID('Landing.COMETA_TelefonoView', 'V') IS NULL EXEC('CREATE VIEW Landing.COMETA_TelefonoView AS SELECT 1 AS fld;');
GO

ALTER VIEW Landing.COMETA_TelefonoView
AS
WITH TableData
AS (
    SELECT
        id_telefono,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            id_telefono,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            id_anagrafica,
            tipo,
            num_riferimento,
            descrizione,
            interlocutore,
            nome,
            cognome,
            ruolo,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        id_anagrafica,
        tipo,
        num_riferimento,
        descrizione,
        interlocutore,
        nome,
        cognome,
        ruolo

    FROM COMETA.Telefono
    WHERE num_riferimento IS NOT NULL
)
SELECT
    -- Chiavi
    TD.id_telefono,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.id_anagrafica,
    TD.tipo,
    LOWER(TD.num_riferimento) COLLATE DATABASE_DEFAULT AS num_riferimento,
    TD.descrizione COLLATE DATABASE_DEFAULT AS descrizione,
    TD.interlocutore COLLATE DATABASE_DEFAULT AS interlocutore,
    TD.nome COLLATE DATABASE_DEFAULT AS nome,
    TD.cognome COLLATE DATABASE_DEFAULT AS cognome,
    TD.ruolo

FROM TableData TD;
GO

--DROP TABLE IF EXISTS Landing.COMETA_Telefono;
GO

IF OBJECT_ID(N'Landing.COMETA_Telefono', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Landing.COMETA_Telefono FROM Landing.COMETA_TelefonoView;

    ALTER TABLE Landing.COMETA_Telefono ALTER COLUMN id_telefono INT NOT NULL;

    ALTER TABLE Landing.COMETA_Telefono ADD CONSTRAINT PK_Landing_COMETA_Telefono PRIMARY KEY CLUSTERED (UpdateDatetime, id_telefono);

    ALTER TABLE Landing.COMETA_Telefono ALTER COLUMN id_anagrafica INT NULL;
    ALTER TABLE Landing.COMETA_Telefono ALTER COLUMN tipo CHAR(1) NULL;
    ALTER TABLE Landing.COMETA_Telefono ALTER COLUMN num_riferimento NVARCHAR(200) NULL;
    ALTER TABLE Landing.COMETA_Telefono ALTER COLUMN descrizione NVARCHAR(400) NULL;

    CREATE UNIQUE NONCLUSTERED INDEX IX_COMETA_Telefono_BusinessKey ON Landing.COMETA_Telefono (id_telefono);
END;
GO

IF OBJECT_ID('COMETA.usp_Merge_Telefono', 'P') IS NULL EXEC('CREATE PROCEDURE COMETA.usp_Merge_Telefono AS RETURN 0;');
GO

ALTER PROCEDURE COMETA.usp_Merge_Telefono
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @IsCometaExportRunning BIT = 1;

    SELECT TOP (1) @IsCometaExportRunning = IsCometaExportRunning FROM COMETA.Semaforo;

    IF (COALESCE(@IsCometaExportRunning, 1) = 1) RETURN -1;

    MERGE INTO Landing.COMETA_Telefono AS TGT
    USING Landing.COMETA_TelefonoView (nolock) AS SRC
    ON SRC.id_telefono = TGT.id_telefono

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.id_anagrafica = SRC.id_anagrafica,
        TGT.tipo = SRC.tipo,
        TGT.num_riferimento = SRC.num_riferimento,
        TGT.descrizione = SRC.descrizione,
        TGT.interlocutore = SRC.interlocutore,
        TGT.nome = SRC.nome,
        TGT.cognome = SRC.cognome,
        TGT.ruolo = SRC.ruolo

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        id_telefono,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        id_anagrafica,
        tipo,
        num_riferimento,
        descrizione,
        interlocutore,
        nome,
        cognome,
        ruolo
      )

    WHEN NOT MATCHED BY SOURCE
        AND TGT.IsDeleted = CAST(0 AS BIT)
      THEN UPDATE
        SET TGT.IsDeleted = CAST(1 AS BIT),
        TGT.UpdateDatetime = CURRENT_TIMESTAMP,
        TGT.ChangeHashKey = CONVERT(VARBINARY(20), ''),
        TGT.ChangeHashKeyASCII = ''

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        CASE WHEN Inserted.IsDeleted = CAST(1 AS BIT) THEN N'DELETE' ELSE $action END AS merge_action,
        'Landing.COMETA_Telefono' AS full_olap_table_name,
        'id_telefono = ' + CAST(COALESCE(inserted.id_telefono, deleted.id_telefono) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;
GO

EXEC COMETA.usp_Merge_Telefono;
GO

/*
    SCHEMA_NAME > COMETA
    TABLE_NAME > Tipo_Fatturazione
*/

/**
 * @table Landing.COMETA_Tipo_Fatturazione
 * @description 

 * @depends COMETA.Tipo_Fatturazione

SELECT TOP 100 * FROM COMETA.Tipo_Fatturazione;
*/

IF OBJECT_ID('Landing.COMETA_Tipo_FatturazioneView', 'V') IS NULL EXEC('CREATE VIEW Landing.COMETA_Tipo_FatturazioneView AS SELECT 1 AS fld;');
GO

ALTER VIEW Landing.COMETA_Tipo_FatturazioneView
AS
WITH TableData
AS (
    SELECT
        id_tipo_fatturazione,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            id_tipo_fatturazione,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            codice,
            descrizione,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        codice,
        descrizione

    FROM COMETA.Tipo_Fatturazione
)
SELECT
    -- Chiavi
    TD.id_tipo_fatturazione,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.codice COLLATE DATABASE_DEFAULT AS codice,
    TD.descrizione COLLATE DATABASE_DEFAULT AS descrizione

FROM TableData TD;
GO

--DROP TABLE IF EXISTS Landing.COMETA_Tipo_Fatturazione;
GO

IF OBJECT_ID(N'Landing.COMETA_Tipo_Fatturazione', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Landing.COMETA_Tipo_Fatturazione FROM Landing.COMETA_Tipo_FatturazioneView;

    ALTER TABLE Landing.COMETA_Tipo_Fatturazione ALTER COLUMN id_tipo_fatturazione INT NOT NULL;

    ALTER TABLE Landing.COMETA_Tipo_Fatturazione ADD CONSTRAINT PK_Landing_COMETA_Tipo_Fatturazione PRIMARY KEY CLUSTERED (UpdateDatetime, id_tipo_fatturazione);

    ALTER TABLE Landing.COMETA_Tipo_Fatturazione ALTER COLUMN codice NVARCHAR(10) NULL;
    ALTER TABLE Landing.COMETA_Tipo_Fatturazione ALTER COLUMN descrizione NVARCHAR(200) NULL;

    CREATE UNIQUE NONCLUSTERED INDEX IX_COMETA_Tipo_Fatturazione_BusinessKey ON Landing.COMETA_Tipo_Fatturazione (id_tipo_fatturazione);
END;
GO

IF OBJECT_ID('COMETA.usp_Merge_Tipo_Fatturazione', 'P') IS NULL EXEC('CREATE PROCEDURE COMETA.usp_Merge_Tipo_Fatturazione AS RETURN 0;');
GO

ALTER PROCEDURE COMETA.usp_Merge_Tipo_Fatturazione
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @IsCometaExportRunning BIT = 1;

    SELECT TOP (1) @IsCometaExportRunning = IsCometaExportRunning FROM COMETA.Semaforo;

    IF (COALESCE(@IsCometaExportRunning, 1) = 1) RETURN -1;

    MERGE INTO Landing.COMETA_Tipo_Fatturazione AS TGT
    USING Landing.COMETA_Tipo_FatturazioneView (nolock) AS SRC
    ON SRC.id_tipo_fatturazione = TGT.id_tipo_fatturazione

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.codice = SRC.codice,
        TGT.descrizione = SRC.descrizione

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        id_Tipo_Fatturazione,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        codice,
        descrizione
      )

    WHEN NOT MATCHED BY SOURCE
        AND TGT.IsDeleted = CAST(0 AS BIT)
      THEN UPDATE
        SET TGT.IsDeleted = CAST(1 AS BIT),
        TGT.UpdateDatetime = CURRENT_TIMESTAMP,
        TGT.ChangeHashKey = CONVERT(VARBINARY(20), ''),
        TGT.ChangeHashKeyASCII = ''

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        CASE WHEN Inserted.IsDeleted = CAST(1 AS BIT) THEN N'DELETE' ELSE $action END AS merge_action,
        'Landing.COMETA_Tipo_Fatturazione' AS full_olap_table_name,
        'id_tipo_fatturazione = ' + CAST(COALESCE(inserted.id_tipo_fatturazione, deleted.id_tipo_fatturazione) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;
GO

EXEC COMETA.usp_Merge_Tipo_Fatturazione;
GO

/**
 * @table Landing.COMETA_MySolutionTrascodifica
 * @description 

 * @depends COMETA.MySolutionTrascodifica

SELECT TOP 100 * FROM COMETA.MySolutionTrascodifica;
*/

IF OBJECT_ID('Landing.COMETA_MySolutionTrascodificaView', 'V') IS NULL EXEC('CREATE VIEW Landing.COMETA_MySolutionTrascodificaView AS SELECT 1 AS fld;');
GO

ALTER VIEW Landing.COMETA_MySolutionTrascodificaView
AS
WITH TableData
AS (
    SELECT
        codice,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            codice,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            tipo,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        tipo

    FROM COMETA.MySolutionTrascodifica
)
SELECT
    -- Chiavi
    TD.codice COLLATE DATABASE_DEFAULT AS codice,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.tipo COLLATE DATABASE_DEFAULT AS tipo

FROM TableData TD;
GO

--DROP TABLE IF EXISTS Landing.COMETA_MySolutionTrascodifica;
GO

IF OBJECT_ID(N'Landing.COMETA_MySolutionTrascodifica', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Landing.COMETA_MySolutionTrascodifica FROM Landing.COMETA_MySolutionTrascodificaView;

    ALTER TABLE Landing.COMETA_MySolutionTrascodifica ALTER COLUMN codice NVARCHAR(40) NOT NULL;

    ALTER TABLE Landing.COMETA_MySolutionTrascodifica ADD CONSTRAINT PK_Landing_COMETA_MySolutionTrascodifica PRIMARY KEY CLUSTERED (UpdateDatetime, codice);

    ALTER TABLE Landing.COMETA_MySolutionTrascodifica ALTER COLUMN tipo NVARCHAR(20) NOT NULL;

    CREATE UNIQUE NONCLUSTERED INDEX IX_COMETA_MySolutionTrascodifica_BusinessKey ON Landing.COMETA_MySolutionTrascodifica (codice);
END;
GO

IF OBJECT_ID('COMETA.usp_Merge_MySolutionTrascodifica', 'P') IS NULL EXEC('CREATE PROCEDURE COMETA.usp_Merge_MySolutionTrascodifica AS RETURN 0;');
GO

ALTER PROCEDURE COMETA.usp_Merge_MySolutionTrascodifica
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @IsCometaExportRunning BIT = 1;

    SELECT TOP (1) @IsCometaExportRunning = IsCometaExportRunning FROM COMETA.Semaforo;

    IF (COALESCE(@IsCometaExportRunning, 1) = 1) RETURN -1;

    MERGE INTO Landing.COMETA_MySolutionTrascodifica AS TGT
    USING Landing.COMETA_MySolutionTrascodificaView (nolock) AS SRC
    ON SRC.codice = TGT.codice

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.IsDeleted = SRC.IsDeleted,
        TGT.tipo = SRC.tipo

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        codice,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        tipo
      )

    WHEN NOT MATCHED BY SOURCE
        AND TGT.IsDeleted = CAST(0 AS BIT)
      THEN UPDATE
        SET TGT.IsDeleted = CAST(1 AS BIT),
        TGT.UpdateDatetime = CURRENT_TIMESTAMP,
        TGT.ChangeHashKey = CONVERT(VARBINARY(20), ''),
        TGT.ChangeHashKeyASCII = ''

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        CASE WHEN Inserted.IsDeleted = CAST(1 AS BIT) THEN N'DELETE' ELSE $action END AS merge_action,
        'Landing.COMETA_MySolutionTrascodifica' AS full_olap_table_name,
        'codice = ' + CAST(COALESCE(inserted.codice, deleted.codice) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;
GO

EXEC COMETA.usp_Merge_MySolutionTrascodifica;
GO
