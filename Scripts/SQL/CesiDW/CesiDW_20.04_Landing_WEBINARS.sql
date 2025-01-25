USE CesiDW;
GO

/*
SET NOEXEC OFF;
--*/ SET NOEXEC ON;
GO

/**
 * @table Landing.WEBINARS_WeAutocertificazioni
 * @description 

 * @depends WEBINARS.WeAutocertificazioni

SELECT TOP (100) * FROM WEBINARS.WeAutocertificazioni;
*/

CREATE OR ALTER VIEW Landing.WEBINARS_WeAutocertificazioniView
AS
WITH TableData
AS (
    SELECT
        ID,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            ID,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            Corso,
            Nome,
            Cognome,
            CodiceFiscale,
            Professione,
            Ordine,
            CONVERT(DATE, CreatedOn),
            EMail,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        Corso,
        Nome,
        Cognome,
        CodiceFiscale,
        Professione,
        Ordine,
        CONVERT(DATE, CreatedOn) AS DataCreazione,
        EMail

    FROM WEBINARS.WeAutocertificazioni
)
SELECT
    -- Chiavi
    TD.ID,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.Corso COLLATE DATABASE_DEFAULT AS Corso,
    TD.Nome COLLATE DATABASE_DEFAULT AS Nome,
    TD.Cognome COLLATE DATABASE_DEFAULT AS Cognome,
    TD.CodiceFiscale COLLATE DATABASE_DEFAULT AS CodiceFiscale,
    TD.Professione COLLATE DATABASE_DEFAULT AS Professione,
    TD.Ordine COLLATE DATABASE_DEFAULT AS Ordine,
    TD.DataCreazione,
    TD.EMail COLLATE DATABASE_DEFAULT AS Email

FROM TableData TD;
GO

--DROP TABLE IF EXISTS Landing.WEBINARS_WeAutocertificazioni;
GO

IF OBJECT_ID(N'Landing.WEBINARS_WeAutocertificazioni', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Landing.WEBINARS_WeAutocertificazioni FROM Landing.WEBINARS_WeAutocertificazioniView;

    ALTER TABLE Landing.WEBINARS_WeAutocertificazioni ALTER COLUMN ID INT NOT NULL;

    ALTER TABLE Landing.WEBINARS_WeAutocertificazioni ADD CONSTRAINT PK_Landing_WEBINARS_WeAutocertificazioni PRIMARY KEY CLUSTERED (UpdateDatetime, ID);

    --ALTER TABLE Landing.WEBINARS_WeAutocertificazioni ALTER COLUMN rag_soc_1 NVARCHAR(60) NULL;

    CREATE UNIQUE NONCLUSTERED INDEX IX_WEBINARS_WeAutocertificazioni_BusinessKey ON Landing.WEBINARS_WeAutocertificazioni (ID);
END;
GO

CREATE OR ALTER PROCEDURE WEBINARS.usp_Merge_WeAutocertificazioni
AS
BEGIN
    SET NOCOUNT ON;

    MERGE INTO Landing.WEBINARS_WeAutocertificazioni AS TGT
    USING Landing.WEBINARS_WeAutocertificazioniView (nolock) AS SRC
    ON SRC.ID = TGT.ID

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.IsDeleted = SRC.IsDeleted,
        TGT.Corso = SRC.Corso,
        TGT.Nome = SRC.Nome,
        TGT.Cognome = SRC.Cognome,
        TGT.CodiceFiscale = SRC.CodiceFiscale,
        TGT.Professione = SRC.Professione,
        TGT.Ordine = SRC.Ordine,
        TGT.DataCreazione = SRC.DataCreazione,
        TGT.EMail = SRC.EMail

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        ID,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        Corso,
        Nome,
        Cognome,
        CodiceFiscale,
        Professione,
        Ordine,
        DataCreazione,
        EMail
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
        'Landing.WEBINARS_WeAutocertificazioni' AS full_olap_table_name,
        'ID = ' + CAST(COALESCE(inserted.ID, deleted.ID) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;
GO

EXEC WEBINARS.usp_Merge_WeAutocertificazioni;
GO

/**
 * @table Landing.WEBINARS_WeBinars
 * @description 

 * @depends WEBINARS.WeBinars

SELECT TOP (100) * FROM WEBINARS.WeBinars;
*/

CREATE OR ALTER VIEW Landing.WEBINARS_WeBinarsView
AS
WITH TableData
AS (
    SELECT
        Source,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            Source,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            VideoStartDate,
            VideoTitle,
            CourseTitle,
            CourseType,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        VideoStartDate,
        VideoTitle,
        CourseTitle,
        CourseType

    FROM WEBINARS.WeBinars
)
SELECT
    -- Chiavi
    TD.Source COLLATE DATABASE_DEFAULT AS Source,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.VideoStartDate,
    TD.VideoTitle COLLATE DATABASE_DEFAULT AS VideoTitle,
    TD.CourseTitle COLLATE DATABASE_DEFAULT AS CourseTitle,
    TD.CourseType COLLATE DATABASE_DEFAULT AS CourseType

FROM TableData TD;
GO

--DROP TABLE IF EXISTS Landing.WEBINARS_WeBinars;
GO

IF OBJECT_ID(N'Landing.WEBINARS_WeBinars', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Landing.WEBINARS_WeBinars FROM Landing.WEBINARS_WeBinarsView;

    ALTER TABLE Landing.WEBINARS_WeBinars ALTER COLUMN Source NVARCHAR(50) NOT NULL;

    ALTER TABLE Landing.WEBINARS_WeBinars ADD CONSTRAINT PK_Landing_WEBINARS_WeBinars PRIMARY KEY CLUSTERED (UpdateDatetime, Source);

    --ALTER TABLE Landing.WEBINARS_WeBinars ALTER COLUMN rag_soc_1 NVARCHAR(60) NULL;

    CREATE UNIQUE NONCLUSTERED INDEX IX_WEBINARS_WeBinars_BusinessKey ON Landing.WEBINARS_WeBinars (Source);
END;
GO

CREATE OR ALTER PROCEDURE WEBINARS.usp_Merge_WeBinars
AS
BEGIN
    SET NOCOUNT ON;

    MERGE INTO Landing.WEBINARS_WeBinars AS TGT
    USING Landing.WEBINARS_WeBinarsView (nolock) AS SRC
    ON SRC.Source = TGT.Source

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.IsDeleted = SRC.IsDeleted,
        TGT.VideoStartDate = SRC.VideoStartDate,
        TGT.VideoTitle = SRC.VideoTitle,
        TGT.CourseTitle = SRC.CourseTitle,
        TGT.CourseType = SRC.CourseType

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        Source,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        VideoStartDate,
        VideoTitle,
        CourseTitle,
        CourseType
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
        'Landing.WEBINARS_WeBinars' AS full_olap_table_name,
        'Source = ' + CAST(COALESCE(inserted.Source, deleted.Source) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;
GO

EXEC WEBINARS.usp_Merge_WeBinars;
GO

/**
 * @table Landing.WEBINARS_CreditoAutocertificazione
 * @description 

 * @depends WEBINARS.CreditoAutocertificazione

SELECT TOP (100) * FROM WEBINARS.CreditoAutocertificazione;
*/

CREATE OR ALTER VIEW Landing.WEBINARS_CreditoAutocertificazioneView
AS
WITH TableData
AS (
    SELECT
        ID,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            ID,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            AutocertificazioneID,
            CreditoTipologiaID,
            CreditoCorsoID,
            Crediti,
            Stato,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        AutocertificazioneID,
        CreditoTipologiaID,
        CreditoCorsoID,
        Crediti,
        Stato

    FROM WEBINARS.CreditoAutocertificazione
)
SELECT
    -- Chiavi
    TD.ID,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.AutocertificazioneID,
    TD.CreditoTipologiaID,
    TD.CreditoCorsoID,
    TD.Stato,

    -- Misure
    TD.Crediti

FROM TableData TD;
GO

--DROP TABLE IF EXISTS Landing.WEBINARS_CreditoAutocertificazione;
GO

IF OBJECT_ID(N'Landing.WEBINARS_CreditoAutocertificazione', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Landing.WEBINARS_CreditoAutocertificazione FROM Landing.WEBINARS_CreditoAutocertificazioneView;

    ALTER TABLE Landing.WEBINARS_CreditoAutocertificazione ALTER COLUMN ID INT NOT NULL;

    ALTER TABLE Landing.WEBINARS_CreditoAutocertificazione ADD CONSTRAINT PK_Landing_WEBINARS_CreditoAutocertificazione PRIMARY KEY CLUSTERED (UpdateDatetime, ID);

    --ALTER TABLE Landing.WEBINARS_CreditoAutocertificazione ALTER COLUMN rag_soc_1 NVARCHAR(60) NULL;

    CREATE UNIQUE NONCLUSTERED INDEX IX_WEBINARS_CreditoAutocertificazione_BusinessKey ON Landing.WEBINARS_CreditoAutocertificazione (ID);
END;
GO

CREATE OR ALTER PROCEDURE WEBINARS.usp_Merge_CreditoAutocertificazione
AS
BEGIN
    SET NOCOUNT ON;

    MERGE INTO Landing.WEBINARS_CreditoAutocertificazione AS TGT
    USING Landing.WEBINARS_CreditoAutocertificazioneView (nolock) AS SRC
    ON SRC.ID = TGT.ID

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.IsDeleted = SRC.IsDeleted,
        TGT.AutocertificazioneID = SRC.AutocertificazioneID,
        TGT.CreditoTipologiaID = SRC.CreditoTipologiaID,
        TGT.CreditoCorsoID = SRC.CreditoCorsoID,
        TGT.Stato = SRC.Stato,
        TGT.Crediti = SRC.Crediti

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        ID,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        AutocertificazioneID,
        CreditoTipologiaID,
        CreditoCorsoID,
        Stato,
        Crediti
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
        'Landing.WEBINARS_CreditoAutocertificazione' AS full_olap_table_name,
        'ID = ' + CAST(COALESCE(inserted.ID, deleted.ID) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;
GO

EXEC WEBINARS.usp_Merge_CreditoAutocertificazione;
GO

/**
 * @table Landing.WEBINARS_CreditoTipologia
 * @description 

 * @depends WEBINARS.CreditoTipologia

SELECT TOP (100) * FROM WEBINARS.CreditoTipologia;
*/

CREATE OR ALTER VIEW Landing.WEBINARS_CreditoTipologiaView
AS
WITH TableData
AS (
    SELECT
        ID,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            ID,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            Ordine,
            Tipo,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        Ordine,
        Tipo

    FROM WEBINARS.CreditoTipologia
)
SELECT
    -- Chiavi
    TD.ID,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.Ordine COLLATE DATABASE_DEFAULT AS Ordine,
    TD.Tipo COLLATE DATABASE_DEFAULT AS Tipo

FROM TableData TD;
GO

--DROP TABLE IF EXISTS Landing.WEBINARS_CreditoTipologia;
GO

IF OBJECT_ID(N'Landing.WEBINARS_CreditoTipologia', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Landing.WEBINARS_CreditoTipologia FROM Landing.WEBINARS_CreditoTipologiaView;

    ALTER TABLE Landing.WEBINARS_CreditoTipologia ALTER COLUMN ID INT NOT NULL;

    ALTER TABLE Landing.WEBINARS_CreditoTipologia ADD CONSTRAINT PK_Landing_WEBINARS_CreditoTipologia PRIMARY KEY CLUSTERED (UpdateDatetime, ID);

    --ALTER TABLE Landing.WEBINARS_CreditoTipologia ALTER COLUMN rag_soc_1 NVARCHAR(60) NULL;

    CREATE UNIQUE NONCLUSTERED INDEX IX_WEBINARS_CreditoTipologia_BusinessKey ON Landing.WEBINARS_CreditoTipologia (ID);
END;
GO

CREATE OR ALTER PROCEDURE WEBINARS.usp_Merge_CreditoTipologia
AS
BEGIN
    SET NOCOUNT ON;

    MERGE INTO Landing.WEBINARS_CreditoTipologia AS TGT
    USING Landing.WEBINARS_CreditoTipologiaView (nolock) AS SRC
    ON SRC.ID = TGT.ID

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.IsDeleted = SRC.IsDeleted,
        TGT.Ordine = SRC.Ordine,
        TGT.Tipo = SRC.Tipo

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        ID,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        Ordine,
        Tipo
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
        'Landing.WEBINARS_CreditoTipologia' AS full_olap_table_name,
        'ID = ' + CAST(COALESCE(inserted.ID, deleted.ID) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;
GO

EXEC WEBINARS.usp_Merge_CreditoTipologia;
GO

/**
 * @table Landing.WEBINARS_CreditoCorso
 * @description 

 * @depends WEBINARS.CreditoCorso

SELECT TOP (100) * FROM WEBINARS.CreditoCorso;
*/

CREATE OR ALTER VIEW Landing.WEBINARS_CreditoCorsoView
AS
WITH TableData
AS (
    SELECT
        Id,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            Id,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            CreditoTipologiaID,
            WebinarSource,
            Autocertificazione,
            Crediti,
            Ora,
            CodiceMateria,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        CreditoTipologiaID,
        WebinarSource,
        Autocertificazione,
        Crediti,
        Ora,
        CodiceMateria

    FROM WEBINARS.CreditoCorso
)
SELECT
    -- Chiavi
    0+TD.Id AS Id,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.CreditoTipologiaID,
    TD.WebinarSource COLLATE DATABASE_DEFAULT AS WebinarSource,
    TD.Autocertificazione,
    TD.Crediti,
    TD.Ora,
    TD.CodiceMateria COLLATE DATABASE_DEFAULT AS CodiceMateria

FROM TableData TD;
GO

--DROP TABLE IF EXISTS Landing.WEBINARS_CreditoCorso;
GO

IF OBJECT_ID(N'Landing.WEBINARS_CreditoCorso', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Landing.WEBINARS_CreditoCorso FROM Landing.WEBINARS_CreditoCorsoView;

    ALTER TABLE Landing.WEBINARS_CreditoCorso ALTER COLUMN Id INT NOT NULL;

    ALTER TABLE Landing.WEBINARS_CreditoCorso ADD CONSTRAINT PK_Landing_WEBINARS_CreditoCorso PRIMARY KEY CLUSTERED (UpdateDatetime, Id);

    ALTER TABLE Landing.WEBINARS_CreditoCorso ALTER COLUMN Autocertificazione BIT NOT NULL;

    CREATE UNIQUE NONCLUSTERED INDEX IX_WEBINARS_CreditoCorso_BusinessKey ON Landing.WEBINARS_CreditoCorso (Id);
END;
GO

CREATE OR ALTER PROCEDURE WEBINARS.usp_Merge_CreditoCorso
AS
BEGIN
    SET NOCOUNT ON;

    MERGE INTO Landing.WEBINARS_CreditoCorso AS TGT
    USING Landing.WEBINARS_CreditoCorsoView (nolock) AS SRC
    ON SRC.Id = TGT.Id

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.IsDeleted = SRC.IsDeleted,
        TGT.CreditoTipologiaID = SRC.CreditoTipologiaID,
        TGT.WebinarSource = SRC.WebinarSource,
        TGT.Autocertificazione = SRC.Autocertificazione,
        TGT.Crediti = SRC.Crediti,
        TGT.Ora = SRC.Ora,
        TGT.CodiceMateria = SRC.CodiceMateria

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        Id,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        CreditoTipologiaID,
        WebinarSource,
        Autocertificazione,
        Crediti,
        Ora,
        CodiceMateria
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
        'Landing.WEBINARS_CreditoCorso' AS full_olap_table_name,
        'Id = ' + CAST(COALESCE(inserted.Id, deleted.Id) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;
GO

EXEC WEBINARS.usp_Merge_CreditoCorso;
GO
