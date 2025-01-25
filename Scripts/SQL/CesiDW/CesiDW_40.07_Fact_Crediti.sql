USE CesiDW;
GO

/*
SET NOEXEC OFF;
--*/ SET NOEXEC ON;
GO

/*
    SCHEMA_NAME > WEBINARS
    TABLE_NAME > CreditoAutocertificazione
    STAGING_TABLE_NAME > Crediti
*/

/**
 * @table Staging.Crediti
 * @description

 * @depends Landing.WEBINARS_CreditoAutocertificazione
 * @depends Landing.WEBINARS_WeAutocertificazioni
 * @depends Landing.WEBINARS_CreditoTipologia
 * @depends Dim.Data

SELECT TOP (1) * FROM Landing.WEBINARS_CreditoAutocertificazione;
SELECT TOP (1) * FROM Landing.WEBINARS_WeAutocertificazioni;
SELECT TOP (1) * FROM Landing.WEBINARS_CreditoTipologia;
SELECT TOP (1) * FROM Dim.Data;
*/

--DROP TABLE IF EXISTS Staging.Crediti; DELETE FROM audit.tables WHERE provider_name = N'Webinars' AND full_table_name = N'Landing.WEBINARS_CreditoAutocertificazione';
GO

IF NOT EXISTS (SELECT 1 FROM audit.tables WHERE provider_name = N'Webinars' AND full_table_name = N'Landing.WEBINARS_CreditoAutocertificazione')
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
    (   N'Webinars',       -- provider_name - nvarchar(60)
        N'Landing.WEBINARS_CreditoAutocertificazione',      -- full_table_name - sysname
        N'Staging.Crediti',      -- staging_table_name - sysname
        N'Dim.Crediti',      -- datawarehouse_table_name - sysname
        NULL, -- lastupdated_staging - datetime
        NULL  -- lastupdated_local - datetime
    );

END;
GO

CREATE OR ALTER VIEW Staging.CreditiView
AS
WITH TableData
AS (
    SELECT
        CA.ID,

        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            CA.ID,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            A.Corso,
            W.PKCorso,
            A.Nome,
            A.Cognome,
            A.CodiceFiscale,
            A.Professione,
            A.Ordine,
            A.DataCreazione,
            DC.PKData,
            CT.Ordine,
            CT.Tipo,
            DTC.TipoCrediti,
            DSC.StatoCrediti,
            CC.CodiceMateria,
            CA.Crediti,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,

        --CA.AutocertificazioneID,
        --A.ID,
        A.Corso AS IDCorso,
        COALESCE(W.PKCorso, CASE WHEN A.Corso = N'' THEN -1 ELSE -101 END) AS PKCorso,
        UPPER(A.Nome) AS Nome,
        UPPER(A.Cognome) AS Cognome,
        UPPER(A.CodiceFiscale) AS CodiceFiscale,
        COALESCE(A.Professione, N'') AS Professione,
        COALESCE(A.Ordine, N'') AS Ordine,
        --A.DataCreazione,
        COALESCE(DC.PKData, CAST('19000101' AS DATE)) AS PKDataCreazione,
        LOWER(A.Email) AS EMail,

        --CA.CreditoTipologiaID,
        --CT.ID,
        CT.Ordine AS EnteAccreditante,
        --CT.Tipo AS TipoCrediti,
        COALESCE(DTC.TipoCrediti, N'<???>') AS TipoCrediti,

        --CA.Stato,
        COALESCE(DSC.StatoCrediti, N'<???>') AS StatoCrediti,

        COALESCE(CC.CodiceMateria, N'') AS CodiceMateria,

        CA.Crediti

    FROM Landing.WEBINARS_CreditoAutocertificazione CA
    INNER JOIN Landing.WEBINARS_WeAutocertificazioni A ON A.ID = CA.AutocertificazioneID
        AND A.IsDeleted = CAST(0 AS BIT)
    INNER JOIN Landing.WEBINARS_CreditoTipologia CT ON CT.ID = CA.CreditoTipologiaID
        AND CT.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Landing.WEBINARS_CreditoCorso CC ON CC.Id = CA.CreditoCorsoID
        AND CC.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Dim.Corso W ON W.IDCorso = A.Corso
    LEFT JOIN Dim.Data DC ON DC.PKData = A.DataCreazione
    LEFT JOIN Import.Decod_StatoCrediti DSC ON DSC.IDStatoCrediti = CA.Stato
    LEFT JOIN Import.Decod_TipoCrediti DTC ON DTC.IDTipoCrediti = CT.Tipo
    WHERE CA.IsDeleted = CAST(0 AS BIT)
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
    TD.IDCorso,
    TD.PKCorso,
    TD.Nome,
    TD.Cognome,
    TD.CodiceFiscale,
    TD.Professione,
    TD.Ordine,
    TD.PKDataCreazione,
    TD.Email,
    TD.EnteAccreditante,
    TD.TipoCrediti,
    TD.StatoCrediti,
    TD.CodiceMateria,

    -- Misure
    TD.Crediti

FROM TableData TD;
GO

--IF OBJECT_ID(N'Staging.Crediti', N'U') IS NOT NULL DROP TABLE Staging.Crediti;
GO

IF OBJECT_ID(N'Staging.Crediti', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Staging.Crediti FROM Staging.CreditiView;

    ALTER TABLE Staging.Crediti ADD CONSTRAINT PK_Landing_WEBINARS_CreditoAutocertificazione PRIMARY KEY CLUSTERED (UpdateDatetime, ID);

    ALTER TABLE Staging.Crediti ALTER COLUMN PKCorso INT NOT NULL;
    ALTER TABLE Staging.Crediti ALTER COLUMN PKDataCreazione DATE NOT NULL;
    ALTER TABLE Staging.Crediti ALTER COLUMN CodiceMateria NVARCHAR(50) NOT NULL;

    CREATE UNIQUE NONCLUSTERED INDEX IX_WEBINARS_CreditoAutocertificazione_BusinessKey ON Staging.Crediti (ID);
END;
GO

IF OBJECT_ID(N'Staging.usp_Reload_Crediti', N'P') IS NULL EXEC('CREATE PROCEDURE Staging.usp_Reload_Crediti AS RETURN 0;');
GO

ALTER PROCEDURE Staging.usp_Reload_Crediti
AS
BEGIN

    SET NOCOUNT ON;

    DECLARE @lastupdated_staging DATETIME;
    DECLARE @provider_name NVARCHAR(60) = N'Webinars';
    DECLARE @full_table_name sysname = N'Landing.WEBINARS_CreditoAutocertificazione';

    SELECT TOP 1 @lastupdated_staging = lastupdated_staging
    FROM audit.tables
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    IF (@lastupdated_staging IS NULL) SET @lastupdated_staging = CAST('19000101' AS DATETIME);

    BEGIN TRANSACTION

    TRUNCATE TABLE Staging.Crediti;

    INSERT INTO Staging.Crediti
    SELECT * FROM Staging.CreditiView
    WHERE UpdateDatetime > @lastupdated_staging;

    SELECT @lastupdated_staging = MAX(UpdateDatetime) FROM Staging.Crediti;

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

EXEC Staging.usp_Reload_Crediti;
GO

--DROP TABLE IF EXISTS Fact.Crediti; DROP SEQUENCE IF EXISTS dbo.seq_Fact_Crediti;
GO

IF OBJECT_ID('dbo.seq_Fact_Crediti', 'SO') IS NULL
BEGIN

    CREATE SEQUENCE dbo.seq_Fact_Crediti START WITH 1;

END;
GO

IF OBJECT_ID('Fact.Crediti', 'U') IS NULL
BEGIN

    CREATE TABLE Fact.Crediti (
        PKCrediti INT NOT NULL CONSTRAINT PK_Fact_Crediti PRIMARY KEY CLUSTERED CONSTRAINT DFT_Fact_Crediti_PKCrediti DEFAULT (NEXT VALUE FOR dbo.seq_Fact_Crediti),

	    ID INT NOT NULL,
        PKCorso INT NOT NULL CONSTRAINT FK_Fact_Crediti_PKCorso FOREIGN KEY REFERENCES Dim.Corso (PKCorso),
        PKDataCreazione DATE NOT NULL CONSTRAINT FK_Fact_Crediti_PKDataCreazione FOREIGN KEY REFERENCES Dim.Data (PKData),
        AnnoCreazione AS YEAR(PKDataCreazione),

	    HistoricalHashKey VARBINARY(20) NULL,
	    ChangeHashKey VARBINARY(20) NULL,
	    HistoricalHashKeyASCII VARCHAR(34) NULL,
	    ChangeHashKeyASCII VARCHAR(34) NULL,
	    InsertDatetime DATETIME NOT NULL,
	    UpdateDatetime DATETIME NOT NULL,
	    IsDeleted BIT NOT NULL,

	    IDCorso NVARCHAR(100) NOT NULL,
	    Nome NVARCHAR(100) NOT NULL,
	    Cognome NVARCHAR(100) NOT NULL,
	    CodiceFiscale NVARCHAR(20) NOT NULL,
        EMail NVARCHAR(100) NOT NULL,
	    Professione NVARCHAR(100) NOT NULL,
	    Ordine NVARCHAR(100) NOT NULL,
	    EnteAccreditante NVARCHAR(100) NOT NULL,
	    TipoCrediti NVARCHAR(100) NOT NULL,
	    StatoCrediti NVARCHAR(40) NOT NULL,
        CodiceMateria NVARCHAR(50) NOT NULL,

	    Crediti INT NOT NULL
    );

    CREATE NONCLUSTERED INDEX IX_Fact_Crediti_AnnoCreazione_CodiceFiscale ON Fact.Crediti (AnnoCreazione, CodiceFiscale);

    ALTER SEQUENCE dbo.seq_Fact_Crediti RESTART WITH 1;

END;
GO

IF OBJECT_ID(N'Fact.usp_Merge_Crediti', N'P') IS NULL EXEC('CREATE PROCEDURE Fact.usp_Merge_Crediti AS RETURN 0;');
GO

ALTER PROCEDURE Fact.usp_Merge_Crediti
AS
BEGIN

    SET NOCOUNT ON;

    BEGIN TRANSACTION 

    DECLARE @provider_name NVARCHAR(60) = N'Webinars';
    DECLARE @full_table_name sysname = N'Import.Crediti';

    MERGE INTO Fact.Crediti AS TGT
    USING Staging.Crediti (nolock) AS SRC
    ON SRC.ID = TGT.ID

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.IsDeleted = SRC.IsDeleted,
        TGT.IDCorso = SRC.IDCorso,
        TGT.Nome = SRC.Nome,
        TGT.Cognome = SRC.Cognome,
        TGT.CodiceFiscale = SRC.CodiceFiscale,
        TGT.EMail = SRC.EMail,
        TGT.Professione = SRC.Professione,
        TGT.Ordine = SRC.Ordine,
        TGT.EnteAccreditante = SRC.EnteAccreditante,
        TGT.TipoCrediti = SRC.TipoCrediti,
        TGT.StatoCrediti = SRC.StatoCrediti,
        TGT.CodiceMateria = SRC.CodiceMateria,
        TGT.Crediti = SRC.Crediti

    WHEN NOT MATCHED
      THEN INSERT (
        --PKCrediti,
        ID,
        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
        IDCorso,
        PKCorso,
        Nome,
        Cognome,
        CodiceFiscale,
        EMail,
        Professione,
        Ordine,
        PKDataCreazione,
        EnteAccreditante,
        TipoCrediti,
        StatoCrediti,
        CodiceMateria,
        Crediti
    ) VALUES (
        ID,
        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
        IDCorso,
        PKCorso,
        Nome,
        Cognome,
        CodiceFiscale,
        EMail,
        Professione,
        Ordine,
        PKDataCreazione,
        EnteAccreditante,
        TipoCrediti,
        StatoCrediti,
        CodiceMateria,
        Crediti
    )

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        $action AS merge_action,
        'Staging.Crediti' AS full_olap_table_name,
        'ID = ' + CAST(COALESCE(inserted.ID, deleted.ID) AS NVARCHAR(1000)) AS primary_key_description
    INTO audit.merge_log_details;

    DELETE FROM Fact.Crediti
    WHERE IsDeleted = CAST(1 AS BIT);

    UPDATE audit.tables
    SET lastupdated_local = lastupdated_staging
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    COMMIT TRANSACTION;

END;
GO

EXEC Fact.usp_Merge_Crediti;
GO
