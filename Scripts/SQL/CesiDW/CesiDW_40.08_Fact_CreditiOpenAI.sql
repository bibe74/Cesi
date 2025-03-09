USE CesiDW;
GO

/*
SET NOEXEC OFF;
--*/ SET NOEXEC ON;
GO

/*
    SCHEMA_NAME > GPT
    TABLE_NAME > OpenAICredito
    STAGING_TABLE_NAME > CreditiOpenAI
*/

/**
 * @table Staging.CreditiOpenAI
 * @description

*/

--DROP TABLE IF EXISTS Staging.CreditiOpenAI; DELETE FROM audit.tables WHERE provider_name = N'GPT' AND full_table_name = N'Landing.GPT_OpenAICredito';
GO

IF NOT EXISTS (SELECT 1 FROM audit.tables WHERE provider_name = N'GPT' AND full_table_name = N'Landing.GPT_OpenAICredito')
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
    (   N'GPT',       -- provider_name - nvarchar(60)
        N'Landing.GPT_OpenAICredito',      -- full_table_name - sysname
        N'Staging.CreditiOpenAI',      -- staging_table_name - sysname
        N'Fact.CreditiOpenAI',      -- datawarehouse_table_name - sysname
        NULL, -- lastupdated_staging - datetime
        NULL  -- lastupdated_local - datetime
    );

END;
GO

CREATE OR ALTER VIEW Staging.CreditiOpenAIView
AS
WITH OpenAICrediti
AS (
    SELECT
        C.Email,
        SUM(CASE WHEN ICA.IsAcquisto = CAST(1 AS BIT) THEN CR.Quantita ELSE 0 END) AS CreditiAcquistati,
        SUM(CASE WHEN ICA.IsConsumo = CAST(1 AS BIT) AND CR.PartitaId IS NOT NULL THEN CR.Quantita ELSE 0 END) AS CreditiConsumati,
        SUM(CASE WHEN ICA.IsConsumo = CAST(1 AS BIT) AND CR.PartitaId IS NULL THEN CR.Quantita ELSE 0 END) AS CreditiFuoriOrdine,
        SUM(CR.Quantita) AS CreditiResidui,
        MIN(P.datascadenza) AS DataScadenzaOrdine

    FROM Landing.GPT_OpenAICredito CR
    INNER JOIN Landing.GPT_OpenAICliente C ON C.Id = CR.ClienteId
        AND C.IsDeleted = CAST(0 AS BIT)
    INNER JOIN Landing.GPT_OpenAICausale CA ON CA.Id = CR.CausaleId
        AND CA.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Import.OpenAICausale ICA ON ICA.Codice = CA.Codice
    LEFT JOIN Landing.GPT_OpenAIPartita P ON P.Id = CR.PartitaId
    WHERE CR.IsDeleted = CAST(0 AS BIT)
    GROUP BY C.Email
),
EmailClienteDettaglio
AS (
    SELECT
        C.Email,
        C.PKCliente,
        ROW_NUMBER() OVER (PARTITION BY C.Email ORDER BY C.PKCliente DESC) AS rn

    FROM Dim.Cliente C
    WHERE C.IsDeleted = CAST(0 AS BIT)
        AND C.Email LIKE N'%@%'

),
TableData
AS (
    SELECT
        --OAIC.Email,
        ECD.PKCliente,

        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            ECD.PKCliente,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            DSO.PKData,
            OAIC.CreditiAcquistati,
            OAIC.CreditiConsumati,
            OAIC.CreditiFuoriOrdine,
            OAIC.CreditiResidui,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,

        --OAIC.scadenza_ordine,
        DSO.PKData AS PKDataScadenzaOrdine,
        OAIC.CreditiAcquistati,
        OAIC.CreditiConsumati,
        OAIC.CreditiFuoriOrdine,
        OAIC.CreditiResidui

    FROM OpenAICrediti OAIC
    INNER JOIN Dim.Data DSO ON DSO.PKData = OAIC.DataScadenzaOrdine
    INNER JOIN EmailClienteDettaglio ECD ON ECD.Email = OAIC.Email
        AND ECD.rn = 1
)
SELECT
    -- Chiavi
    TD.PKCliente,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.PKDataScadenzaOrdine,

    -- Misure
    TD.CreditiAcquistati,
    TD.CreditiConsumati,
    TD.CreditiFuoriOrdine,
    TD.CreditiResidui

FROM TableData TD;
GO

--IF OBJECT_ID(N'Staging.CreditiOpenAI', N'U') IS NOT NULL DROP TABLE Staging.CreditiOpenAI;
GO

IF OBJECT_ID(N'Staging.CreditiOpenAI', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Staging.CreditiOpenAI FROM Staging.CreditiOpenAIView;

    ALTER TABLE Staging.CreditiOpenAI ADD CONSTRAINT PK_Landing_GPT_OpenAICredito PRIMARY KEY CLUSTERED (UpdateDatetime, PKCliente);

    ALTER TABLE Staging.CreditiOpenAI ALTER COLUMN PKDataScadenzaOrdine DATE NOT NULL;

    CREATE UNIQUE NONCLUSTERED INDEX IX_GPT_OpenAICredito_BusinessKey ON Staging.CreditiOpenAI (PKCliente);
END;
GO

IF OBJECT_ID(N'Staging.usp_Reload_CreditiOpenAI', N'P') IS NULL EXEC('CREATE PROCEDURE Staging.usp_Reload_CreditiOpenAI AS RETURN 0;');
GO

ALTER PROCEDURE Staging.usp_Reload_CreditiOpenAI
AS
BEGIN

    SET NOCOUNT ON;

    DECLARE @lastupdated_staging DATETIME;
    DECLARE @provider_name NVARCHAR(60) = N'GPT';
    DECLARE @full_table_name sysname = N'Landing.GPT_OpenAICredito';

    SELECT TOP 1 @lastupdated_staging = lastupdated_staging
    FROM audit.tables
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    IF (@lastupdated_staging IS NULL) SET @lastupdated_staging = CAST('19000101' AS DATETIME);

    BEGIN TRANSACTION

    TRUNCATE TABLE Staging.CreditiOpenAI;

    INSERT INTO Staging.CreditiOpenAI
    SELECT * FROM Staging.CreditiOpenAIView
    WHERE UpdateDatetime > @lastupdated_staging;

    SELECT @lastupdated_staging = MAX(UpdateDatetime) FROM Staging.CreditiOpenAI;

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

EXEC Staging.usp_Reload_CreditiOpenAI;
GO

--DROP TABLE IF EXISTS Fact.CreditiOpenAI; DROP SEQUENCE IF EXISTS dbo.seq_Fact_Crediti;
GO

IF OBJECT_ID('dbo.seq_Fact_CreditiOpenAI', 'SO') IS NULL
BEGIN

    CREATE SEQUENCE dbo.seq_Fact_CreditiOpenAI START WITH 1;

END;
GO

IF OBJECT_ID('Fact.CreditiOpenAI', 'U') IS NULL
BEGIN

    CREATE TABLE Fact.CreditiOpenAI (
        PKCreditiOpenAI INT NOT NULL CONSTRAINT PK_Fact_CreditiOpenAI PRIMARY KEY CLUSTERED CONSTRAINT DFT_Fact_CreditiOpenAI_PKCreditiOpenAI DEFAULT (NEXT VALUE FOR dbo.seq_Fact_CreditiOpenAI),

	    PKCliente INT NOT NULL CONSTRAINT FK_Fact_CreditiOpenAI_PKCliente FOREIGN KEY REFERENCES Dim.Cliente (PKCliente),

	    HistoricalHashKey VARBINARY(20) NULL,
	    ChangeHashKey VARBINARY(20) NULL,
	    HistoricalHashKeyASCII VARCHAR(34) NULL,
	    ChangeHashKeyASCII VARCHAR(34) NULL,
	    InsertDatetime DATETIME NOT NULL,
	    UpdateDatetime DATETIME NOT NULL,
	    IsDeleted BIT NOT NULL,

	    PKDataScadenzaOrdine DATE NOT NULL CONSTRAINT FK_Fact_CreditiOpenAI_PKDataScadenzaOrdine FOREIGN KEY REFERENCES Dim.Data (PKData),
	    CreditiAcquistati INT NULL,
	    CreditiConsumati INT NULL,
	    CreditiFuoriOrdine INT NULL,
	    CreditiResidui INT NULL
    );

    ALTER SEQUENCE dbo.seq_Fact_CreditiOpenAI RESTART WITH 1;

END;
GO

IF OBJECT_ID(N'Fact.usp_Merge_CreditiOpenAI', N'P') IS NULL EXEC('CREATE PROCEDURE Fact.usp_Merge_CreditiOpenAI AS RETURN 0;');
GO

ALTER PROCEDURE Fact.usp_Merge_CreditiOpenAI
AS
BEGIN

    SET NOCOUNT ON;

    BEGIN TRANSACTION 

    DECLARE @provider_name NVARCHAR(60) = N'GPT';
    DECLARE @full_table_name sysname = N'Import.Crediti';

    MERGE INTO Fact.CreditiOpenAI AS TGT
    USING Staging.CreditiOpenAI (nolock) AS SRC
    ON SRC.PKCliente = TGT.PKCliente

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.IsDeleted = SRC.IsDeleted,
        TGT.PKDataScadenzaOrdine = SRC.PKDataScadenzaOrdine,
        TGT.CreditiAcquistati = SRC.CreditiAcquistati,
        TGT.CreditiConsumati = SRC.CreditiConsumati,
        TGT.CreditiFuoriOrdine = SRC.CreditiFuoriOrdine,
        TGT.CreditiResidui = SRC.CreditiResidui

    WHEN NOT MATCHED
      THEN INSERT (
        --PKCreditiOpenAI,
        PKCliente,
        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
        PKDataScadenzaOrdine,
        CreditiAcquistati,
        CreditiConsumati,
        CreditiFuoriOrdine,
        CreditiResidui
    ) VALUES (
        PKCliente,
        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
        PKDataScadenzaOrdine,
        CreditiAcquistati,
        CreditiConsumati,
        CreditiFuoriOrdine,
        CreditiResidui
    )

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        $action AS merge_action,
        'Staging.CreditiOpenAI' AS full_olap_table_name,
        'PKCliente = ' + CAST(COALESCE(inserted.PKCliente, deleted.PKCliente) AS NVARCHAR(1000)) AS primary_key_description
    INTO audit.merge_log_details;

    DELETE FROM Fact.CreditiOpenAI
    WHERE IsDeleted = CAST(1 AS BIT);

    UPDATE audit.tables
    SET lastupdated_local = lastupdated_staging
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    COMMIT TRANSACTION;

END;
GO

EXEC Fact.usp_Merge_CreditiOpenAI;
GO
