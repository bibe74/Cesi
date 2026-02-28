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

CREATE OR ALTER VIEW Staging.CreditiOpenAIDettaglioView
AS
WITH EmailClienteDettaglio
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
        ECD.PKCliente,
        COALESCE(PA.Codice, '') AS CodiceOrdine,
        COALESCE(DCP.PKData, CAST('19000101' AS DATE)) AS PKDataCreazionePartita,
        COALESCE(DSP.PKData, CAST('19000101' AS DATE)) AS PKDataScadenzaPartita,
        COALESCE(DID.PKData, CAST('19000101' AS DATE)) AS PKDataInizioDemo,
        MAX(COALESCE(DM.PKData, CAST('19000101' AS DATE))) AS PKDataUltimoUtilizzo,
        COALESCE(PA.Quantita, 0) AS QtaCreditiCaricatiInPartita,
        SUM(CR.Quantita) AS QtaCreditiResidui,
        SUM(CASE WHEN CR.Quantita < 0 THEN CR.Quantita * -1 ELSE 0 END) AS QtaCreditiUtilizzati,
        COUNT(1) AS ConteggioDomandeERisposte

    FROM Landing.GPT_OpenAICredito CR
    INNER JOIN Landing.GPT_OpenAICliente CL ON CL.Id = CR.ClienteId
        AND CL.IsDeleted = CAST(0 AS BIT)
    INNER JOIN EmailClienteDettaglio ECD ON ECD.Email = CL.Email
        AND ECD.rn = 1
    INNER JOIN Landing.GPT_OpenAICausale CA ON CA.Id = CR.CausaleId
        AND CA.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Landing.GPT_OpenAIPartita PA ON PA.Id = CR.PartitaId
        AND PA.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Dim.Data DCP ON DCP.PKData = PA.DataCreazione
    LEFT JOIN Dim.Data DSP ON DSP.PKData = PA.DataScadenza
    LEFT JOIN Landing.MYSOLUTION_Demo D ON CL.Email = D.Email
        AND D.IsDeleted = CAST (0 AS BIT)
    LEFT JOIN Dim.Data DID ON DID.PKData = D.DataInizioDemo
    LEFT JOIN Dim.Data DM ON DM.PKData = CR.DataMovimento
    WHERE CR.IsDeleted = CAST(0 AS BIT)
    GROUP BY ECD.PKCliente,
        COALESCE (PA.Codice, ''),
        COALESCE(DCP.PKData, CAST('19000101' AS DATE)),
        COALESCE(DSP.PKData, CAST('19000101' AS DATE)),
        COALESCE(DID.PKData, CAST('19000101' AS DATE)),
        COALESCE(PA.Quantita, 0)
)
SELECT
    -- Chiavi
    TD.PKCliente,
    TD.CodiceOrdine,
    TD.PKDataUltimoUtilizzo,
    TD.PKDataCreazionePartita,
    TD.PKDataScadenzaPartita,
    TD.QtaCreditiCaricatiInPartita,

    -- Campi per data warehouse
    CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
        TD.PKCliente,
        TD.CodiceOrdine,
        TD.PKDataUltimoUtilizzo,
        TD.PKDataCreazionePartita,
        TD.PKDataScadenzaPartita,
        TD.QtaCreditiCaricatiInPartita,
        ' '
    ))) AS HistoricalHashKey,
    CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
        TD.PKDataInizioDemo,
        TD.QtaCreditiResidui,
        TD.QtaCreditiUtilizzati,
        TD.ConteggioDomandeERisposte,
        ' '
    ))) AS ChangeHashKey,
    CURRENT_TIMESTAMP AS InsertDatetime,
    CURRENT_TIMESTAMP AS UpdateDatetime,

    -- Attributi
    TD.PKDataInizioDemo,

    -- Misure
    TD.QtaCreditiResidui,
    TD.QtaCreditiUtilizzati,
    TD.ConteggioDomandeERisposte

FROM TableData TD;
GO

--IF OBJECT_ID(N'Staging.CreditiOpenAIDettaglio', N'U') IS NOT NULL DROP TABLE Staging.CreditiOpenAIDettaglio;
GO

IF OBJECT_ID(N'Staging.CreditiOpenAIDettaglio', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Staging.CreditiOpenAIDettaglio FROM Staging.CreditiOpenAIDettaglioView;

    ALTER TABLE Staging.CreditiOpenAIDettaglio ALTER COLUMN CodiceOrdine NVARCHAR(20) NOT NULL;
    ALTER TABLE Staging.CreditiOpenAIDettaglio ALTER COLUMN PKDataUltimoUtilizzo DATE NOT NULL;
    ALTER TABLE Staging.CreditiOpenAIDettaglio ALTER COLUMN PKDataCreazionePartita DATE NOT NULL;
    ALTER TABLE Staging.CreditiOpenAIDettaglio ALTER COLUMN PKDataScadenzaPartita DATE NOT NULL;
    ALTER TABLE Staging.CreditiOpenAIDettaglio ALTER COLUMN QtaCreditiCaricatiInPartita INT NOT NULL;

    ALTER TABLE Staging.CreditiOpenAIDettaglio ADD CONSTRAINT PK_Staging_CreditiOpenAIDettaglio PRIMARY KEY CLUSTERED (UpdateDatetime, PKCliente, CodiceOrdine, PKDataUltimoUtilizzo, PKDataCreazionePartita, PKDataScadenzaPartita, QtaCreditiCaricatiInPartita);

    ALTER TABLE Staging.CreditiOpenAIDettaglio ALTER COLUMN PKDataInizioDemo DATE NOT NULL;

    CREATE UNIQUE NONCLUSTERED INDEX IX_Staging_CreditiOpenAI_BusinessKey ON Staging.CreditiOpenAIDettaglio (PKCliente, CodiceOrdine, PKDataUltimoUtilizzo, PKDataCreazionePartita, PKDataScadenzaPartita, QtaCreditiCaricatiInPartita);
END;
GO

IF OBJECT_ID(N'Staging.usp_Reload_CreditiOpenAIDettaglio', N'P') IS NULL EXEC('CREATE PROCEDURE Staging.usp_Reload_CreditiOpenAIDettaglio AS RETURN 0;');
GO

ALTER PROCEDURE Staging.usp_Reload_CreditiOpenAIDettaglio
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

    TRUNCATE TABLE Staging.CreditiOpenAIDettaglio;

    INSERT INTO Staging.CreditiOpenAIDettaglio
    SELECT * FROM Staging.CreditiOpenAIDettaglioView
    WHERE UpdateDatetime > @lastupdated_staging;

    SELECT @lastupdated_staging = MAX(UpdateDatetime) FROM Staging.CreditiOpenAIDettaglio;

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

EXEC Staging.usp_Reload_CreditiOpenAIDettaglio;
GO

CREATE OR ALTER VIEW Staging.CreditiOpenAIView
AS
WITH OpenAICrediti
AS (
    SELECT
        C.Email,
        SUM(CASE WHEN LEFT(CA.Codice, 1) = N'C' THEN CR.Quantita ELSE 0 END) AS CreditiAcquistati,
        SUM(CASE WHEN LEFT(CA.Codice, 1) = N'S' AND CR.PartitaId IS NOT NULL THEN CR.Quantita ELSE 0 END) AS CreditiConsumati,
        SUM(CASE WHEN LEFT(CA.Codice, 1) = N'S' AND CR.PartitaId IS NULL THEN CR.Quantita ELSE 0 END) AS CreditiFuoriOrdine,
        SUM(CR.Quantita) AS CreditiResidui,
        MIN(P.datascadenza) AS DataScadenzaOrdine

    FROM Landing.GPT_OpenAICredito CR
    INNER JOIN Landing.GPT_OpenAICliente C ON C.Id = CR.ClienteId
        AND C.IsDeleted = CAST(0 AS BIT)
    INNER JOIN Landing.GPT_OpenAICausale CA ON CA.Id = CR.CausaleId
        AND CA.IsDeleted = CAST(0 AS BIT)
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

--DROP TABLE IF EXISTS Fact.CreditiOpenAIDettaglio; DROP SEQUENCE IF EXISTS dbo.seq_Fact_CreditiOpenAIDettaglio;
GO

IF OBJECT_ID('dbo.seq_Fact_CreditiOpenAIDettaglio', 'SO') IS NULL
BEGIN

    CREATE SEQUENCE dbo.seq_Fact_CreditiOpenAIDettaglio START WITH 1;

END;
GO

IF OBJECT_ID('Fact.CreditiOpenAIDettaglio', 'U') IS NULL
BEGIN

    CREATE TABLE Fact.CreditiOpenAIDettaglio (
        PKCreditiOpenAIDettaglio INT NOT NULL CONSTRAINT PK_Fact_CreditiOpenAIDettaglio PRIMARY KEY CLUSTERED CONSTRAINT DFT_Fact_CreditiOpenAIDettaglio_PKCreditiOpenAIDettaglio DEFAULT (NEXT VALUE FOR dbo.seq_Fact_CreditiOpenAIDettaglio),

	    PKCliente INT NOT NULL CONSTRAINT FK_Fact_CreditiOpenAIDettaglio_PKCliente FOREIGN KEY REFERENCES Dim.Cliente (PKCliente),
	    CodiceOrdine NVARCHAR(20) NOT NULL,
	    PKDataUltimoUtilizzo DATE NOT NULL CONSTRAINT FK_Fact_CreditiOpenAIDettaglio_PKDataUltimoUtilizzo FOREIGN KEY REFERENCES Dim.Data (PKData),
	    PKDataCreazionePartita DATE NOT NULL CONSTRAINT FK_Fact_CreditiOpenAIDettaglio_PKDataCreazionePartita FOREIGN KEY REFERENCES Dim.Data (PKData),
	    PKDataScadenzaPartita DATE NOT NULL CONSTRAINT FK_Fact_CreditiOpenAIDettaglio_PKDataScadenzaPartita FOREIGN KEY REFERENCES Dim.Data (PKData),
	    QtaCreditiCaricatiInPartita INT NOT NULL,

	    HistoricalHashKey VARBINARY(20) NULL,
	    ChangeHashKey VARBINARY(20) NULL,
	    InsertDatetime DATETIME NOT NULL,
	    UpdateDatetime DATETIME NOT NULL,
	    IsDeleted BIT NOT NULL,

	    PKDataInizioDemo DATE NOT NULL,
	    QtaCreditiResidui INT NULL,
	    QtaCreditiUtilizzati INT NULL,
	    ConteggioDomandeERisposte INT NULL
    );

    CREATE UNIQUE NONCLUSTERED INDEX IX_Fact_CreditiOpenAIDettaglio_BusinessKey ON Fact.CreditiOpenAIDettaglio (PKCliente, CodiceOrdine, PKDataUltimoUtilizzo, PKDataCreazionePartita, PKDataScadenzaPartita, QtaCreditiCaricatiInPartita);

    ALTER SEQUENCE dbo.seq_Fact_CreditiOpenAIDettaglio RESTART WITH 1;

END;
GO

IF OBJECT_ID(N'Fact.usp_Merge_CreditiOpenAIDettaglio', N'P') IS NULL EXEC('CREATE PROCEDURE Fact.usp_Merge_CreditiOpenAIDettaglio AS RETURN 0;');
GO

ALTER PROCEDURE Fact.usp_Merge_CreditiOpenAIDettaglio
AS
BEGIN

    SET NOCOUNT ON;

    BEGIN TRANSACTION 

    DECLARE @provider_name NVARCHAR(60) = N'GPT';
    DECLARE @full_table_name sysname = N'Import.Crediti';

    MERGE INTO Fact.CreditiOpenAIDettaglio AS TGT
    USING Staging.CreditiOpenAIDettaglio (nolock) AS SRC
    ON SRC.PKCliente = TGT.PKCliente
	    AND SRC.CodiceOrdine = TGT.CodiceOrdine
	    AND SRC.PKDataUltimoUtilizzo = TGT.PKDataUltimoUtilizzo
	    AND SRC.PKDataCreazionePartita = TGT.PKDataCreazionePartita
	    AND SRC.PKDataScadenzaPartita = TGT.PKDataScadenzaPartita
        AND SRC.QtaCreditiCaricatiInPartita = TGT.QtaCreditiCaricatiInPartita

    WHEN MATCHED AND (SRC.ChangeHashKey <> TGT.ChangeHashKey)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.IsDeleted = 0,
        TGT.PKDataInizioDemo = SRC.PKDataInizioDemo,
        TGT.QtaCreditiResidui = SRC.QtaCreditiResidui,
        TGT.QtaCreditiUtilizzati = SRC.QtaCreditiUtilizzati,
        TGT.ConteggioDomandeERisposte = SRC.ConteggioDomandeERisposte

    WHEN NOT MATCHED
      THEN INSERT (
        --PKCreditiOpenAIDettaglio,
        PKCliente,
        CodiceOrdine,
        PKDataUltimoUtilizzo,
        PKDataCreazionePartita,
        PKDataScadenzaPartita,
        QtaCreditiCaricatiInPartita,
        HistoricalHashKey,
        ChangeHashKey,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
        PKDataInizioDemo,
        QtaCreditiResidui,
        QtaCreditiUtilizzati,
        ConteggioDomandeERisposte
    ) VALUES (
        PKCliente,
        CodiceOrdine,
        PKDataUltimoUtilizzo,
        PKDataCreazionePartita,
        PKDataScadenzaPartita,
        QtaCreditiCaricatiInPartita,
        HistoricalHashKey,
        ChangeHashKey,
        InsertDatetime,
        UpdateDatetime,
        0,
        PKDataInizioDemo,
        QtaCreditiResidui,
        QtaCreditiUtilizzati,
        ConteggioDomandeERisposte
    )

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        $action AS merge_action,
        'Fact.CreditiOpenAIDettaglio' AS full_olap_table_name,
        'PKCliente = ' + CAST(COALESCE(inserted.PKCliente, deleted.PKCliente) AS NVARCHAR(1000))
            + ', CodiceOrdine = ' + CAST(COALESCE(inserted.CodiceOrdine, deleted.CodiceOrdine) AS NVARCHAR(1000))
	        + ', PKDataUltimoUtilizzo = ' + CAST(COALESCE(inserted.PKDataUltimoUtilizzo, deleted.PKDataUltimoUtilizzo) AS NVARCHAR(1000))
	        + ', PKDataCreazionePartita = ' + CAST(COALESCE(inserted.PKDataCreazionePartita, deleted.PKDataCreazionePartita) AS NVARCHAR(1000))
	        + ', PKDataScadenzaPartita = ' + CAST(COALESCE(inserted.PKDataScadenzaPartita, deleted.PKDataScadenzaPartita) AS NVARCHAR(1000))
	        + ', QtaCreditiCaricatiInPartita = ' + CAST(COALESCE(inserted.QtaCreditiCaricatiInPartita, deleted.QtaCreditiCaricatiInPartita) AS NVARCHAR(1000))
        
        AS primary_key_description
    INTO audit.merge_log_details;

    DELETE FROM Fact.CreditiOpenAIDettaglio
    WHERE IsDeleted = CAST(1 AS BIT);

    UPDATE audit.tables
    SET lastupdated_local = lastupdated_staging
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    COMMIT TRANSACTION;

END;
GO

EXEC Fact.usp_Merge_CreditiOpenAIDettaglio;
GO

--DROP TABLE IF EXISTS Fact.CreditiOpenAI; DROP SEQUENCE IF EXISTS dbo.seq_Fact_CreditiOpenAI;
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
        'Fact.CreditiOpenAI' AS full_olap_table_name,
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
