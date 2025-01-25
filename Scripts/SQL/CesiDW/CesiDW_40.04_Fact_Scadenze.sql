USE CesiDW;
GO

/*
SET NOEXEC OFF;
--*/ SET NOEXEC ON;
GO

/*
    SCHEMA_NAME > COMETA
    TABLE_NAME > Scadenza
    STAGING_TABLE_NAME > Scadenze
*/

/**
 * @table Staging.Scadenze
 * @description

 * @depends Landing.COMETA_Scadenza

SELECT TOP 1 * FROM Landing.COMETA_Scadenza;
*/

--DROP TABLE IF EXISTS Staging.Scadenze; DELETE FROM audit.tables WHERE provider_name = N'MyDatamartReporting' AND full_table_name = N'Landing.COMETA_Scadenza';
GO

IF NOT EXISTS (SELECT 1 FROM audit.tables WHERE provider_name = N'MyDatamartReporting' AND full_table_name = N'Landing.COMETA_Scadenza')
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
        N'Landing.COMETA_Scadenza',      -- full_table_name - sysname
        N'Staging.Scadenze',      -- staging_table_name - sysname
        N'Fact.Scadenze',      -- datawarehouse_table_name - sysname
        NULL, -- lastupdated_staging - datetime
        NULL  -- lastupdated_local - datetime
    );

END;
GO

IF OBJECT_ID(N'Staging.ScadenzeView', N'V') IS NULL EXEC('CREATE VIEW Staging.ScadenzeView AS SELECT 1 AS fld;');
GO

ALTER VIEW Staging.ScadenzeView
AS
WITH Documenti
AS (
    SELECT
        D.IDDocumento,
        MIN(D.PKDocumenti) AS PKDocumenti

    FROM Fact.Documenti D
    WHERE D.IsDeleted = CAST(0 AS BIT)
    GROUP BY D.IDDocumento
),
ScadenzeSaldi
AS (
    SELECT
        S.id_scadenza AS IDScadenza,
        COALESCE(S.tipo_scadenza, N'') AS TipoScadenza,
        --S.id_sog_commerciale,
        SC.IDSoggettoCommerciale,
        COALESCE(C.PKCliente, -101) AS PKCliente,
        --S.data_scadenza,
        DS.PKData AS PKDataScadenza,
        COALESCE(S.importo, 0.0) AS ImportoScadenza,
        COALESCE(S.stato_scadenza, N'') AS StatoScadenza,
        COALESCE(S.esito_pagamento, N'') AS EsitoPagamento,
        --S.id_documento,
        DD.IDDocumento,
        DD.PKDocumenti,
        SUM(COALESCE(MS.importo, 0.0)) AS ImportoSaldato,
        COALESCE(S.importo, 0.0) - SUM(COALESCE(MS.importo, 0.0)) AS ImportoResiduo

    FROM Landing.COMETA_Scadenza S
    INNER JOIN Staging.SoggettoCommerciale SC ON SC.IDSoggettoCommerciale = S.id_sog_commerciale
    LEFT JOIN Dim.Cliente C ON C.IDSoggettoCommerciale = SC.IDSoggettoCommerciale
        AND C.IsDeleted = CAST(0 AS BIT)
    INNER JOIN Dim.Data DS ON DS.PKData = S.data_scadenza
    INNER JOIN Documenti DD ON DD.IDDocumento = S.id_documento
    LEFT JOIN Landing.COMETA_MovimentiScadenza MS ON MS.id_scadenza = S.id_scadenza
        AND MS.IsDeleted = CAST(0 AS BIT)
    WHERE S.IsDeleted = CAST(0 AS BIT)
        AND S.esito_pagamento IN (N'E', N'I')
        AND S.data_scadenza <= CAST(CURRENT_TIMESTAMP AS DATETIME2)
        AND S.stato_scadenza = N'D'
    GROUP BY COALESCE (S.tipo_scadenza, N''),
        COALESCE (C.PKCliente, -101),
        COALESCE (S.importo, 0.0),
        COALESCE (S.stato_scadenza, N''),
        COALESCE (S.esito_pagamento, N''),
        S.id_scadenza,
        SC.IDSoggettoCommerciale,
        DS.PKData,
        DD.IDDocumento,
        DD.PKDocumenti,
        S.importo
),
TableData
AS (
    SELECT
        SS.IDScadenza,

        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            SS.IDScadenza,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            SS.TipoScadenza,
            SS.IDSoggettoCommerciale,
            SS.PKCliente,
            SS.PKDataScadenza,
            SS.ImportoScadenza,
            SS.StatoScadenza,
            SS.EsitoPagamento,
            SS.IDDocumento,
            SS.PKDocumenti,
            SS.ImportoSaldato,
            SS.ImportoResiduo,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,

        SS.TipoScadenza,
        SS.IDSoggettoCommerciale,
        SS.PKCliente,
        SS.PKDataScadenza,
        SS.ImportoScadenza,
        SS.StatoScadenza,
        SS.EsitoPagamento,
        SS.IDDocumento,
        SS.PKDocumenti,
        SS.ImportoSaldato,
        SS.ImportoResiduo

    FROM ScadenzeSaldi SS
)
SELECT
    -- Chiavi
    TD.IDScadenza,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Dimensioni
    TD.TipoScadenza,
    TD.IDSoggettoCommerciale,
    TD.PKCliente,
    TD.PKDataScadenza,
    TD.StatoScadenza,
    TD.EsitoPagamento,
    TD.IDDocumento,
    TD.PKDocumenti,

    -- Misure
    TD.ImportoScadenza,
    TD.ImportoSaldato,
    TD.ImportoResiduo

FROM TableData TD;
GO

--IF OBJECT_ID(N'Staging.Scadenze', N'U') IS NOT NULL DROP TABLE Staging.Scadenze;
GO

IF OBJECT_ID(N'Staging.Scadenze', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Staging.Scadenze FROM Staging.ScadenzeView;

    ALTER TABLE Staging.Scadenze ADD CONSTRAINT PK_Landing_COMETA_Scadenza PRIMARY KEY CLUSTERED (UpdateDatetime, IDScadenza);

    ALTER TABLE Staging.Scadenze ALTER COLUMN TipoScadenza CHAR(1) NOT NULL;
    ALTER TABLE Staging.Scadenze ALTER COLUMN PKCliente INT NOT NULL;
    ALTER TABLE Staging.Scadenze ALTER COLUMN StatoScadenza CHAR(1) NOT NULL;
    ALTER TABLE Staging.Scadenze ALTER COLUMN EsitoPagamento CHAR(1) NOT NULL;
    ALTER TABLE Staging.Scadenze ALTER COLUMN IDDocumento INT NOT NULL;
    ALTER TABLE Staging.Scadenze ALTER COLUMN PKDocumenti INT NOT NULL;
    ALTER TABLE Staging.Scadenze ALTER COLUMN ImportoScadenza DECIMAL(10, 2) NOT NULL;
    ALTER TABLE Staging.Scadenze ALTER COLUMN ImportoSaldato DECIMAL(10, 2) NOT NULL;
    ALTER TABLE Staging.Scadenze ALTER COLUMN ImportoResiduo DECIMAL(10, 2) NOT NULL;

    CREATE UNIQUE NONCLUSTERED INDEX IX_COMETA_Scadenza_BusinessKey ON Staging.Scadenze (IDScadenza);
END;
GO

IF OBJECT_ID(N'Staging.usp_Reload_Scadenze', N'P') IS NULL EXEC('CREATE PROCEDURE Staging.usp_Reload_Scadenze AS RETURN 0;');
GO

ALTER PROCEDURE Staging.usp_Reload_Scadenze
AS
BEGIN

    SET NOCOUNT ON;

    DECLARE @lastupdated_staging DATETIME;
    DECLARE @provider_name NVARCHAR(60) = N'MyDatamartReporting';
    DECLARE @full_table_name sysname = N'Landing.COMETA_Scadenza';

    SELECT TOP 1 @lastupdated_staging = lastupdated_staging
    FROM audit.tables
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    IF (@lastupdated_staging IS NULL) SET @lastupdated_staging = CAST('19000101' AS DATETIME);

    BEGIN TRANSACTION

    TRUNCATE TABLE Staging.Scadenze;

    INSERT INTO Staging.Scadenze
    SELECT * FROM Staging.ScadenzeView;
    --WHERE UpdateDatetime > @lastupdated_staging;

    SELECT @lastupdated_staging = MAX(UpdateDatetime) FROM Staging.Scadenze;

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

EXEC Staging.usp_Reload_Scadenze;
GO

--DROP TABLE IF EXISTS Fact.Scadenze; DROP SEQUENCE IF EXISTS dbo.seq_Fact_Scadenze;
GO

IF OBJECT_ID('dbo.seq_Fact_Scadenze', 'SO') IS NULL
BEGIN

    CREATE SEQUENCE dbo.seq_Fact_Scadenze START WITH 1;

END;
GO

IF OBJECT_ID('Fact.Scadenze', 'U') IS NULL
BEGIN

    CREATE TABLE Fact.Scadenze (
        PKScadenze INT NOT NULL CONSTRAINT PK_Fact_Scadenze PRIMARY KEY CLUSTERED CONSTRAINT DFT_Fact_Scadenze_PKScadenze DEFAULT (NEXT VALUE FOR dbo.seq_Fact_Scadenze),

	    PKCliente INT NOT NULL CONSTRAINT FK_Fact_Scadenze_PKCliente FOREIGN KEY REFERENCES Dim.Cliente (PKCliente),
        PKDataScadenza DATE NOT NULL CONSTRAINT FK_Fact_Scadenze_PKDataScadenza FOREIGN KEY REFERENCES Dim.Data (PKData),
        PKDocumenti INT NOT NULL CONSTRAINT FK_Fact_Scadenze_PKDocumenti FOREIGN KEY REFERENCES Fact.Documenti (PKDocumenti),

	    HistoricalHashKey VARBINARY(20) NULL,
	    ChangeHashKey VARBINARY(20) NULL,
	    HistoricalHashKeyASCII VARCHAR(34) NULL,
	    ChangeHashKeyASCII VARCHAR(34) NULL,
	    InsertDatetime DATETIME NOT NULL,
	    UpdateDatetime DATETIME NOT NULL,
	    IsDeleted BIT NOT NULL,

	    IDScadenza INT NOT NULL,
	    TipoScadenza CHAR(1) NOT NULL,
	    IDSoggettoCommerciale INT NOT NULL,
	    StatoScadenza CHAR(1) NOT NULL,
	    EsitoPagamento CHAR(1) NOT NULL,
	    IDDocumento INT NOT NULL,

	    ImportoScadenza DECIMAL(10, 2) NOT NULL,
	    ImportoSaldato DECIMAL(10, 2) NOT NULL,
	    ImportoResiduo DECIMAL(10, 2) NOT NULL
    );

    CREATE UNIQUE NONCLUSTERED INDEX IX_Fact_Scadenze_IDScadenza ON Fact.Scadenze (IDScadenza);

    ALTER SEQUENCE dbo.seq_Fact_Scadenze RESTART WITH 1;

END;
GO

IF OBJECT_ID(N'Fact.usp_Merge_Scadenze', N'P') IS NULL EXEC('CREATE PROCEDURE Fact.usp_Merge_Scadenze AS RETURN 0;');
GO

ALTER PROCEDURE Fact.usp_Merge_Scadenze
AS
BEGIN

    SET NOCOUNT ON;

    BEGIN TRANSACTION 

    DECLARE @provider_name NVARCHAR(60) = N'MyDatamartReporting';
    DECLARE @full_table_name sysname = N'Landing.COMETA_Documento_Riga';

    MERGE INTO Fact.Scadenze AS TGT
    USING Staging.Scadenze (nolock) AS SRC
    ON SRC.IDScadenza = TGT.IDScadenza

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.IsDeleted = SRC.IsDeleted,

        TGT.PKCliente = SRC.PKCliente,
        TGT.PKDataScadenza = SRC.PKDataScadenza,
        TGT.PKDocumenti = SRC.PKDocumenti,

        TGT.IDScadenza = SRC.IDScadenza,
        TGT.TipoScadenza = SRC.TipoScadenza,
        TGT.IDSoggettoCommerciale = SRC.IDSoggettoCommerciale,
        TGT.StatoScadenza = SRC.StatoScadenza,
        TGT.EsitoPagamento = SRC.EsitoPagamento,
        TGT.IDDocumento = SRC.IDDocumento,
        TGT.ImportoScadenza = SRC.ImportoScadenza,
        TGT.ImportoSaldato = SRC.ImportoSaldato,
        TGT.ImportoResiduo = SRC.ImportoResiduo

    WHEN NOT MATCHED
      THEN INSERT (
        PKCliente,
        PKDataScadenza,
        PKDocumenti,
        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
        IDScadenza,
        TipoScadenza,
        IDSoggettoCommerciale,
        StatoScadenza,
        EsitoPagamento,
        IDDocumento,
        ImportoScadenza,
        ImportoSaldato,
        ImportoResiduo
      )
      VALUES (
        SRC.PKCliente,
        SRC.PKDataScadenza,
        SRC.PKDocumenti,
        SRC.HistoricalHashKey,
        SRC.ChangeHashKey,
        SRC.HistoricalHashKeyASCII,
        SRC.ChangeHashKeyASCII,
        SRC.InsertDatetime,
        SRC.UpdateDatetime,
        SRC.IsDeleted,
        SRC.IDScadenza,
        SRC.TipoScadenza,
        SRC.IDSoggettoCommerciale,
        SRC.StatoScadenza,
        SRC.EsitoPagamento,
        SRC.IDDocumento,
        SRC.ImportoScadenza,
        SRC.ImportoSaldato,
        SRC.ImportoResiduo
      )

    WHEN NOT MATCHED BY SOURCE
      THEN UPDATE
        SET TGT.ChangeHashKey = CONVERT(VARBINARY(20), ''),
            TGT.ChangeHashKeyASCII = '',
            TGT.UpdateDatetime = CURRENT_TIMESTAMP,
            TGT.IsDeleted = CAST(1 AS BIT)

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        $action AS merge_action,
        'Staging.Scadenze' AS full_olap_table_name,
        'IDScadenza = ' + CAST(COALESCE(inserted.IDScadenza, deleted.IDScadenza) AS NVARCHAR(1000)) AS primary_key_description
    INTO audit.merge_log_details;

    --DELETE FROM Fact.Scadenze
    --WHERE IsDeleted = CAST(1 AS BIT);

    UPDATE audit.tables
    SET lastupdated_local = lastupdated_staging
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    COMMIT TRANSACTION;

END;
GO

EXEC Fact.usp_Merge_Scadenze;
GO
