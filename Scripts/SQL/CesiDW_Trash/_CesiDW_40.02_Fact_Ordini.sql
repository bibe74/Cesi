USE CesiDW;
GO

/*
SET NOEXEC OFF;
--*/ SET NOEXEC ON;
GO

/*
    SCHEMA_NAME > COMETA
    TABLE_NAME > Documento_Riga
    STAGING_TABLE_NAME > Ordini
*/

/**
 * @table Staging.Ordini
 * @description

 * @depends Landing.COMETA_Documento_Riga

SELECT TOP 1 * FROM Landing.COMETA_Documento_Riga;
*/

--DROP TABLE IF EXISTS Staging.Ordini; DELETE FROM audit.tables WHERE provider_name = N'MyDatamartReporting' AND full_table_name = N'Landing.COMETA_Documento_Riga_OLD';
GO

IF NOT EXISTS (SELECT 1 FROM audit.tables WHERE provider_name = N'MyDatamartReporting' AND full_table_name = N'Landing.COMETA_Documento_Riga_OLD')
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
        N'Landing.COMETA_Documento_Riga_OLD',      -- full_table_name - sysname
        N'Staging.Ordini',      -- staging_table_name - sysname
        N'Fact.Ordini',      -- datawarehouse_table_name - sysname
        NULL, -- lastupdated_staging - datetime
        NULL  -- lastupdated_local - datetime
    );

END;
GO

IF OBJECT_ID(N'Staging.OrdiniView', N'V') IS NULL EXEC('CREATE VIEW Staging.OrdiniView AS SELECT 1 AS fld;');
GO

ALTER VIEW Staging.OrdiniView
AS
WITH TableData
AS (
    SELECT
        DR.id_riga_documento AS IDDocumento_Riga,

        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            DR.id_riga_documento,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            D.id_documento,
            D.data_documento,
            D.num_documento,
            DR.num_riga,
            C.PKCliente,
            D.data_inizio_contratto,
            D.data_fine_contratto,
            DR.totale_riga,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,

        D.id_documento AS IDDocumento,
        --D.data_documento,
        COALESCE(DD.PKData, DIC.PKData, CAST('19000101' AS DATE)) AS PKDataDocumento,
        COALESCE(D.num_documento, N'') AS NumeroDocumento,
        DR.num_riga AS NumeroRiga,
        --D.id_prof_documento,
        PD.codice AS IDProfilo,
        PD.descrizione AS Profilo,
        --D.id_sog_commerciale,
        DSC.tipo AS IDTipoSoggettoCommerciale,
        DSC.descr_sog_com AS TipoSoggettoCommerciale,
        --A.id_anagrafica,
        C.PKCliente,
        --D.id_sog_commerciale_fattura,
        --D.data_inizio_contratto,
        COALESCE(DIC.PKData, CAST('19000101' AS DATE)) AS PKDataInizioContratto,
        --D.data_fine_contratto,
        COALESCE(DFC.PKData, CAST('19000101' AS DATE)) AS PKDataFineContratto,

        --DR.id_articolo,
        --DR.descrizione,
        COALESCE(DR.totale_riga, 0.0) AS ImportoTotale,

        ROW_NUMBER() OVER (PARTITION BY DR.id_riga_documento ORDER BY D.id_documento) AS rn

    FROM Landing.COMETA_Documento_Riga DR
    INNER JOIN Landing.COMETA_Documento D ON D.id_documento = DR.id_documento
        ----AND D.id_prof_documento = 1 -- 1: Ordine cliente
    INNER JOIN Landing.COMETA_Profilo_Documento PD ON PD.id_prof_documento = D.id_prof_documento
    INNER JOIN Landing.COMETA_SoggettoCommerciale SC ON SC.id_sog_commerciale = D.id_sog_commerciale
        ----AND SC.tipo = N'C' -- C: Cliente
    INNER JOIN Import.Decod_Sog_Comm DSC ON DSC.tipo = SC.tipo
    INNER JOIN Landing.COMETA_Anagrafica A ON A.id_anagrafica = SC.id_anagrafica
    INNER JOIN Dim.Cliente C ON C.IDSoggettoCommerciale = D.id_sog_commerciale
    LEFT JOIN Dim.Data DD ON DD.PKData = D.data_documento
    LEFT JOIN Dim.Data DIC ON DIC.PKData = D.data_inizio_contratto
    LEFT JOIN Dim.Data DFC ON DFC.PKData = D.data_fine_contratto
  ----  WHERE DR.descrizione LIKE '%MySolution%'
		----AND DR.descrizione NOT LIKE '%master%'
		----AND DR.descrizione NOT LIKE '%senza pensieri%'
		----AND DR.descrizione NOT LIKE '%quesiti%'
		----AND DR.descrizione NOT LIKE '%quesito%'
		----AND DR.descrizione NOT LIKE '%light%'
		----AND DR.descrizione NOT LIKE '%storno%'
		----AND DR.descrizione NOT LIKE '%dichiarazioni%'
		----AND DR.descrizione NOT LIKE '%bilancio%'
		----AND DR.descrizione NOT LIKE '%percorso%'
)
SELECT
    -- Chiavi
    TD.IDDocumento_Riga,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Altri campi
    TD.IDDocumento,
    TD.PKDataDocumento,
    TD.NumeroDocumento,
    TD.NumeroRiga,
    TD.IDProfilo,
    TD.Profilo,
    TD.IDTipoSoggettoCommerciale,
    TD.TipoSoggettoCommerciale,
    TD.PKCliente,
    TD.PKDataInizioContratto,
    TD.PKDataFineContratto,
    TD.ImportoTotale

FROM TableData TD
WHERE TD.rn = 1;
GO

--IF OBJECT_ID(N'Staging.Ordini', N'U') IS NOT NULL DROP TABLE Staging.Ordini;
GO

IF OBJECT_ID(N'Staging.Ordini', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Staging.Ordini FROM Staging.OrdiniView;

    ALTER TABLE Staging.Ordini ADD CONSTRAINT PK_Landing_COMETA_Documento_Riga PRIMARY KEY CLUSTERED (UpdateDatetime, IDDocumento_Riga);

    ALTER TABLE Staging.Ordini ALTER COLUMN PKDataDocumento DATE NOT NULL;
    ALTER TABLE Staging.Ordini ALTER COLUMN NumeroDocumento NVARCHAR(20) NOT NULL;
    ALTER TABLE Staging.Ordini ALTER COLUMN NumeroRiga INT NOT NULL;
    ALTER TABLE Staging.Ordini ALTER COLUMN PKCliente INT NOT NULL;
    ALTER TABLE Staging.Ordini ALTER COLUMN PKDataInizioContratto DATE NOT NULL;
    ALTER TABLE Staging.Ordini ALTER COLUMN PKDataFineContratto DATE NOT NULL;
    ALTER TABLE Staging.Ordini ALTER COLUMN ImportoTotale DECIMAL(10, 2) NOT NULL;

    CREATE UNIQUE NONCLUSTERED INDEX IX_COMETA_Documento_Riga_BusinessKey ON Staging.Ordini (IDDocumento_Riga);
END;
GO

IF OBJECT_ID(N'Staging.usp_Reload_Ordini', N'P') IS NULL EXEC('CREATE PROCEDURE Staging.usp_Reload_Ordini AS RETURN 0;');
GO

ALTER PROCEDURE Staging.usp_Reload_Ordini
AS
BEGIN

    SET NOCOUNT ON;

    DECLARE @lastupdated_staging DATETIME;
    DECLARE @provider_name NVARCHAR(60) = N'MyDatamartReporting';
    DECLARE @full_table_name sysname = N'Landing.COMETA_Documento_Riga_OLD';

    SELECT TOP 1 @lastupdated_staging = lastupdated_staging
    FROM audit.tables
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    IF (@lastupdated_staging IS NULL) SET @lastupdated_staging = CAST('19000101' AS DATETIME);

    BEGIN TRANSACTION

    TRUNCATE TABLE Staging.Ordini;

    INSERT INTO Staging.Ordini
    SELECT * FROM Staging.OrdiniView
    --WHERE UpdateDatetime > @lastupdated_staging;

    SELECT @lastupdated_staging = MAX(UpdateDatetime) FROM Staging.Ordini;

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

EXEC Staging.usp_Reload_Ordini;
GO

--DROP TABLE IF EXISTS Fact.Ordini; DROP SEQUENCE IF EXISTS dbo.seq_Fact_Ordini;
GO

IF OBJECT_ID('dbo.seq_Fact_Ordini', 'SO') IS NULL
BEGIN

    CREATE SEQUENCE dbo.seq_Fact_Ordini START WITH 1;

END;
GO

IF OBJECT_ID('Fact.Ordini', 'U') IS NULL
BEGIN

    CREATE TABLE Fact.Ordini (
        PKOrdini INT NOT NULL CONSTRAINT PK_Fact_Ordini PRIMARY KEY CLUSTERED DEFAULT (NEXT VALUE FOR dbo.seq_Fact_Ordini),
        PKDataDocumento DATE NOT NULL CONSTRAINT FK_Fact_Ordini_PKDataDocumento FOREIGN KEY REFERENCES Dim.Data (PKData),
        PKCliente INT NOT NULL CONSTRAINT FK_Fact_Ordini_PKCliente FOREIGN KEY REFERENCES Dim.Cliente (PKCliente),
        PKDataInizioContratto DATE NOT NULL CONSTRAINT FK_Fact_Ordini_PKDataInizioContratto FOREIGN KEY REFERENCES Dim.Data (PKData),
        PKDataFineContratto DATE NOT NULL CONSTRAINT FK_Fact_Ordini_PKDataFineContratto FOREIGN KEY REFERENCES Dim.Data (PKData),

	    HistoricalHashKey VARBINARY(20) NULL,
	    ChangeHashKey VARBINARY(20) NULL,
	    HistoricalHashKeyASCII VARCHAR(34) NULL,
	    ChangeHashKeyASCII VARCHAR(34) NULL,
	    InsertDatetime DATETIME NOT NULL,
	    UpdateDatetime DATETIME NOT NULL,
	    IsDeleted BIT NOT NULL,

    	IDDocumento_Riga INT NOT NULL,
	    IDDocumento INT NOT NULL,
	    NumeroDocumento NVARCHAR(20) NOT NULL,
	    NumeroRiga INT NOT NULL,

	    ImportoTotale DECIMAL(10, 2) NOT NULL
    );

    CREATE UNIQUE NONCLUSTERED INDEX IX_Fact_Ordini_PKData_PKCliente ON Fact.Ordini (IDDocumento_Riga);

    ALTER SEQUENCE dbo.seq_Fact_Ordini RESTART WITH 1;

END;
GO

IF OBJECT_ID(N'Fact.usp_Merge_Ordini', N'P') IS NULL EXEC('CREATE PROCEDURE Fact.usp_Merge_Ordini AS RETURN 0;');
GO

ALTER PROCEDURE Fact.usp_Merge_Ordini
AS
BEGIN

    SET NOCOUNT ON;

    BEGIN TRANSACTION 

    DECLARE @provider_name NVARCHAR(60) = N'MyDatamartReporting';
    DECLARE @full_table_name sysname = N'Landing.COMETA_Documento_Riga_OLD';

    MERGE INTO Fact.Ordini AS TGT
    USING Staging.Ordini (nolock) AS SRC
    ON SRC.IDDocumento_Riga = TGT.IDDocumento_Riga

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.IsDeleted = SRC.IsDeleted,
        
        TGT.IDDocumento = SRC.IDDocumento,
        TGT.PKDataDocumento = SRC.PKDataDocumento,
        TGT.NumeroDocumento = SRC.NumeroDocumento,
        TGT.NumeroRiga = SRC.NumeroRiga,
        TGT.PKCliente = SRC.PKCliente,
        TGT.PKDataInizioContratto = SRC.PKDataInizioContratto,
        TGT.PKDataFineContratto = SRC.PKDataFineContratto,
        TGT.ImportoTotale = SRC.ImportoTotale

    WHEN NOT MATCHED
      THEN INSERT (
        IDDocumento_Riga,
        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
        IDDocumento,
        PKDataDocumento,
        NumeroDocumento,
        NumeroRiga,
        PKCliente,
        PKDataInizioContratto,
        PKDataFineContratto,
        ImportoTotale
      )
      VALUES (
        SRC.IDDocumento_Riga,
        SRC.HistoricalHashKey,
        SRC.ChangeHashKey,
        SRC.HistoricalHashKeyASCII,
        SRC.ChangeHashKeyASCII,
        SRC.InsertDatetime,
        SRC.UpdateDatetime,
        SRC.IsDeleted,
        SRC.IDDocumento,
        SRC.PKDataDocumento,
        SRC.NumeroDocumento,
        SRC.NumeroRiga,
        SRC.PKCliente,
        SRC.PKDataInizioContratto,
        SRC.PKDataFineContratto,
        SRC.ImportoTotale
      )

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        $action AS merge_action,
        'Staging.Ordini' AS full_olap_table_name,
        'PKData = ' + CAST(COALESCE(inserted.IDDocumento_Riga, deleted.IDDocumento_Riga) AS NVARCHAR(1000)) AS primary_key_description
    INTO audit.merge_log_details;

    --DELETE FROM Fact.Ordini
    --WHERE IsDeleted = CAST(1 AS BIT);

    UPDATE audit.tables
    SET lastupdated_local = lastupdated_staging
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    COMMIT TRANSACTION;

END;
GO

EXEC Fact.usp_Merge_Ordini;
GO
