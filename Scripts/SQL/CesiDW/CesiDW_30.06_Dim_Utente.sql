USE CesiDW;
GO

/*
SET NOEXEC OFF;
--*/ SET NOEXEC ON;
GO

/*
    SCHEMA_NAME > MYSOLUTION
    TABLE_NAME > Users
    STAGING_TABLE_NAME > Utente
*/

/**
 * @table Staging.Utente
 * @description

 * @depends Landing.MYSOLUTION_Users

SELECT TOP 1 * FROM Landing.MYSOLUTION_Users;
*/

--DROP TABLE IF EXISTS Staging.Utente; DELETE FROM audit.tables WHERE provider_name = N'MyDatamartReporting' AND full_table_name = N'Landing.MYSOLUTION_Users';
GO

IF NOT EXISTS (SELECT 1 FROM audit.tables WHERE provider_name = N'MyDatamartReporting' AND full_table_name = N'Landing.MYSOLUTION_Users')
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
        N'Landing.MYSOLUTION_Users',      -- full_table_name - sysname
        N'Staging.Utente',      -- staging_table_name - sysname
        N'Dim.Utente',      -- datawarehouse_table_name - sysname
        NULL, -- lastupdated_staging - datetime
        NULL  -- lastupdated_local - datetime
    );

END;
GO

IF OBJECT_ID(N'Staging.UtenteView', N'V') IS NULL EXEC('CREATE VIEW Staging.UtenteView AS SELECT 1 AS fld;');
GO

ALTER VIEW Staging.UtenteView
AS
WITH EmailCliente
AS (
    SELECT
        C.Email,
        C.HasAnagraficaCometa,
        C.HasAnagraficaNopCommerce,
        C.HasAnagraficaMySolution,
        C.PKCliente,
        ROW_NUMBER() OVER (PARTITION BY C.Email ORDER BY C.PKCliente DESC) AS rn
    FROM Dim.Cliente C
    WHERE C.Email <> N''
        AND C.IsDeleted = CAST(0 AS BIT)
),
TableData
AS (
    SELECT

        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            T.ID,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            T.Email,
            T.RagioneSociale,
            T.Nome,
            T.Cognome,
            T.Citta,
            COALESCE(EC.PKCliente, -1),
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,

        T.ID,
        COALESCE(T.Email, N'') AS Email,
        T.RagioneSociale,
        T.Nome,
        T.Cognome,
        T.Citta,
        COALESCE(EC.PKCliente, -1) AS PKCliente

    FROM Landing.MYSOLUTION_Users T
    LEFT JOIN EmailCliente EC ON EC.Email = T.Email
        AND EC.rn = 1
)
SELECT
    -- Chiavi
    TD.ID AS IDUtente,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Altri campi
    TD.Email,
    TD.RagioneSociale,
    TD.Nome,
    TD.Cognome,
    TD.Citta,
    TD.PKCliente

FROM TableData TD;
GO

--IF OBJECT_ID(N'Staging.Utente', N'U') IS NOT NULL DROP TABLE Staging.Utente;
GO

IF OBJECT_ID(N'Staging.Utente', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Staging.Utente FROM Staging.UtenteView;

    ALTER TABLE Staging.Utente ADD CONSTRAINT PK_Landing_MYSOLUTION_Users PRIMARY KEY CLUSTERED (UpdateDatetime, IDUtente);

    ALTER TABLE Staging.Utente ALTER COLUMN Email NVARCHAR(60) NOT NULL;
    ALTER TABLE Staging.Utente ALTER COLUMN RagioneSociale NVARCHAR(120) NOT NULL;
    ALTER TABLE Staging.Utente ALTER COLUMN Nome NVARCHAR(60) NOT NULL;
    ALTER TABLE Staging.Utente ALTER COLUMN Cognome NVARCHAR(60) NOT NULL;
    ALTER TABLE Staging.Utente ALTER COLUMN Citta NVARCHAR(60) NOT NULL;
    ALTER TABLE Staging.Utente ALTER COLUMN PKCliente INT NULL;

    CREATE UNIQUE NONCLUSTERED INDEX IX_MYSOLUTION_Users_BusinessKey ON Staging.Utente (IDUtente);
END;
GO

IF OBJECT_ID(N'Staging.usp_Reload_Utente', N'P') IS NULL EXEC('CREATE PROCEDURE Staging.usp_Reload_Utente AS RETURN 0;');
GO

ALTER PROCEDURE Staging.usp_Reload_Utente
AS
BEGIN

    SET NOCOUNT ON;

    DECLARE @lastupdated_staging DATETIME;
    DECLARE @provider_name NVARCHAR(60) = N'MyDatamartReporting';
    DECLARE @full_table_name sysname = N'Landing.MYSOLUTION_Users';

    SELECT TOP 1 @lastupdated_staging = lastupdated_staging
    FROM audit.tables
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    IF (@lastupdated_staging IS NULL) SET @lastupdated_staging = CAST('19000101' AS DATETIME);

    BEGIN TRANSACTION

    TRUNCATE TABLE Staging.Utente;

    INSERT INTO Staging.Utente
    SELECT * FROM Staging.UtenteView
    WHERE UpdateDatetime > @lastupdated_staging;

    SELECT @lastupdated_staging = MAX(UpdateDatetime) FROM Staging.Utente;

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

EXEC Staging.usp_Reload_Utente;
GO

--DROP TABLE IF EXISTS Fact.Corsi; DROP TABLE IF EXISTS Dim.Utente; DROP SEQUENCE IF EXISTS dbo.seq_Dim_Utente;
GO

IF OBJECT_ID('dbo.seq_Dim_Utente', 'SO') IS NULL
BEGIN

    CREATE SEQUENCE dbo.seq_Dim_Utente START WITH 1;

END;
GO

IF OBJECT_ID('Dim.Utente', 'U') IS NULL
BEGIN

    CREATE TABLE Dim.Utente (
        PKUtente INT NOT NULL CONSTRAINT PK_Dim_Utente PRIMARY KEY CLUSTERED CONSTRAINT DFT_Dim_Utente_PKUtente DEFAULT (NEXT VALUE FOR dbo.seq_Dim_Utente),
	    IDUtente INT NOT NULL,

	    HistoricalHashKey VARBINARY(20) NULL,
	    ChangeHashKey VARBINARY(20) NULL,
	    HistoricalHashKeyASCII VARCHAR(34) NULL,
	    ChangeHashKeyASCII VARCHAR(34) NULL,
	    InsertDatetime DATETIME NOT NULL,
	    UpdateDatetime DATETIME NOT NULL,
	    IsDeleted BIT NOT NULL,

	    Email NVARCHAR(60) NOT NULL,
	    RagioneSociale NVARCHAR(120) NOT NULL,
	    Nome NVARCHAR(60) NOT NULL,
	    Cognome NVARCHAR(60) NOT NULL,
	    Citta NVARCHAR(60) NOT NULL,
        PKCliente INT NOT NULL CONSTRAINT FK_Dim_Utente_PKCliente FOREIGN KEY REFERENCES Dim.Cliente (PKCliente)
    );

    CREATE UNIQUE NONCLUSTERED INDEX IX_Dim_Utente_id_Utente ON Dim.Utente (IDUtente);
    
    ALTER TABLE Dim.Utente ADD CONSTRAINT DFT_Dim_Utente_InsertDatetime DEFAULT (CURRENT_TIMESTAMP) FOR InsertDatetime;
    ALTER TABLE Dim.Utente ADD CONSTRAINT DFT_Dim_Utente_UpdateDatetime DEFAULT (CURRENT_TIMESTAMP) FOR UpdateDatetime;
    ALTER TABLE Dim.Utente ADD CONSTRAINT DFT_Dim_Utente_IsDeleted DEFAULT (0) FOR IsDeleted;

    INSERT INTO Dim.Utente (
        PKUtente,
        IDUtente,
        Email,
        RagioneSociale,
        Nome,
        Cognome,
        Citta,
        PKCliente
    )
    VALUES
    (   -1, -- PKUtente - int
        -1,       -- IDUtente - int
        N'',     -- Email - nvarchar(60)
        N'',     -- RagioneSociale - nvarchar(120)
        N'',     -- Nome - nvarchar(60)
        N'',     -- Cognome - nvarchar(60)
        N'',      -- Citta - nvarchar(60)
        -1     -- PKCliente - int
    ), (
        -101,
        -101,
        N'<???>',
        N'<???>',
        N'<???>',
        N'<???>',
        N'<???>',
        -1
    );

    ALTER SEQUENCE dbo.seq_Dim_Utente RESTART WITH 1;

END;
GO

IF OBJECT_ID(N'Dim.usp_Merge_Utente', N'P') IS NULL EXEC('CREATE PROCEDURE Dim.usp_Merge_Utente AS RETURN 0;');
GO

ALTER PROCEDURE Dim.usp_Merge_Utente
AS
BEGIN

    SET NOCOUNT ON;

    BEGIN TRANSACTION 

    DECLARE @provider_name NVARCHAR(60) = N'MyDatamartReporting';
    DECLARE @full_table_name sysname = N'Landing.COMETA_Utente';

    MERGE INTO Dim.Utente AS TGT
    USING Staging.Utente (nolock) AS SRC
    ON SRC.IDUtente = TGT.IDUtente

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.IsDeleted = SRC.IsDeleted,
        
        TGT.Email = SRC.Email,
        TGT.RagioneSociale = SRC.RagioneSociale,
        TGT.Nome = SRC.Nome,
        TGT.Cognome = SRC.Cognome,
        TGT.Citta = SRC.Citta,
        TGT.PKCliente = SRC.PKCliente

    WHEN NOT MATCHED
      THEN INSERT (
        IDUtente,
        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
        Email,
        RagioneSociale,
        Nome,
        Cognome,
        Citta,
        PKCliente
      )
      VALUES (
        SRC.IDUtente,
        SRC.HistoricalHashKey,
        SRC.ChangeHashKey,
        SRC.HistoricalHashKeyASCII,
        SRC.ChangeHashKeyASCII,
        SRC.InsertDatetime,
        SRC.UpdateDatetime,
        SRC.IsDeleted,
        SRC.Email,
        SRC.RagioneSociale,
        SRC.Nome,
        SRC.Cognome,
        SRC.Citta,
        SRC.PKCliente
      )

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        $action AS merge_action,
        'Staging.Utente' AS full_olap_table_name,
        'IDUtente = ' + CAST(COALESCE(inserted.IDUtente, deleted.IDUtente) AS NVARCHAR(1000)) AS primary_key_description
    INTO audit.merge_log_details;

    --DELETE FROM Dim.Utente
    --WHERE IsDeleted = CAST(1 AS BIT);

    UPDATE audit.tables
    SET lastupdated_local = lastupdated_staging
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    COMMIT TRANSACTION;

END;
GO

EXEC Dim.usp_Merge_Utente;
GO
