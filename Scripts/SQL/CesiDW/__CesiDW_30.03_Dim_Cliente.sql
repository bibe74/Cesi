USE CesiDW;
GO

/*
SET NOEXEC OFF;
--*/ SET NOEXEC ON;
GO

/*
    SCHEMA_NAME > COMETA
    TABLE_NAME > SoggettoCommerciale
    STAGING_TABLE_NAME > SoggettoCommerciale
*/

/**
 * @table Staging.SoggettoCommerciale
 * @description

 * @depends Landing.COMETA_SoggettoCommerciale

SELECT TOP 1 * FROM Landing.COMETA_SoggettoCommerciale;
*/

--DROP TABLE IF EXISTS Staging.SoggettoCommerciale; DELETE FROM audit.tables WHERE provider_name = N'MyDatamartReporting' AND full_table_name = N'Landing.COMETA_SoggettoCommerciale';
GO

IF NOT EXISTS (SELECT 1 FROM audit.tables WHERE provider_name = N'MyDatamartReporting' AND full_table_name = N'Landing.COMETA_SoggettoCommerciale')
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
        N'Landing.COMETA_SoggettoCommerciale',      -- full_table_name - sysname
        N'Staging.SoggettoCommerciale',      -- staging_table_name - sysname
        N'Dim.SoggettoCommerciale',      -- datawarehouse_table_name - sysname
        NULL, -- lastupdated_staging - datetime
        NULL  -- lastupdated_local - datetime
    );

END;
GO

IF OBJECT_ID(N'Staging.SoggettoCommercialeView', N'V') IS NULL EXEC('CREATE VIEW Staging.SoggettoCommercialeView AS SELECT 1 AS fld;');
GO

ALTER VIEW Staging.SoggettoCommercialeView
AS
WITH TableData
AS (
    SELECT
        SC.id_sog_commerciale AS IDSoggettoCommerciale,

        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            SC.id_sog_commerciale,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            SC.codice,
            SC.id_anagrafica,
            SC.tipo,
            A.rag_soc_1,
            A.rag_soc_2,
            A.indirizzo,
            A.cap,
            A.localita,
            A.provincia,
            A.nazione,
            A.cod_fiscale,
            A.par_iva,
            A.indirizzo2,
            GA.PKGruppoAgenti,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,

        SC.codice AS CodiceSoggettoCommerciale,
        SC.id_anagrafica AS IDAnagrafica,
        SC.tipo AS TipoSoggettoCommerciale,
        --A.rag_soc_1,
        --A.rag_soc_2,
        RTRIM(LTRIM(RTRIM(LTRIM(COALESCE(A.rag_soc_1, N'')) + N' ' + RTRIM(LTRIM(COALESCE(A.rag_soc_2, N'')))))) AS RagioneSociale,
        --A.indirizzo,
        --A.indirizzo2,
        RTRIM(LTRIM(RTRIM(LTRIM(COALESCE(A.indirizzo, N'')) + N' ' + RTRIM(LTRIM(COALESCE(A.indirizzo2, N'')))))) AS Indirizzo,
        COALESCE(A.cap, N'') AS CAP,
        COALESCE(A.localita, N'') AS Localita,
        COALESCE(A.provincia, N'') AS Provincia,
        COALESCE(A.nazione, N'') AS Nazione,
        COALESCE(A.cod_fiscale, N'') AS CodiceFiscale,
        COALESCE(A.par_iva, N'') AS PartitaIVA,
        --SC.id_gruppo_agenti,
        COALESCE(GA.PKGruppoAgenti, -1) AS PKGruppoAgenti

    FROM Landing.COMETA_SoggettoCommerciale SC
    INNER JOIN Landing.COMETA_Anagrafica A ON A.id_anagrafica = SC.id_anagrafica
    LEFT JOIN Dim.GruppoAgenti GA ON GA.id_gruppo_agenti = SC.id_gruppo_agenti
        AND SC.tipo = 'C' -- C: Cliente
    WHERE SC.IsDeleted = CAST(0 AS BIT)
)
SELECT
    -- Chiavi
    TD.IDSoggettoCommerciale,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Altri campi
    TD.CodiceSoggettoCommerciale,
    TD.IDAnagrafica,
    TD.TipoSoggettoCommerciale,
    TD.RagioneSociale,
    TD.Indirizzo,
    TD.CAP,
    TD.Localita,
    TD.Provincia,
    TD.Nazione,
    TD.CodiceFiscale,
    TD.PartitaIVA,
    TD.PKGruppoAgenti

FROM TableData TD;
GO

--IF OBJECT_ID(N'Staging.SoggettoCommerciale', N'U') IS NOT NULL DROP TABLE Staging.SoggettoCommerciale;
GO

IF OBJECT_ID(N'Staging.SoggettoCommerciale', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Staging.SoggettoCommerciale FROM Staging.SoggettoCommercialeView;

    ALTER TABLE Staging.SoggettoCommerciale ADD CONSTRAINT PK_Landing_COMETA_SoggettoCommerciale PRIMARY KEY CLUSTERED (UpdateDatetime, IDSoggettoCommerciale);

    ALTER TABLE Staging.SoggettoCommerciale ALTER COLUMN CodiceSoggettoCommerciale NVARCHAR(10) NOT NULL;
    ALTER TABLE Staging.SoggettoCommerciale ALTER COLUMN IDAnagrafica INT NOT NULL;
    ALTER TABLE Staging.SoggettoCommerciale ALTER COLUMN TipoSoggettoCommerciale CHAR(1) NOT NULL;
    ALTER TABLE Staging.SoggettoCommerciale ALTER COLUMN RagioneSociale NVARCHAR(120) NOT NULL;
    ALTER TABLE Staging.SoggettoCommerciale ALTER COLUMN Indirizzo NVARCHAR(120) NOT NULL;
    ALTER TABLE Staging.SoggettoCommerciale ALTER COLUMN CAP NVARCHAR(10) NOT NULL;
    ALTER TABLE Staging.SoggettoCommerciale ALTER COLUMN Localita NVARCHAR(60) NOT NULL;
    ALTER TABLE Staging.SoggettoCommerciale ALTER COLUMN Provincia NVARCHAR(10) NOT NULL;
    ALTER TABLE Staging.SoggettoCommerciale ALTER COLUMN Nazione NVARCHAR(60) NOT NULL;
    ALTER TABLE Staging.SoggettoCommerciale ALTER COLUMN CodiceFiscale NVARCHAR(20) NOT NULL;
    ALTER TABLE Staging.SoggettoCommerciale ALTER COLUMN PartitaIVA NVARCHAR(20) NOT NULL;
    ALTER TABLE Staging.SoggettoCommerciale ALTER COLUMN PKGruppoAgenti INT NOT NULL;

    CREATE UNIQUE NONCLUSTERED INDEX IX_COMETA_SoggettoCommerciale_BusinessKey ON Staging.SoggettoCommerciale (IDSoggettoCommerciale);
END;
GO

IF OBJECT_ID(N'Staging.usp_Reload_SoggettoCommerciale', N'P') IS NULL EXEC('CREATE PROCEDURE Staging.usp_Reload_SoggettoCommerciale AS RETURN 0;');
GO

ALTER PROCEDURE Staging.usp_Reload_SoggettoCommerciale
AS
BEGIN

    SET NOCOUNT ON;

    DECLARE @lastupdated_staging DATETIME;
    DECLARE @provider_name NVARCHAR(60) = N'MyDatamartReporting';
    DECLARE @full_table_name sysname = N'Landing.COMETA_SoggettoCommerciale';

    SELECT TOP 1 @lastupdated_staging = lastupdated_staging
    FROM audit.tables
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    IF (@lastupdated_staging IS NULL) SET @lastupdated_staging = CAST('19000101' AS DATETIME);

    BEGIN TRANSACTION

    TRUNCATE TABLE Staging.SoggettoCommerciale;

    INSERT INTO Staging.SoggettoCommerciale
    SELECT * FROM Staging.SoggettoCommercialeView;
    --WHERE UpdateDatetime > @lastupdated_staging;

    SELECT @lastupdated_staging = MAX(UpdateDatetime) FROM Staging.SoggettoCommerciale;

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

EXEC Staging.usp_Reload_SoggettoCommerciale;
GO

/*
    SCHEMA_NAME > COMETA
    TABLE_NAME > Telefono
    STAGING_TABLE_NAME > SoggettoCommerciale_Email
*/

/**
 * @table Staging.SoggettoCommerciale_Email
 * @description

 * @depends Landing.COMETA_Telefono

SELECT TOP 1 * FROM Landing.COMETA_Telefono;
*/

--DROP TABLE IF EXISTS Staging.SoggettoCommerciale_Email; DELETE FROM audit.tables WHERE provider_name = N'MyDatamartReporting' AND full_table_name = N'Landing.COMETA_Telefono';
GO

IF NOT EXISTS (SELECT 1 FROM audit.tables WHERE provider_name = N'MyDatamartReporting' AND full_table_name = N'Landing.COMETA_Telefono')
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
        N'Landing.COMETA_Telefono',      -- full_table_name - sysname
        N'Staging.SoggettoCommerciale_Email',      -- staging_table_name - sysname
        N'',      -- datawarehouse_table_name - sysname
        NULL, -- lastupdated_staging - datetime
        NULL  -- lastupdated_local - datetime
    );

END;
GO

IF OBJECT_ID(N'Staging.SoggettoCommerciale_EmailView', N'V') IS NULL EXEC('CREATE VIEW Staging.SoggettoCommerciale_EmailView AS SELECT 1 AS fld;');
GO

ALTER VIEW Staging.SoggettoCommerciale_EmailView
AS
WITH TelefonoSoggettoCommerciale
AS (
    SELECT DISTINCT
        SC.IDSoggettoCommerciale,
        T.num_riferimento AS Email

    FROM Landing.COMETA_Telefono T
    INNER JOIN Staging.SoggettoCommerciale SC ON SC.IDAnagrafica = T.id_anagrafica
        AND SC.TipoSoggettoCommerciale = 'C'
    WHERE T.tipo = 'E'
        AND T.num_riferimento LIKE N'%@%'
        AND T.descrizione = N'Abbonato'
        AND T.IsDeleted = CAST(0 AS BIT)
),
TableData
AS (
    SELECT
        TSC.IDSoggettoCommerciale,
        TSC.Email,

        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            TSC.IDSoggettoCommerciale,
            TSC.Email,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            ROW_NUMBER() OVER (PARTITION BY TSC.IDSoggettoCommerciale ORDER BY TSC.Email),
            ROW_NUMBER() OVER (PARTITION BY TSC.Email ORDER BY TSC.IDSoggettoCommerciale DESC),
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,

        ROW_NUMBER() OVER (PARTITION BY TSC.IDSoggettoCommerciale ORDER BY TSC.Email) AS rnEmail,
        ROW_NUMBER() OVER (PARTITION BY TSC.Email ORDER BY TSC.IDSoggettoCommerciale DESC) AS rnSoggettoCommercialeDESC

    FROM TelefonoSoggettoCommerciale TSC
)
SELECT
    -- Chiavi
    TD.IDSoggettoCommerciale,
    TD.Email,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Altri campi
    TD.rnEmail,
    TD.rnSoggettoCommercialeDESC

FROM TableData TD;
GO

--IF OBJECT_ID(N'Staging.SoggettoCommerciale_Email', N'U') IS NOT NULL DROP TABLE Staging.SoggettoCommerciale_Email;
GO

IF OBJECT_ID(N'Staging.SoggettoCommerciale_Email', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Staging.SoggettoCommerciale_Email FROM Staging.SoggettoCommerciale_EmailView;

    ALTER TABLE Staging.SoggettoCommerciale_Email ALTER COLUMN Email NVARCHAR(120) NOT NULL;

    ALTER TABLE Staging.SoggettoCommerciale_Email ADD CONSTRAINT PK_Landing_COMETA_Telefono PRIMARY KEY CLUSTERED (UpdateDatetime, IDSoggettoCommerciale, Email);

    ALTER TABLE Staging.SoggettoCommerciale_Email ALTER COLUMN rnEmail INT NOT NULL;
    ALTER TABLE Staging.SoggettoCommerciale_Email ALTER COLUMN rnSoggettoCommercialeDESC INT NOT NULL;

    CREATE UNIQUE NONCLUSTERED INDEX Staging_SoggettoCommerciale_Email_BusinessKey ON Staging.SoggettoCommerciale_Email (IDSoggettoCommerciale, Email);

    CREATE UNIQUE NONCLUSTERED INDEX Staging_SoggettoCommerciale_Email_Email_rnSoggettoCommercialeDESC ON Staging.SoggettoCommerciale_Email (Email, rnSoggettoCommercialeDESC);
    CREATE UNIQUE NONCLUSTERED INDEX Staging_SoggettoCommerciale_IDSoggettoCommerciale_rnEmail ON Staging.SoggettoCommerciale_Email (IDSoggettoCommerciale, rnEmail);
END;
GO

IF OBJECT_ID(N'Staging.usp_Reload_SoggettoCommerciale_Email', N'P') IS NULL EXEC('CREATE PROCEDURE Staging.usp_Reload_SoggettoCommerciale_Email AS RETURN 0;');
GO

ALTER PROCEDURE Staging.usp_Reload_SoggettoCommerciale_Email
AS
BEGIN

    SET NOCOUNT ON;

    DECLARE @lastupdated_staging DATETIME;
    DECLARE @provider_name NVARCHAR(60) = N'MyDatamartReporting';
    DECLARE @full_table_name sysname = N'Landing.COMETA_Telefono';

    SELECT TOP 1 @lastupdated_staging = lastupdated_staging
    FROM audit.tables
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    IF (@lastupdated_staging IS NULL) SET @lastupdated_staging = CAST('19000101' AS DATETIME);

    BEGIN TRANSACTION

    TRUNCATE TABLE Staging.SoggettoCommerciale_Email;

    INSERT INTO Staging.SoggettoCommerciale_Email
    SELECT * FROM Staging.SoggettoCommerciale_EmailView;
    --WHERE UpdateDatetime > @lastupdated_staging;

    SELECT @lastupdated_staging = MAX(UpdateDatetime) FROM Staging.SoggettoCommerciale_Email;

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

EXEC Staging.usp_Reload_SoggettoCommerciale_Email;
GO

/*
    SCHEMA_NAME > COMETA
    TABLE_NAME > MySolutionUsers
    STAGING_TABLE_NAME > MySolutionUsers
*/

/**
 * @table Staging.MySolutionUsers
 * @description

 * @depends Landing.COMETA_MySolutionUsers

SELECT TOP 1 * FROM Landing.COMETA_MySolutionUsers;
*/

--DROP TABLE IF EXISTS Staging.MySolutionUsers; DELETE FROM audit.tables WHERE provider_name = N'MyDatamartReporting' AND full_table_name = N'COMETA.MySolutionUsers';
GO

IF NOT EXISTS (SELECT 1 FROM audit.tables WHERE provider_name = N'MyDatamartReporting' AND full_table_name = N'Landing.COMETA_MySolutionUsers')
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
        N'Landing.COMETA_MySolutionUsers',      -- full_table_name - sysname
        N'Staging.MySolutionUsers',      -- staging_table_name - sysname
        N'',      -- datawarehouse_table_name - sysname
        NULL, -- lastupdated_staging - datetime
        NULL  -- lastupdated_local - datetime
    );

END;
GO

IF OBJECT_ID(N'Staging.MySolutionUsersView', N'V') IS NULL EXEC('CREATE VIEW Staging.MySolutionUsersView AS SELECT 1 AS fld;');
GO

ALTER VIEW Staging.MySolutionUsersView
AS
WITH MySolutionUsersDetail
AS (
    SELECT
        MSU.EMail,
        ROW_NUMBER() OVER (PARTITION BY MSU.EMail ORDER BY MSU.data_inizio_contratto DESC) AS rnDataInizioContrattoDESC,
        ROW_NUMBER() OVER (PARTITION BY MSU.EMail, MSU.codice ORDER BY MSU.data_inizio_contratto DESC) AS rnCodiceDataInizioContrattoDESC,

        COALESCE(MSU.codice, N'') AS CodiceCliente,
        COALESCE(MSU.RagioneSociale, N'') AS RagioneSociale,
        COALESCE(MSU.cod_fiscale, N'') AS CodiceFiscale,
        COALESCE(MSU.par_iva, N'') AS PartitaIVA,
        COALESCE(MSU.localita, N'') AS Localita,
        COALESCE(MSU.provincia, N'') AS Provincia,
        COALESCE(T.num_riferimento, N'') AS Telefono,
        COALESCE(MSU.tipo, N'') AS TipoCliente,
        --MSU.data_inizio_contratto,
        COALESCE(DIC.PKData, CAST('19000101' AS DATE)) AS PKDataInizioContratto,
        --MSU.data_fine_contratto,
        COALESCE(DFC.PKData, CAST('19000101' AS DATE)) AS PKDataFineContratto,
        COALESCE(MSU.Cognome, N'') AS Cognome,
        COALESCE(MSU.Nome, N'') AS Nome,
        COALESCE(MSU.id_documento, -1) AS IDDocumento

    FROM Landing.COMETA_MySolutionUsers MSU
    LEFT JOIN Dim.Data DIC ON DIC.PKData = MSU.data_inizio_contratto
    LEFT JOIN Dim.Data DFC ON DFC.PKData = MSU.data_fine_contratto
    LEFT JOIN Landing.COMETA_Anagrafica A ON A.id_anagrafica = MSU.id_anagrafica
    LEFT JOIN Landing.COMETA_Telefono T ON T.id_telefono = MSU.id_telefono
        AND T.IsDeleted = CAST(0 AS BIT)
),
TableData
AS (
    SELECT
        MSUD.Email,
        MSUD.rnDataInizioContrattoDESC,

        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            MSUD.Email,
            MSUD.rnDataInizioContrattoDESC,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            MSUD.rnDataInizioContrattoDESC,
            MSUD.CodiceCliente,
            MSUD.RagioneSociale,
            MSUD.CodiceFiscale,
            MSUD.PartitaIVA,
            MSUD.Localita,
            MSUD.Provincia,
            MSUD.Telefono,
            MSUD.TipoCliente,
            MSUD.PKDataInizioContratto,
            MSUD.PKDataFineContratto,
            MSUD.Cognome,
            MSUD.Nome,
            MSUD.IDDocumento,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,

        MSUD.rnCodiceDataInizioContrattoDESC,
        MSUD.CodiceCliente,
        MSUD.RagioneSociale,
        MSUD.CodiceFiscale,
        MSUD.PartitaIVA,
        MSUD.Localita,
        MSUD.Provincia,
        MSUD.Telefono,
        MSUD.TipoCliente,
        MSUD.PKDataInizioContratto,
        MSUD.PKDataFineContratto,
        MSUD.Cognome,
        MSUD.Nome,
        MSUD.IDDocumento

    FROM MySolutionUsersDetail MSUD
)
SELECT
    -- Chiavi
    TD.Email,
    TD.rnDataInizioContrattoDESC,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Altri campi
    TD.rnCodiceDataInizioContrattoDESC,
    TD.CodiceCliente,
    TD.RagioneSociale,
    TD.CodiceFiscale,
    TD.PartitaIVA,
    TD.Localita,
    TD.Provincia,
    TD.Telefono,
    TD.TipoCliente,
    TD.PKDataInizioContratto,
    TD.PKDataFineContratto,
    TD.Cognome,
    TD.Nome,
    TD.IDDocumento

FROM TableData TD;
GO

--IF OBJECT_ID(N'Staging.MySolutionUsers', N'U') IS NOT NULL DROP TABLE Staging.MySolutionUsers;
GO

IF OBJECT_ID(N'Staging.MySolutionUsers', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Staging.MySolutionUsers FROM Staging.MySolutionUsersView;

    ALTER TABLE Staging.MySolutionUsers ALTER COLUMN Email NVARCHAR(120) NOT NULL;
    ALTER TABLE Staging.MySolutionUsers ALTER COLUMN rnDataInizioContrattoDESC INT NOT NULL;

    ALTER TABLE Staging.MySolutionUsers ADD CONSTRAINT PK_Landing_COMETA_MySolutionUsers PRIMARY KEY CLUSTERED (UpdateDatetime, Email, rnDataInizioContrattoDESC);

    ALTER TABLE Staging.MySolutionUsers ALTER COLUMN rnCodiceDataInizioContrattoDESC INT NOT NULL;
    ALTER TABLE Staging.MySolutionUsers ALTER COLUMN CodiceCliente NVARCHAR(10) NOT NULL;
    ALTER TABLE Staging.MySolutionUsers ALTER COLUMN RagioneSociale NVARCHAR(120) NOT NULL;
    ALTER TABLE Staging.MySolutionUsers ALTER COLUMN CodiceFiscale NVARCHAR(20) NOT NULL;
    ALTER TABLE Staging.MySolutionUsers ALTER COLUMN PartitaIVA NVARCHAR(20) NOT NULL;
    ALTER TABLE Staging.MySolutionUsers ALTER COLUMN Localita NVARCHAR(60) NOT NULL;
    ALTER TABLE Staging.MySolutionUsers ALTER COLUMN Provincia NVARCHAR(10) NOT NULL;
    ALTER TABLE Staging.MySolutionUsers ALTER COLUMN Telefono NVARCHAR(60) NOT NULL;
    ALTER TABLE Staging.MySolutionUsers ALTER COLUMN TipoCliente NVARCHAR(10) NOT NULL;
    ----ALTER TABLE Staging.MySolutionUsers ALTER COLUMN Agente NVARCHAR(20) NOT NULL;
    ALTER TABLE Staging.MySolutionUsers ALTER COLUMN PKDataInizioContratto DATE NOT NULL;
    ALTER TABLE Staging.MySolutionUsers ALTER COLUMN PKDataFineContratto DATE NOT NULL;
    ----ALTER TABLE Staging.MySolutionUsers ALTER COLUMN PKDataDisdetta DATE NOT NULL;
    ----ALTER TABLE Staging.MySolutionUsers ALTER COLUMN StatoDisdetta NVARCHAR(10) NOT NULL;
    ----ALTER TABLE Staging.MySolutionUsers ALTER COLUMN RiferimentoDisdetta NVARCHAR(60) NOT NULL;
    ALTER TABLE Staging.MySolutionUsers ALTER COLUMN Cognome NVARCHAR(60) NOT NULL;
    ALTER TABLE Staging.MySolutionUsers ALTER COLUMN Nome NVARCHAR(60) NOT NULL;
    ALTER TABLE Staging.MySolutionUsers ALTER COLUMN IDDocumento INT NOT NULL;

    CREATE UNIQUE NONCLUSTERED INDEX IX_COMETA_MySolutionUsers_BusinessKey ON Staging.MySolutionUsers (Email, rnDataInizioContrattoDESC);
END;
GO

IF OBJECT_ID(N'Staging.usp_Reload_MySolutionUsers', N'P') IS NULL EXEC('CREATE PROCEDURE Staging.usp_Reload_MySolutionUsers AS RETURN 0;');
GO

ALTER PROCEDURE Staging.usp_Reload_MySolutionUsers
AS
BEGIN

    SET NOCOUNT ON;

    DECLARE @lastupdated_staging DATETIME;
    DECLARE @provider_name NVARCHAR(60) = N'MyDatamartReporting';
    DECLARE @full_table_name sysname = N'COMETA.MySolutionUsers';

    SELECT TOP 1 @lastupdated_staging = lastupdated_staging
    FROM audit.tables
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    IF (@lastupdated_staging IS NULL) SET @lastupdated_staging = CAST('19000101' AS DATETIME);

    BEGIN TRANSACTION

    TRUNCATE TABLE Staging.MySolutionUsers;

    INSERT INTO Staging.MySolutionUsers
    SELECT * FROM Staging.MySolutionUsersView;
    --WHERE UpdateDatetime > @lastupdated_staging;

    SELECT @lastupdated_staging = MAX(UpdateDatetime) FROM Staging.MySolutionUsers;

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

EXEC Staging.usp_Reload_MySolutionUsers;
GO

/*
    SCHEMA_NAME > MYSOLUTION
    TABLE_NAME > Customer
    STAGING_TABLE_NAME > Customer
*/

/**
 * @table Staging.Customer
 * @description

 * @depends Landing.MYSOLUTION_Customer

SELECT TOP 1 * FROM Landing.MYSOLUTION_Customer;
*/

--DROP TABLE IF EXISTS Staging.Customer; DELETE FROM audit.tables WHERE provider_name = N'MyDatamartReporting' AND full_table_name = N'Landing.MYSOLUTION_Customer';
GO

IF NOT EXISTS (SELECT 1 FROM audit.tables WHERE provider_name = N'MyDatamartReporting' AND full_table_name = N'Landing.MYSOLUTION_Customer')
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
        N'Landing.MYSOLUTION_Customer',      -- full_table_name - sysname
        N'Staging.Customer',      -- staging_table_name - sysname
        N'',      -- datawarehouse_table_name - sysname
        NULL, -- lastupdated_staging - datetime
        NULL  -- lastupdated_local - datetime
    );

END;
GO

IF OBJECT_ID(N'Staging.CustomerView', N'V') IS NULL EXEC('CREATE VIEW Staging.CustomerView AS SELECT 1 AS fld;');
GO

ALTER VIEW Staging.CustomerView
AS
WITH TableDataDetail
AS (
    SELECT
        C.Id,

        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            C.Id,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            C.Username,
            C.Email,
            C.IdCometa,
            GA1.Value,
            GA2.Value,
            GA3.Value,
            GA4.Value,
            GA5.Value,
            GA6.Value,
            GA7.Value,
            GA8.Value,
            GA9.Value,
            GA10.Value,
            GA11.Value,
            CY.Name,
            GA12.Value,
            SP.Name,
            CCRM11.Customer_Id,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,

        LOWER(C.Username) AS Username,
        LOWER(C.Email) AS Email,
        C.IdCometa,
        COALESCE(GA1.Value, A.Company, N'') AS Company,
        COALESCE(GA2.Value, A.CodiceFiscale, N'') AS CodiceFiscale,
        COALESCE(GA3.Value, A.Piva, N'') AS VATNumber,
        COALESCE(GA4.Value, A.FirstName, N'') AS FirstName,
        COALESCE(GA5.Value, A.LastName, N'') AS LastName,
        COALESCE(GA6.Value, A.Address1 + N' ' + COALESCE(A.Address2, N''), N'') AS StreetAddress,
        COALESCE(GA7.Value, A.ZipPostalCode, N'') AS ZipPostalCode,
        COALESCE(GA8.Value, A.PhoneNumber, N'') AS Phone,
        COALESCE(GA9.Value, N'') AS Cellulare,
        COALESCE(GA10.Value, A.City, N'') AS City,
        COALESCE(GA11.Value, N'') AS CountryId,
        COALESCE(CY.Name, A.Country, N'') AS Country,
        COALESCE(GA12.Value, N'') AS StateProvinceId,
        COALESCE(SP.Name, A.StateProvince, N'') AS StateProvince,
        ROW_NUMBER() OVER (PARTITION BY C.Id ORDER BY A.Id DESC) AS rnAddressDESC,
        ROW_NUMBER() OVER (PARTITION BY C.Username ORDER BY C.Id DESC, A.Id DESC) AS rnCustomerDESC,
        CASE WHEN CCRM11.Customer_Id IS NOT NULL THEN 1 ELSE 0 END AS HasRoleMySolutionDemo

    FROM Landing.MYSOLUTION_Customer C
    LEFT JOIN Landing.MYSOLUTION_CustomerAddresses CA ON CA.Customer_Id = C.Id
    LEFT JOIN Landing.MYSOLUTION_Address A ON A.Id = CA.Address_Id
    LEFT JOIN Landing.MYSOLUTION_GenericAttribute GA1 ON GA1.EntityId = C.Id
        AND GA1.[Key] = N'Company'
    LEFT JOIN Landing.MYSOLUTION_GenericAttribute GA2 ON GA2.EntityId = C.Id
        AND GA2.[Key] = N'CodiceFiscale'
    LEFT JOIN Landing.MYSOLUTION_GenericAttribute GA3 ON GA3.EntityId = C.Id
        AND GA3.[Key] = N'VATNumber'
    LEFT JOIN Landing.MYSOLUTION_GenericAttribute GA4 ON GA4.EntityId = C.Id
        AND GA4.[Key] = N'FirstName'
    LEFT JOIN Landing.MYSOLUTION_GenericAttribute GA5 ON GA5.EntityId = C.Id
        AND GA5.[Key] = N'LastName'
    LEFT JOIN Landing.MYSOLUTION_GenericAttribute GA6 ON GA6.EntityId = C.Id
        AND GA6.[Key] = N'StreetAddress'
    LEFT JOIN Landing.MYSOLUTION_GenericAttribute GA7 ON GA7.EntityId = C.Id
        AND GA7.[Key] = N'ZipPostalCode'
    LEFT JOIN Landing.MYSOLUTION_GenericAttribute GA8 ON GA8.EntityId = C.Id
        AND GA8.[Key] = N'Phone'
    LEFT JOIN Landing.MYSOLUTION_GenericAttribute GA9 ON GA9.EntityId = C.Id
        AND GA9.[Key] = N'Cellulare'
    LEFT JOIN Landing.MYSOLUTION_GenericAttribute GA10 ON GA10.EntityId = C.Id
        AND GA10.[Key] = N'City'
    LEFT JOIN Landing.MYSOLUTION_GenericAttribute GA11 ON GA11.EntityId = C.Id
        AND GA11.[Key] = N'CountryId'
    LEFT JOIN Landing.MYSOLUTION_Country CY ON CY.Id = GA11.Value
    LEFT JOIN Landing.MYSOLUTION_GenericAttribute GA12 ON GA12.EntityId = C.Id
        AND GA12.[Key] = N'StateProvinceId'
    LEFT JOIN Landing.MYSOLUTION_StateProvince SP ON SP.Id = GA12.Value
    LEFT JOIN Landing.MYSOLUTION_Customer_CustomerRole_Mapping CCRM11 ON CCRM11.Customer_Id = C.Id
        AND CCRM11.CustomerRole_Id = 11 -- 11: MySolution.Demo
)
SELECT
    -- Chiavi
    TDD.Id,

    -- Campi per sincronizzazione
    TDD.HistoricalHashKey,
    TDD.ChangeHashKey,
    CONVERT(VARCHAR(34), TDD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TDD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TDD.InsertDatetime,
    TDD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Altri campi
    TDD.Username,
    TDD.Email,
    TDD.IdCometa,
    TDD.Company,
    TDD.CodiceFiscale,
    TDD.VATNumber,
    TDD.FirstName,
    TDD.LastName,
    TDD.StreetAddress,
    TDD.ZipPostalCode,
    TDD.Phone,
    TDD.Cellulare,
    TDD.City,
    TDD.CountryId,
    TDD.Country,
    TDD.StateProvinceId,
    TDD.StateProvince,
    TDD.rnCustomerDESC,
    TDD.HasRoleMySolutionDemo

FROM TableDataDetail TDD
WHERE TDD.rnAddressDESC = 1;
GO

--IF OBJECT_ID(N'Staging.Customer', N'U') IS NOT NULL DROP TABLE Staging.Customer;
GO

IF OBJECT_ID(N'Staging.Customer', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Staging.Customer FROM Staging.CustomerView;

    ALTER TABLE Staging.Customer ADD CONSTRAINT PK_Landing_MYSOLUTION_Customer PRIMARY KEY CLUSTERED (UpdateDatetime, Id);

    ALTER TABLE Staging.Customer ALTER COLUMN HasRoleMySolutionDemo BIT NOT NULL;

    CREATE UNIQUE NONCLUSTERED INDEX IX_MYSOLUTION_Customer_BusinessKey ON Staging.Customer (Id);
END;
GO

IF OBJECT_ID(N'Staging.usp_Reload_Customer', N'P') IS NULL EXEC('CREATE PROCEDURE Staging.usp_Reload_Customer AS RETURN 0;');
GO

ALTER PROCEDURE Staging.usp_Reload_Customer
AS
BEGIN

    SET NOCOUNT ON;

    DECLARE @lastupdated_staging DATETIME;
    DECLARE @provider_name NVARCHAR(60) = N'MyDatamartReporting';
    DECLARE @full_table_name sysname = N'Landing.MYSOLUTION_Customer';

    SELECT TOP 1 @lastupdated_staging = lastupdated_staging
    FROM audit.tables
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    IF (@lastupdated_staging IS NULL) SET @lastupdated_staging = CAST('19000101' AS DATETIME);

    BEGIN TRANSACTION

    TRUNCATE TABLE Staging.Customer;

    INSERT INTO Staging.Customer
    SELECT * FROM Staging.CustomerView;
    --WHERE UpdateDatetime > @lastupdated_staging;

    SELECT @lastupdated_staging = MAX(UpdateDatetime) FROM Staging.Customer;

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

EXEC Staging.usp_Reload_Customer;
GO

/**
 * @table Staging.Cliente
 * @description Tabella di staging per dimensione Cliente

 * @depends Staging.SoggettoCommerciale
 * @depends Staging.SoggettoCommerciale_Email
 * @depends Staging.Customer
 * @depends Staging.InfoAccounts

SELECT TOP 1 * FROM Staging.SoggettoCommerciale;
SELECT TOP 1 * FROM Staging.SoggettoCommerciale_Email;
SELECT TOP 1 * FROM Staging.Customer;
SELECT TOP 1 * FROM Staging.InfoAccounts;
*/

IF OBJECT_ID(N'Staging.ClienteCometaView', N'V') IS NULL EXEC('CREATE VIEW Staging.ClienteCometaView AS SELECT 1 AS fld;');
GO

ALTER VIEW Staging.ClienteCometaView
AS
WITH TelefonoDettaglio
AS (
    SELECT
        id_anagrafica,
        tipo,
        num_riferimento,
        ROW_NUMBER() OVER (PARTITION BY id_anagrafica, tipo ORDER BY id_telefono DESC) AS rn
    FROM Landing.COMETA_Telefono T
    WHERE tipo IN ('T', 'C', 'F')
        AND COALESCE(num_riferimento, N'') <> N''
        AND T.IsDeleted = CAST(0 AS BIT)
),
NopCustomerDetail
AS (
    SELECT
        NOPC.Email,
        NOPC.HasRoleMySolutionDemo,
        ROW_NUMBER() OVER (PARTITION BY NOPC.Email ORDER BY NOPC.Id DESC) AS rn
    FROM Staging.Customer NOPC
),
TableData
AS (
    SELECT
	    T.IDSoggettoCommerciale,

	    CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            T.IDSoggettoCommerciale,
		    ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            T.Email,
            T.IDAnagraficaCometa,
            T.HasAnagraficaCometa,
            T.ProvenienzaAnagrafica,
            T.CodiceCliente,
            T.TipoSoggettoCommerciale,
            T.RagioneSociale,
            T.CodiceFiscale,
            T.PartitaIVA,
            T.Indirizzo,
            T.CAP,
            T.Localita,
            T.Provincia,
            T.Regione,
            T.Macroregione,
            T.Nazione,
            T.TipoCliente,
            T.Agente,
            T.PKDataInizioContratto,
            T.PKDataFineContratto,
            T.PKDataDisdetta,
            T.MotivoDisdetta,
            T.PKGruppoAgenti,
            T.Cognome,
            T.Nome,
            T.Telefono,
            T.Cellulare,
            T.Fax,
            T.IsAbbonato,
            T.IDProvincia,
            T.HasRoleMySolutionDemo,
		    ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,

        T.Email,
        T.IDAnagraficaCometa,
        T.HasAnagraficaCometa,
        CAST(0 AS BIT) AS HasAnagraficaNopCommerce,
        CAST(0 AS BIT) AS HasAnagraficaMySolution,
        T.ProvenienzaAnagrafica,
        T.CodiceCliente,
        T.TipoSoggettoCommerciale,
        T.RagioneSociale,
        T.CodiceFiscale,
        T.PartitaIVA,
        T.Indirizzo,
        T.CAP,
        T.Localita,
        T.Provincia,
        T.Regione,
        T.Macroregione,
        T.Nazione,
        T.TipoCliente,
        T.Agente,
        T.PKDataInizioContratto,
        T.PKDataFineContratto,
        T.PKDataDisdetta,
        T.MotivoDisdetta,
        T.PKGruppoAgenti,
        T.Cognome,
        T.Nome,
        T.Telefono,
        T.Cellulare,
        T.Fax,
        T.IsAbbonato,
        T.IDProvincia,
        T.HasRoleMySolutionDemo
	
    FROM (

        SELECT
            SC.IDSoggettoCommerciale,
            COALESCE(SCE.Email, N'') AS Email,
            SC.IDAnagrafica AS IDAnagraficaCometa,
            CAST(1 AS BIT) AS HasAnagraficaCometa,
            N'COMETA' AS ProvenienzaAnagrafica,
            SC.CodiceSoggettoCommerciale AS CodiceCliente,
            SC.TipoSoggettoCommerciale,
            SC.RagioneSociale,
            SC.CodiceFiscale,
            SC.PartitaIVA,
            SC.Indirizzo,
            SC.CAP,
            SC.Localita,
            COALESCE(P.DescrProvincia, CASE WHEN SC.Provincia = N'' THEN N'' ELSE N'<???>' END) AS Provincia,
            COALESCE(P.DescrRegione, CASE WHEN SC.Provincia = N'' THEN N'' ELSE N'<???>' END) AS Regione,
            COALESCE(P.DescrMacroregione, CASE WHEN SC.Provincia = N'' THEN N'' ELSE N'<???>' END) AS Macroregione,
            COALESCE(P.DescrNazione, SC.Nazione) AS Nazione,
            COALESCE(MSU.TipoCliente, N'') AS TipoCliente,
            COALESCE(GA.CapoArea, N'') AS Agente,
            COALESCE(MSU.PKDataInizioContratto, CAST('19000101' AS DATE)) AS PKDataInizioContratto,
            COALESCE(MSU.PKDataFineContratto, CAST('19000101' AS DATE)) AS PKDataFineContratto,
            COALESCE(DD.PKData, CAST('19000101' AS DATE)) AS PKDataDisdetta,
            COALESCE(D.motivo_disdetta, N'') AS MotivoDisdetta,
            SC.PKGruppoAgenti,
            COALESCE(MSU.Cognome, N'') AS Cognome,
            COALESCE(MSU.Nome, N'') AS Nome,
            COALESCE(TDT.num_riferimento, N'') AS Telefono,
            COALESCE(TDC.num_riferimento, N'') AS Cellulare,
            COALESCE(TDF.num_riferimento, N'') AS Fax,
            CASE WHEN MSU.Email IS NOT NULL THEN 1 ELSE 0 END AS IsAbbonato,
            SC.Provincia AS IDProvincia,
            COALESCE(NCD.HasRoleMySolutionDemo, 0) AS HasRoleMySolutionDemo

        FROM Staging.SoggettoCommerciale SC
        LEFT JOIN Staging.SoggettoCommerciale_Email SCE ON SCE.IDSoggettoCommerciale = SC.IDSoggettoCommerciale
            AND SCE.rnEmail = 1
        LEFT JOIN Staging.MySolutionUsers MSU ON MSU.Email = SCE.Email AND MSU.CodiceCliente = SC.CodiceSoggettoCommerciale
            AND MSU.rnCodiceDataInizioContrattoDESC = 1
        LEFT JOIN Dim.GruppoAgenti GA ON GA.PKGruppoAgenti = SC.PKGruppoAgenti
        LEFT JOIN Import.Provincia P ON P.CodSiglaProvincia = SC.Provincia
        LEFT JOIN Landing.COMETA_Documento D ON D.id_documento = MSU.IDDocumento
            AND D.id_libero_1 = 9 -- 9: Disdettato
        LEFT JOIN Dim.Data DD ON DD.PKData = D.data_disdetta
        LEFT JOIN TelefonoDettaglio TDT ON TDT.id_anagrafica = SC.IDAnagrafica
            AND TDT.tipo = N'T'
            AND TDT.rn = 1
        LEFT JOIN TelefonoDettaglio TDC ON TDC.id_anagrafica = SC.IDAnagrafica
            AND TDC.tipo = N'C'
            AND TDC.rn = 1
        LEFT JOIN TelefonoDettaglio TDF ON TDF.id_anagrafica = SC.IDAnagrafica
            AND TDF.tipo = N'F'
            AND TDF.rn = 1
        LEFT JOIN NopCustomerDetail NCD ON NCD.Email = SCE.Email
            AND NCD.rn = 1
        WHERE SC.TipoSoggettoCommerciale = 'C'

    ) T
)
SELECT
    -- Chiavi
    TD.IDSoggettoCommerciale,

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
    TD.IDAnagraficaCometa,
    TD.HasAnagraficaCometa,
    TD.HasAnagraficaNopCommerce,
    TD.HasAnagraficaMySolution,
    TD.ProvenienzaAnagrafica,
    TD.CodiceCliente,
    TD.TipoSoggettoCommerciale,
    TD.RagioneSociale,
    TD.CodiceFiscale,
    TD.PartitaIVA,
    TD.Indirizzo,
    TD.CAP,
    TD.Localita,
    TD.Provincia,
    TD.Regione,
    TD.Macroregione,
    TD.Nazione,
    TD.TipoCliente,
    TD.Agente,
    TD.PKDataInizioContratto,
    TD.PKDataFineContratto,
    TD.PKDataDisdetta,
    TD.MotivoDisdetta,
    TD.PKGruppoAgenti,
    TD.Cognome,
    TD.Nome,
    TD.Telefono,
    TD.Cellulare,
    TD.Fax,
    TD.IsAbbonato,
    TD.IDSoggettoCommerciale AS IDSoggettoCommerciale_migrazione,
    CAST(NULL AS INT) AS IDSoggettoCommerciale_migrazione_old,
    TD.IDProvincia,
    CAST(N'' AS NVARCHAR(60)) AS CapoAreaDefault,
    CAST(N'' AS NVARCHAR(60)) AS AgenteDefault,
    TD.HasRoleMySolutionDemo

FROM TableData TD;
GO

--IF OBJECT_ID(N'Staging.Cliente', N'U') IS NOT NULL DROP TABLE Staging.Cliente;
GO

IF OBJECT_ID(N'Staging.Cliente', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Staging.Cliente FROM Staging.ClienteCometaView;

    ALTER TABLE Staging.Cliente ADD CONSTRAINT PK_Staging_Cliente PRIMARY KEY CLUSTERED (UpdateDatetime, IDSoggettoCommerciale);

    ALTER TABLE Staging.Cliente ALTER COLUMN Email NVARCHAR(80) NOT NULL;
    ALTER TABLE Staging.Cliente ALTER COLUMN IDAnagraficaCometa INT NOT NULL;
    ALTER TABLE Staging.Cliente ALTER COLUMN HasAnagraficaCometa BIT NOT NULL;
    ALTER TABLE Staging.Cliente ALTER COLUMN HasAnagraficaNopCommerce BIT NOT NULL;
    ALTER TABLE Staging.Cliente ALTER COLUMN HasAnagraficaMySolution BIT NOT NULL;
    ALTER TABLE Staging.Cliente ALTER COLUMN ProvenienzaAnagrafica NVARCHAR(20) NOT NULL;
    ALTER TABLE Staging.Cliente ALTER COLUMN CodiceCliente NVARCHAR(10) NOT NULL;
    ALTER TABLE Staging.Cliente ALTER COLUMN RagioneSociale NVARCHAR(120) NOT NULL;
    ALTER TABLE Staging.Cliente ALTER COLUMN CodiceFiscale NVARCHAR(20) NOT NULL;
    ALTER TABLE Staging.Cliente ALTER COLUMN PartitaIVA NVARCHAR(20) NOT NULL;
    ALTER TABLE Staging.Cliente ALTER COLUMN Indirizzo NVARCHAR(120) NOT NULL;
    ALTER TABLE Staging.Cliente ALTER COLUMN CAP NVARCHAR(10) NOT NULL;
    ALTER TABLE Staging.Cliente ALTER COLUMN Localita NVARCHAR(60) NOT NULL;
    ALTER TABLE Staging.Cliente ALTER COLUMN Provincia NVARCHAR(50) NOT NULL;
    ALTER TABLE Staging.Cliente ALTER COLUMN Regione NVARCHAR(60) NOT NULL;
    ALTER TABLE Staging.Cliente ALTER COLUMN Macroregione NVARCHAR(60) NOT NULL;
    ALTER TABLE Staging.Cliente ALTER COLUMN Nazione NVARCHAR(60) NOT NULL;
    ALTER TABLE Staging.Cliente ALTER COLUMN TipoCliente NVARCHAR(10) NOT NULL;
    ALTER TABLE Staging.Cliente ALTER COLUMN Agente NVARCHAR(60) NOT NULL;
    ALTER TABLE Staging.Cliente ALTER COLUMN PKDataInizioContratto DATE NOT NULL;
    ALTER TABLE Staging.Cliente ALTER COLUMN PKDataFineContratto DATE NOT NULL;
    ALTER TABLE Staging.Cliente ALTER COLUMN PKDataDisdetta DATE NOT NULL;
    ALTER TABLE Staging.Cliente ALTER COLUMN MotivoDisdetta NVARCHAR(120) NOT NULL;
    ALTER TABLE Staging.Cliente ALTER COLUMN PKGruppoAgenti INT NOT NULL;
    ALTER TABLE Staging.Cliente ALTER COLUMN Cognome NVARCHAR(60) NOT NULL;
    ALTER TABLE Staging.Cliente ALTER COLUMN Nome NVARCHAR(60) NOT NULL;
    ALTER TABLE Staging.Cliente ALTER COLUMN Telefono NVARCHAR(60) NOT NULL;
    ALTER TABLE Staging.Cliente ALTER COLUMN Cellulare NVARCHAR(60) NOT NULL;
    ALTER TABLE Staging.Cliente ALTER COLUMN Fax NVARCHAR(60) NOT NULL;
    ALTER TABLE Staging.Cliente ALTER COLUMN IsAbbonato BIT NOT NULL;
    ALTER TABLE Staging.Cliente ALTER COLUMN IDSoggettoCommerciale_migrazione INT NULL;
    ALTER TABLE Staging.Cliente ALTER COLUMN IDSoggettoCommerciale_migrazione_old INT NULL;
    ALTER TABLE Staging.Cliente ALTER COLUMN IDProvincia NVARCHAR(10) NOT NULL;
    ALTER TABLE Staging.Cliente ALTER COLUMN CapoAreaDefault NVARCHAR(60) NOT NULL;
    ALTER TABLE Staging.Cliente ALTER COLUMN AgenteDefault NVARCHAR(60) NOT NULL;
    ALTER TABLE Staging.Cliente ALTER COLUMN HasRoleMySolutionDemo BIT NOT NULL;

    CREATE UNIQUE NONCLUSTERED INDEX IX_Staging_Cliente_BusinessKey ON Staging.Cliente (IDSoggettoCommerciale);
END;
GO

IF OBJECT_ID(N'Staging.ClienteNopCommerceView', N'V') IS NULL EXEC('CREATE VIEW Staging.ClienteNopCommerceView AS SELECT 1 AS fld;');
GO

ALTER VIEW Staging.ClienteNopCommerceView
AS
WITH UtentiConAccessi
AS (
    SELECT DISTINCT Username
    FROM Landing.MYSOLUTION_LogsForReport LFR
),
GruppoAgentiDettaglio
AS (
    SELECT
        GA.GruppoAgenti,
        GA.PKGruppoAgenti,
        ROW_NUMBER() OVER (PARTITION BY GA.GruppoAgenti ORDER BY GA.PKGruppoAgenti DESC) AS rn

    FROM Dim.GruppoAgenti GA
    WHERE GA.GruppoAgenti NOT IN (N'', N'<???>')
),
TableData
AS (
    SELECT
        C.Id AS IDCustomerNopCommerce,

	    CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            C.Id,
		    ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            UCA.Username,
            C.IdCometa,
            MSU.CodiceCliente,
            C.FirstName,
            C.LastName,
            C.Company,
            C.CodiceFiscale,
            C.VATNumber,
            C.StreetAddress,
            C.ZipPostalCode,
            C.City,
            MSU.TipoCliente,
            MSU.PKDataInizioContratto,
            MSU.PKDataFineContratto,
            DD.PKData,
            D.motivo_disdetta,
            C.LastName,
            C.FirstName,
            C.Phone,
            C.Cellulare,
            MSU.Email,
            P.CodSiglaProvincia,
            C.HasRoleMySolutionDemo,
		    ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,

        UCA.Username AS Email,
        C.IdCometa AS IDAnagraficaCometa,
        CASE WHEN C.IdCometa > 0 THEN 1 ELSE 0 END AS HasAnagraficaCometa,
        1 AS HasAnagraficaNopCommerce,
        0 AS HasAnagraficaMySolution,
        N'NOPCOMMERCE' AS ProvenienzaAnagrafica,
        COALESCE(MSU.CodiceCliente, N'') AS CodiceCliente,
        'C' AS TipoSoggettoCommerciale,
        UPPER(CASE
          WHEN C.FirstName + C.LastName = N'' THEN C.Company
          WHEN C.Company = N'' THEN LTRIM(RTRIM(C.FirstName + N' ' + C.LastName))
          ELSE LTRIM(RTRIM(C.FirstName + N' ' + C.LastName)) + N' - ' + C.Company
        END) AS RagioneSociale,
        C.CodiceFiscale,
        C.VATNumber AS PartitaIVA,
        C.StreetAddress AS Indirizzo,
        LEFT(C.ZipPostalCode, 10) AS CAP,
        C.City AS Localita,
        COALESCE(P.DescrProvincia, CASE WHEN COALESCE(P.CodSiglaProvincia, N'') = N'' THEN N'' ELSE N'<???>' END) AS Provincia,
        COALESCE(P.DescrRegione, N'') AS Regione,
        COALESCE(P.DescrMacroregione, N'') AS Macroregione,
        CASE WHEN C.Country = N'Italy' THEN N'Italia' ELSE C.Country END AS Nazione,
        COALESCE(MSU.TipoCliente, N'') AS TipoCliente,
        N'' AS Agente,
        COALESCE(MSU.PKDataInizioContratto, CAST('19000101' AS DATE)) AS PKDataInizioContratto,
        COALESCE(MSU.PKDataFineContratto, CAST('19000101' AS DATE)) AS PKDataFineContratto,
        COALESCE(DD.PKData, CAST('19000101' AS DATE)) AS PKDataDisdetta,
        COALESCE(D.motivo_disdetta, N'') AS MotivoDisdetta,
        -1 AS PKGruppoAgenti,
        C.LastName AS Cognome,
        C.FirstName AS Nome,
        C.Phone AS Telefono,
        C.Cellulare,
        N'' AS Fax,
        CASE WHEN MSU.Email IS NOT NULL THEN 1 ELSE 0 END AS IsAbbonato,
        COALESCE(P.CodSiglaProvincia, N'') AS IDProvincia,
        C.HasRoleMySolutionDemo

    FROM UtentiConAccessi UCA
    LEFT JOIN Staging.SoggettoCommerciale_Email SCE ON SCE.Email = UCA.Username
        AND SCE.rnSoggettoCommercialeDESC = 1
    LEFT JOIN Staging.SoggettoCommerciale SC ON SC.IDSoggettoCommerciale = SCE.IDSoggettoCommerciale
    INNER JOIN Staging.Customer C ON C.Username = UCA.Username
    LEFT JOIN Landing.COMETA_Anagrafica A ON A.id_anagrafica = C.IdCometa
    LEFT JOIN Landing.COMETA_SoggettoCommerciale SCA ON SCA.id_anagrafica = A.id_anagrafica
        AND SCA.rnIDSoggettoCommercialeDESC = 1
    LEFT JOIN Dim.GruppoAgenti GA ON GA.id_gruppo_agenti = SCA.id_gruppo_agenti
    LEFT JOIN Staging.MySolutionUsers MSU ON MSU.Email = SCE.Email
        AND MSU.rnDataInizioContrattoDESC = 1
    LEFT JOIN Landing.MYSOLUTION_StateProvince SP ON SP.Id = C.StateProvinceId
    LEFT JOIN Import.Provincia P ON P.CodSiglaProvincia = SP.Abbreviation
    LEFT JOIN Landing.COMETA_Documento D ON D.id_documento = MSU.IDDocumento
        AND D.id_libero_1 = 9 -- 9: Disdettato
    LEFT JOIN Dim.Data DD ON DD.PKData = D.data_disdetta
    WHERE SC.IDSoggettoCommerciale IS NULL
)
SELECT
    -- Chiavi
    TD.IDCustomerNopCommerce AS PKClienteNopCommerce,

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
    TD.IDAnagraficaCometa,
    TD.HasAnagraficaCometa,
    TD.HasAnagraficaNopCommerce,
    TD.HasAnagraficaMySolution,
    TD.ProvenienzaAnagrafica,
    TD.CodiceCliente,
    TD.TipoSoggettoCommerciale,
    TD.RagioneSociale,
    TD.CodiceFiscale,
    TD.PartitaIVA,
    LEFT(TD.Indirizzo, 120) AS Indirizzo,
    TD.CAP,
    TD.Localita,
    TD.Provincia,
    TD.Regione,
    TD.Macroregione,
    TD.Nazione,
    TD.TipoCliente,
    TD.Agente,
    TD.PKDataInizioContratto,
    TD.PKDataFineContratto,
    TD.PKDataDisdetta,
    TD.MotivoDisdetta,
    TD.PKGruppoAgenti,
    TD.Cognome,
    TD.Nome,
    TD.Telefono,
    TD.Cellulare,
    TD.Fax,
    TD.IsAbbonato,
    -TD.IDCustomerNopCommerce AS IDSoggettoCommerciale_migrazione,
    CAST(NULL AS INT) AS IDSoggettoCommerciale_migrazione_old,
    TD.IDProvincia,
    CAST(N'' AS NVARCHAR(60)) AS CapoAreaDefault,
    CAST(N'' AS NVARCHAR(60)) AS AgenteDefault,
    TD.HasRoleMySolutionDemo

FROM TableData TD;
GO

IF OBJECT_ID(N'Staging.ClienteMySolutionView', N'V') IS NULL EXEC('CREATE VIEW Staging.ClienteMySolutionView AS SELECT 1 AS fld;');
GO

ALTER VIEW Staging.ClienteMySolutionView
AS
WITH UtentiConAccessi
AS (
    SELECT DISTINCT Username
    FROM Landing.MYSOLUTION_LogsForReport LFR
),
GruppoAgentiDettaglio
AS (
    SELECT
        GA.GruppoAgenti,
        GA.PKGruppoAgenti,
        ROW_NUMBER() OVER (PARTITION BY GA.GruppoAgenti ORDER BY GA.PKGruppoAgenti DESC) AS rn

    FROM Dim.GruppoAgenti GA
    WHERE GA.GruppoAgenti NOT IN (N'', N'<???>')
),
NopCustomerDetail
AS (
    SELECT
        NOPC.Email,
        NOPC.HasRoleMySolutionDemo,
        ROW_NUMBER() OVER (PARTITION BY NOPC.Email ORDER BY NOPC.Id DESC) AS rn
    FROM Staging.Customer NOPC
),
TableData
AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY UCA.Username) AS rn,

	    CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            UCA.Username,
		    ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            UCA.Username,
            MSU.CodiceCliente,
            MSU.RagioneSociale,
            MSU.CodiceFiscale,
            MSU.PartitaIVA,
            MSU.Localita,
            MSU.Provincia,
            MSU.TipoCliente,
            MSU.PKDataInizioContratto,
            MSU.PKDataFineContratto,
            MSU.Cognome,
            MSU.Nome,
            MSU.Email,
            NCD.HasRoleMySolutionDemo,
		    ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,

        UCA.Username AS Email,
        0 AS IDAnagraficaCometa,
        0 AS HasAnagraficaCometa,
        0 AS HasAnagraficaNopCommerce,
        1 AS HasAnagraficaMySolution,
        N'MYSOLUTION' AS ProvenienzaAnagrafica,
        COALESCE(MSU.CodiceCliente, N'') AS CodiceCliente,
        'C' AS TipoSoggettoCommerciale,
        COALESCE(MSU.RagioneSociale, N'') AS RagioneSociale,
        COALESCE(MSU.CodiceFiscale, N'') AS CodiceFiscale,
        COALESCE(MSU.PartitaIVA, N'') AS PartitaIVA,
        N'' AS Indirizzo,
        N'' AS CAP,
        COALESCE(MSU.Localita, N'') AS Localita,
        COALESCE(P.DescrProvincia, CASE WHEN COALESCE(MSU.Provincia, N'') = N'' THEN N'' ELSE N'<???>' END) AS Provincia,
        COALESCE(P.DescrRegione, CASE WHEN COALESCE(MSU.Provincia, N'') = N'' THEN N'' ELSE N'<???>' END) AS Regione,
        COALESCE(P.DescrMacroregione, CASE WHEN COALESCE(MSU.Provincia, N'') = N'' THEN N'' ELSE N'<???>' END) AS Macroregione,
        COALESCE(P.DescrNazione, N'') AS Nazione,
        COALESCE(MSU.TipoCliente, N'') AS TipoCliente,
        N'' AS Agente,
        COALESCE(MSU.PKDataInizioContratto, CAST('19000101' AS DATE)) AS PKDataInizioContratto,
        COALESCE(MSU.PKDataFineContratto, CAST('19000101' AS DATE)) AS PKDataFineContratto,
        CAST('19000101' AS DATE) AS PKDataDisdetta,
        N'' AS MotivoDisdetta,
        -1 AS PKGruppoAgenti,
        COALESCE(MSU.Cognome, N'') AS Cognome,
        COALESCE(MSU.Nome, N'') AS Nome,
        N'' AS Telefono,
        N'' AS Cellulare,
        N'' AS Fax,
        CASE WHEN MSU.Email IS NOT NULL THEN 1 ELSE 0 END AS IsAbbonato,
        COALESCE(MSU.Provincia, N'') AS IDProvincia,
        COALESCE(NCD.HasRoleMySolutionDemo, 0) AS HasRoleMySolutionDemo

    FROM UtentiConAccessi UCA
    LEFT JOIN Staging.SoggettoCommerciale_Email SCE ON SCE.Email = UCA.Username
        AND SCE.rnSoggettoCommercialeDESC = 1
    LEFT JOIN Staging.SoggettoCommerciale SC ON SC.IDSoggettoCommerciale = SCE.IDSoggettoCommerciale
    LEFT JOIN Staging.Customer C ON C.Username = UCA.Username
    INNER JOIN Staging.MySolutionUsers MSU ON MSU.Email = UCA.Username
        AND MSU.rnDataInizioContrattoDESC = 1
    LEFT JOIN Import.Provincia P ON P.CodSiglaProvincia = MSU.Provincia
    LEFT JOIN NopCustomerDetail NCD ON NCD.Email = UCA.Username
        AND NCD.rn = 1
    WHERE SC.IDSoggettoCommerciale IS NULL
        AND C.Username IS NULL
)
SELECT
    -- Chiavi
    TD.rn AS PKClienteMySolution,

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
    TD.IDAnagraficaCometa,
    TD.HasAnagraficaCometa,
    TD.HasAnagraficaNopCommerce,
    TD.HasAnagraficaMySolution,
    TD.ProvenienzaAnagrafica,
    TD.CodiceCliente,
    TD.TipoSoggettoCommerciale,
    TD.RagioneSociale,
    TD.CodiceFiscale,
    TD.PartitaIVA,
    TD.Indirizzo,
    TD.CAP,
    TD.Localita,
    TD.Provincia,
    TD.Regione,
    TD.Macroregione,
    TD.Nazione,
    TD.TipoCliente,
    TD.Agente,
    TD.PKDataInizioContratto,
    TD.PKDataFineContratto,
    TD.PKDataDisdetta,
    TD.MotivoDisdetta,
    TD.PKGruppoAgenti,
    TD.Cognome,
    TD.Nome,
    TD.Telefono,
    TD.Cellulare,
    TD.Fax,
    TD.IsAbbonato,
    CAST(NULL AS INT) AS IDSoggettoCommerciale_migrazione,
    CAST(NULL AS INT) AS IDSoggettoCommerciale_migrazione_old,
    TD.IDProvincia,
    CAST(N'' AS NVARCHAR(60)) AS CapoAreaDefault,
    CAST(N'' AS NVARCHAR(60)) AS AgenteDefault,
    TD.HasRoleMySolutionDemo

FROM TableData TD;
GO

IF OBJECT_ID(N'Staging.ClienteAccessiView', N'V') IS NULL EXEC('CREATE VIEW Staging.ClienteAccessiView AS SELECT 1 AS fld;');
GO

ALTER VIEW Staging.ClienteAccessiView
AS
WITH UtentiConAccessi
AS (
    SELECT DISTINCT Username
    FROM Landing.MYSOLUTION_LogsForReport LFR
),
NopCustomerDetail
AS (
    SELECT
        NOPC.Email,
        NOPC.HasRoleMySolutionDemo,
        ROW_NUMBER() OVER (PARTITION BY NOPC.Email ORDER BY NOPC.Id DESC) AS rn
    FROM Staging.Customer NOPC
),
TableData
AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY UCA.Username) AS rn,

	    CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            UCA.Username,
		    ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            UCA.Username,
            MSU.Email,
            NCD.HasRoleMySolutionDemo,
		    ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,

        UCA.Username AS Email,
        0 AS IDAnagraficaCometa,
        0 AS HasAnagraficaCometa,
        0 AS HasAnagraficaNopCommerce,
        0 AS HasAnagraficaMySolution,
        N'ACCESSI' AS ProvenienzaAnagrafica,
        N'' AS CodiceCliente,
        'C' AS TipoSoggettoCommerciale,
        UCA.Username AS RagioneSociale,
        N'' AS CodiceFiscale,
        N'' AS PartitaIVA,
        N'' AS Indirizzo,
        N'' AS CAP,
        N'' AS Localita,
        N'' AS Provincia,
        N'' AS Regione,
        N'' AS Macroregione,
        N'' AS Nazione,
        N'' AS TipoCliente,
        N'' AS Agente,
        CAST('19000101' AS DATE) AS PKDataInizioContratto,
        CAST('19000101' AS DATE) AS PKDataFineContratto,
        CAST('19000101' AS DATE) AS PKDataDisdetta,
        N'' AS MotivoDisdetta,
        -1 AS PKGruppoAgenti,
        N'' AS Cognome,
        N'' AS Nome,
        N'' AS Telefono,
        N'' AS Cellulare,
        N'' AS Fax,
        CASE WHEN MSU.Email IS NOT NULL THEN 1 ELSE 0 END AS IsAbbonato,
        N'' AS IDProvincia,
        COALESCE(NCD.HasRoleMySolutionDemo, 0) AS HasRoleMySolutionDemo

    FROM UtentiConAccessi UCA
    LEFT JOIN Staging.SoggettoCommerciale_Email SCE ON SCE.Email = UCA.Username
        AND SCE.rnSoggettoCommercialeDESC = 1
    LEFT JOIN Staging.SoggettoCommerciale SC ON SC.IDSoggettoCommerciale = SCE.IDSoggettoCommerciale
    LEFT JOIN Staging.Customer C ON C.Username = UCA.Username
    LEFT JOIN Staging.MySolutionUsers MSU ON MSU.Email = UCA.Username
        AND MSU.rnDataInizioContrattoDESC = 1
    LEFT JOIN NopCustomerDetail NCD ON NCD.Email = UCA.Username
        AND NCD.rn = 1
    WHERE SC.IDSoggettoCommerciale IS NULL
        AND C.Username IS NULL
        AND MSU.Email IS NULL
)
SELECT
    -- Chiavi
    TD.rn AS PKClienteAccessi,

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
    TD.IDAnagraficaCometa,
    TD.HasAnagraficaCometa,
    TD.HasAnagraficaNopCommerce,
    TD.HasAnagraficaMySolution,
    TD.ProvenienzaAnagrafica,
    TD.CodiceCliente,
    TD.TipoSoggettoCommerciale,
    TD.RagioneSociale,
    TD.CodiceFiscale,
    TD.PartitaIVA,
    TD.Indirizzo,
    TD.CAP,
    TD.Localita,
    TD.Provincia,
    TD.Regione,
    TD.Macroregione,
    TD.Nazione,
    TD.TipoCliente,
    TD.Agente,
    TD.PKDataInizioContratto,
    TD.PKDataFineContratto,
    TD.PKDataDisdetta,
    TD.MotivoDisdetta,
    TD.PKGruppoAgenti,
    TD.Cognome,
    TD.Nome,
    TD.Telefono,
    TD.Cellulare,
    TD.Fax,
    TD.IsAbbonato,
    CAST(NULL AS INT) AS IDSoggettoCommerciale_migrazione,
    CAST(NULL AS INT) AS IDSoggettoCommerciale_migrazione_old,
    TD.IDProvincia,
    CAST(N'' AS NVARCHAR(60)) AS CapoAreaDefault,
    CAST(N'' AS NVARCHAR(60)) AS AgenteDefault,
    TD.HasRoleMySolutionDemo

FROM TableData TD;
GO

IF OBJECT_ID(N'Staging.usp_Reload_Cliente', N'P') IS NULL EXEC('CREATE PROCEDURE Staging.usp_Reload_Cliente AS RETURN 0;');
GO

ALTER PROCEDURE Staging.usp_Reload_Cliente
AS
BEGIN

    SET NOCOUNT ON;

    DECLARE @lastupdated_staging DATETIME;
    DECLARE @provider_name NVARCHAR(60) = N'MyDatamartReporting';
    DECLARE @full_table_name sysname = N'Landing.COMETA_SoggettoCommerciale';
    DECLARE @minIDSoggettoCommerciale INT = -1000000;

    SELECT TOP 1 @lastupdated_staging = lastupdated_staging
    FROM audit.tables
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    IF (@lastupdated_staging IS NULL) SET @lastupdated_staging = CAST('19000101' AS DATETIME);

    BEGIN TRANSACTION

    TRUNCATE TABLE Staging.Cliente;

    INSERT INTO Staging.Cliente
    SELECT * FROM Staging.ClienteCometaView;
    --WHERE UpdateDatetime > @lastupdated_staging;

    SELECT @minIDSoggettoCommerciale = CASE WHEN MIN(C.IDSoggettoCommerciale) > @minIDSoggettoCommerciale THEN @minIDSoggettoCommerciale ELSE MIN(C.IDSoggettoCommerciale) END
    FROM Staging.Cliente C;

    INSERT INTO Staging.Cliente
    (
        IDSoggettoCommerciale,
        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
        Email,
        IDAnagraficaCometa,
        HasAnagraficaCometa,
        HasAnagraficaNopCommerce,
        HasAnagraficaMySolution,
        ProvenienzaAnagrafica,
        CodiceCliente,
        TipoSoggettoCommerciale,
        RagioneSociale,
        CodiceFiscale,
        PartitaIVA,
        Indirizzo,
        CAP,
        Localita,
        Provincia,
        Regione,
        Macroregione,
        Nazione,
        TipoCliente,
        Agente,
        PKDataInizioContratto,
        PKDataFineContratto,
        PKDataDisdetta,
        MotivoDisdetta,
        PKGruppoAgenti,
        Cognome,
        Nome,
        Telefono,
        Cellulare,
        Fax,
        IsAbbonato,
        IDSoggettoCommerciale_migrazione,
        IDSoggettoCommerciale_migrazione_old,
        IDProvincia,
        CapoAreaDefault,
        AgenteDefault,
        HasRoleMySolutionDemo
    )
    SELECT
        -PKClienteNopCommerce AS IDSoggettoCommerciale,
        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
        Email,
        IDAnagraficaCometa,
        HasAnagraficaCometa,
        HasAnagraficaNopCommerce,
        HasAnagraficaMySolution,
        ProvenienzaAnagrafica,
        CodiceCliente,
        TipoSoggettoCommerciale,
        RagioneSociale,
        CodiceFiscale,
        PartitaIVA,
        Indirizzo,
        CAP,
        Localita,
        Provincia,
        Regione,
        Macroregione,
        Nazione,
        TipoCliente,
        Agente,
        PKDataInizioContratto,
        PKDataFineContratto,
        PKDataDisdetta,
        MotivoDisdetta,
        PKGruppoAgenti,
        Cognome,
        Nome,
        Telefono,
        Cellulare,
        Fax,
        IsAbbonato,
        IDSoggettoCommerciale_migrazione,
        IDSoggettoCommerciale_migrazione_old,
        IDProvincia,
        CapoAreaDefault,
        AgenteDefault,
        HasRoleMySolutionDemo

    FROM Staging.ClienteNopCommerceView;

    SELECT @minIDSoggettoCommerciale = CASE WHEN MIN(C.IDSoggettoCommerciale) > @minIDSoggettoCommerciale THEN @minIDSoggettoCommerciale ELSE MIN(C.IDSoggettoCommerciale) END
    FROM Staging.Cliente C;

    INSERT INTO Staging.Cliente
    (
        IDSoggettoCommerciale,
        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
        Email,
        IDAnagraficaCometa,
        HasAnagraficaCometa,
        HasAnagraficaNopCommerce,
        HasAnagraficaMySolution,
        ProvenienzaAnagrafica,
        CodiceCliente,
        TipoSoggettoCommerciale,
        RagioneSociale,
        CodiceFiscale,
        PartitaIVA,
        Indirizzo,
        CAP,
        Localita,
        Provincia,
        Regione,
        Macroregione,
        Nazione,
        TipoCliente,
        Agente,
        PKDataInizioContratto,
        PKDataFineContratto,
        PKDataDisdetta,
        MotivoDisdetta,
        PKGruppoAgenti,
        Cognome,
        Nome,
        Telefono,
        Cellulare,
        Fax,
        IsAbbonato,
        IDSoggettoCommerciale_migrazione,
        IDSoggettoCommerciale_migrazione_old,
        IDProvincia,
        CapoAreaDefault,
        AgenteDefault,
        HasRoleMySolutionDemo
    )
    SELECT
        @minIDSoggettoCommerciale - PKClienteMySolution AS IDSoggettoCommerciale,
        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
        Email,
        IDAnagraficaCometa,
        HasAnagraficaCometa,
        HasAnagraficaNopCommerce,
        HasAnagraficaMySolution,
        ProvenienzaAnagrafica,
        CodiceCliente,
        TipoSoggettoCommerciale,
        RagioneSociale,
        CodiceFiscale,
        PartitaIVA,
        Indirizzo,
        CAP,
        Localita,
        Provincia,
        Regione,
        Macroregione,
        Nazione,
        TipoCliente,
        Agente,
        PKDataInizioContratto,
        PKDataFineContratto,
        PKDataDisdetta,
        MotivoDisdetta,
        PKGruppoAgenti,
        Cognome,
        Nome,
        Telefono,
        Cellulare,
        Fax,
        IsAbbonato,
        IDSoggettoCommerciale_migrazione,
        IDSoggettoCommerciale_migrazione_old,
        IDProvincia,
        CapoAreaDefault,
        AgenteDefault,
        HasRoleMySolutionDemo

    FROM Staging.ClienteMySolutionView;

    SELECT @minIDSoggettoCommerciale = CASE WHEN MIN(C.IDSoggettoCommerciale) > @minIDSoggettoCommerciale THEN @minIDSoggettoCommerciale ELSE MIN(C.IDSoggettoCommerciale) END
    FROM Staging.Cliente C;

    INSERT INTO Staging.Cliente
    (
        IDSoggettoCommerciale,
        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
        Email,
        IDAnagraficaCometa,
        HasAnagraficaCometa,
        HasAnagraficaNopCommerce,
        HasAnagraficaMySolution,
        ProvenienzaAnagrafica,
        CodiceCliente,
        TipoSoggettoCommerciale,
        RagioneSociale,
        CodiceFiscale,
        PartitaIVA,
        Indirizzo,
        CAP,
        Localita,
        Provincia,
        Regione,
        Macroregione,
        Nazione,
        TipoCliente,
        Agente,
        PKDataInizioContratto,
        PKDataFineContratto,
        PKDataDisdetta,
        MotivoDisdetta,
        PKGruppoAgenti,
        Cognome,
        Nome,
        Telefono,
        Cellulare,
        Fax,
        IsAbbonato,
        IDSoggettoCommerciale_migrazione,
        IDSoggettoCommerciale_migrazione_old,
        IDProvincia,
        CapoAreaDefault,
        AgenteDefault,
        HasRoleMySolutionDemo
    )
    SELECT
        @minIDSoggettoCommerciale - PKClienteAccessi AS IDSoggettoCommerciale,
        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
        Email,
        IDAnagraficaCometa,
        HasAnagraficaCometa,
        HasAnagraficaNopCommerce,
        HasAnagraficaMySolution,
        ProvenienzaAnagrafica,
        CodiceCliente,
        TipoSoggettoCommerciale,
        RagioneSociale,
        CodiceFiscale,
        PartitaIVA,
        Indirizzo,
        CAP,
        Localita,
        Provincia,
        Regione,
        Macroregione,
        Nazione,
        TipoCliente,
        Agente,
        PKDataInizioContratto,
        PKDataFineContratto,
        PKDataDisdetta,
        MotivoDisdetta,
        PKGruppoAgenti,
        Cognome,
        Nome,
        Telefono,
        Cellulare,
        Fax,
        IsAbbonato,
        IDSoggettoCommerciale_migrazione,
        IDSoggettoCommerciale_migrazione_old,
        IDProvincia,
        CapoAreaDefault,
        AgenteDefault,
        HasRoleMySolutionDemo

    FROM Staging.ClienteAccessiView;

    UPDATE Staging.Cliente
    SET RagioneSociale = Email
    WHERE RagioneSociale = N'';

    SELECT @lastupdated_staging = MAX(UpdateDatetime) FROM Staging.Cliente;

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

EXEC Staging.usp_Reload_Cliente;
GO

--DROP TABLE IF EXISTS Fact.Documenti; DROP TABLE IF EXISTS Fact.Accessi; DROP TABLE IF EXISTS Dim.Cliente; DROP SEQUENCE IF EXISTS dbo.seq_Dim_Cliente;
GO

IF OBJECT_ID('dbo.seq_Dim_Cliente', 'SO') IS NULL
BEGIN

    CREATE SEQUENCE dbo.seq_Dim_Cliente START WITH 1;

END;
GO

IF OBJECT_ID('Dim.Cliente', 'U') IS NULL
BEGIN

    CREATE TABLE Dim.Cliente (
        PKCliente INT NOT NULL CONSTRAINT PK_Dim_Cliente PRIMARY KEY CLUSTERED CONSTRAINT DFT_Dim_Cliente_PKCliente DEFAULT (NEXT VALUE FOR dbo.seq_Dim_Cliente),
        IDSoggettoCommerciale INT NOT NULL,

	    HistoricalHashKey VARBINARY(20) NULL,
	    ChangeHashKey VARBINARY(20) NULL,
	    HistoricalHashKeyASCII VARCHAR(34) NULL,
	    ChangeHashKeyASCII VARCHAR(34) NULL,
	    InsertDatetime DATETIME NOT NULL,
	    UpdateDatetime DATETIME NOT NULL,
	    IsDeleted BIT NOT NULL,

        Email NVARCHAR(120) NOT NULL,
        IDAnagraficaCometa INT NOT NULL,
        HasAnagraficaCometa BIT NOT NULL,
        HasAnagraficaNopCommerce BIT NOT NULL,
        HasAnagraficaMySolution BIT NOT NULL,
        ProvenienzaAnagrafica NVARCHAR(20) NOT NULL,
        CodiceCliente NVARCHAR(10) NOT NULL,
        TipoSoggettoCommerciale NVARCHAR(10) NOT NULL,
        RagioneSociale NVARCHAR(120) NOT NULL,
        CodiceFiscale NVARCHAR(20) NOT NULL,
        PartitaIVA NVARCHAR(20) NOT NULL,
        Indirizzo NVARCHAR(120) NOT NULL,
        CAP NVARCHAR(10) NOT NULL,
        Localita NVARCHAR(60) NOT NULL,
        Provincia NVARCHAR(50) NOT NULL,
        Regione NVARCHAR(60) NOT NULL,
        Macroregione NVARCHAR(60) NOT NULL,
        Nazione NVARCHAR(60) NOT NULL,
        Telefono NVARCHAR(60) NOT NULL,
        Cellulare NVARCHAR(60) NOT NULL,
        Fax NVARCHAR(60) NOT NULL,
        TipoCliente NVARCHAR(10) NOT NULL,
        Agente NVARCHAR(60) NOT NULL,
        PKDataInizioContratto DATE NOT NULL CONSTRAINT FK_Dim_Cliente_PKDataInizioContratto REFERENCES Dim.Data (PKData),
        PKDataFineContratto DATE NOT NULL CONSTRAINT FK_Dim_Cliente_PKDataFineContratto REFERENCES Dim.Data (PKData),
        PKDataDisdetta DATE NOT NULL CONSTRAINT FK_Dim_Cliente_PKDataDisdetta REFERENCES Dim.Data (PKData),
        MotivoDisdetta NVARCHAR(120) NOT NULL,
        PKGruppoAgenti INT NOT NULL CONSTRAINT FK_Dim_Cliente_PKGruppoAgenti REFERENCES Dim.GruppoAgenti (PKGruppoAgenti),
        Cognome NVARCHAR(60) NOT NULL,
        Nome NVARCHAR(60) NOT NULL,
        IsAttivo BIT NOT NULL,
        IsAbbonato BIT NOT NULL,
        IDSoggettoCommerciale_migrazione INT NULL,
        IDSoggettoCommerciale_migrazione_old INT NULL,
        IDProvincia NVARCHAR(10) NOT NULL,
        IsClienteFormazione BIT NOT NULL,
        CapoAreaDefault NVARCHAR(60) NOT NULL,
        AgenteDefault NVARCHAR(60) NOT NULL,
        HasRoleMySolutionDemo BIT NOT NULL
    );

    CREATE UNIQUE NONCLUSTERED INDEX IX_Dim_Cliente_IDSoggettoCommerciale ON Dim.Cliente (IDSoggettoCommerciale);
    --TODO: ripristinare CREATE UNIQUE NONCLUSTERED INDEX IX_Dim_Cliente_Email ON Dim.Cliente (Email) WHERE Email <> N'';
    
    ALTER TABLE Dim.Cliente ADD CONSTRAINT DFT_Dim_Cliente_InsertDatetime DEFAULT (CURRENT_TIMESTAMP) FOR InsertDatetime;
    ALTER TABLE Dim.Cliente ADD CONSTRAINT DFT_Dim_Cliente_UpdateDatetime DEFAULT (CURRENT_TIMESTAMP) FOR UpdateDatetime;
    ALTER TABLE Dim.Cliente ADD CONSTRAINT DFT_Dim_Cliente_IsDeleted DEFAULT (0) FOR IsDeleted;

    ----ALTER TABLE Dim.Cliente ADD CONSTRAINT DFT_Dim_Cliente_IsApproved DEFAULT (0) FOR IsApproved;
    ----ALTER TABLE Dim.Cliente ADD CONSTRAINT DFT_Dim_Cliente_IsLockedOut DEFAULT (0) FOR IsLockedOut;
    ----ALTER TABLE Dim.Cliente ADD CONSTRAINT DFT_Dim_Cliente_PKCreazione DEFAULT (CAST('19000101' AS DATE)) FOR PKDataCreazione;
    ----ALTER TABLE Dim.Cliente ADD CONSTRAINT DFT_Dim_Cliente_PKUltimoLogin DEFAULT (CAST('19000101' AS DATE)) FOR PKDataUltimoLogin;
    ALTER TABLE Dim.Cliente ADD CONSTRAINT DFT_Dim_Cliente_IDAnagraficaCometa DEFAULT (-1) FOR IDAnagraficaCometa;
    ALTER TABLE Dim.Cliente ADD CONSTRAINT DFT_Dim_Cliente_HasAnagraficaCometa DEFAULT (0) FOR HasAnagraficaCometa;
    ALTER TABLE Dim.Cliente ADD CONSTRAINT DFT_Dim_Cliente_HasAnagraficaNopCommerce DEFAULT (0) FOR HasAnagraficaNopCommerce;
    ALTER TABLE Dim.Cliente ADD CONSTRAINT DFT_Dim_Cliente_HasAnagraficaMySolution DEFAULT (0) FOR HasAnagraficaMySolution;
    ALTER TABLE Dim.Cliente ADD CONSTRAINT DFT_Dim_Cliente_ProvenienzaAnagrafica DEFAULT (N'') FOR ProvenienzaAnagrafica;
    ALTER TABLE Dim.Cliente ADD CONSTRAINT DFT_Dim_Cliente_TipoSoggettoCommerciale DEFAULT (N'') FOR TipoSoggettoCommerciale;
    ALTER TABLE Dim.Cliente ADD CONSTRAINT DFT_Dim_Cliente_PKInizioContratto DEFAULT (CAST('19000101' AS DATE)) FOR PKDataInizioContratto;
    ALTER TABLE Dim.Cliente ADD CONSTRAINT DFT_Dim_Cliente_PKFineContratto DEFAULT (CAST('19000101' AS DATE)) FOR PKDataFineContratto;
    ALTER TABLE Dim.Cliente ADD CONSTRAINT DFT_Dim_Cliente_PKDataDisdetta DEFAULT (CAST('19000101' AS DATE)) FOR PKDataDisdetta;
    ALTER TABLE Dim.Cliente ADD CONSTRAINT DFT_Dim_Cliente_PKGruppoAgenti DEFAULT (-1) FOR PKGruppoAgenti;
    ALTER TABLE Dim.Cliente ADD CONSTRAINT DFT_Dim_Cliente_IsAttivo DEFAULT (0) FOR IsAttivo;
    ALTER TABLE Dim.Cliente ADD CONSTRAINT DFT_Dim_Cliente_IsAbbonato DEFAULT (0) FOR IsAbbonato;
    ALTER TABLE Dim.Cliente ADD CONSTRAINT DFT_Dim_Cliente_IsClienteFormazione DEFAULT (0) FOR IsClienteFormazione;
    ALTER TABLE Dim.Cliente ADD CONSTRAINT DFT_Dim_Cliente_CapoAreaDefault DEFAULT (N'') FOR CapoAreaDefault;
    ALTER TABLE Dim.Cliente ADD CONSTRAINT DFT_Dim_Cliente_AgenteDefault DEFAULT (N'') FOR AgenteDefault;
    ALTER TABLE Dim.Cliente ADD CONSTRAINT DFT_Dim_Cliente_HasRoleMySolutionDemo DEFAULT (0) FOR HasRoleMySolutionDemo;

    INSERT INTO Dim.Cliente (
        PKCliente,
        IDSoggettoCommerciale,
        Email,
        CodiceCliente,
        RagioneSociale,
        CodiceFiscale,
        PartitaIVA,
        Indirizzo,
        CAP,
        Localita,
        Provincia,
        Regione,
        Macroregione,
        Nazione,
        Telefono,
        Cellulare,
        Fax,
        TipoCliente,
        Agente,
        MotivoDisdetta,
        Cognome,
        Nome
    )
    VALUES
    (   -1,         -- PKCliente - int
        -1,         -- IDSoggettoCommerciale - int
        N'',       -- Email - nvarchar(60)
        N'',       -- CodiceCliente - nvarchar(10)
        N'',       -- RagioneSociale - nvarchar(120)
        N'',       -- CodiceFiscale - nvarchar(20)
        N'',       -- PartitaIVA - nvarchar(20)
        N'',       -- Indirizzo - nvarchar(120)
        N'',       -- CAP - nvarchar(10)
        N'',       -- Localita - nvarchar(60)
        N'',       -- Provincia - nvarchar(10)
        N'',       -- Regione - nvarchar(60)
        N'',       -- Macroregione - nvarchar(60)
        N'',       -- Nazione - nvarchar(60)
        N'',       -- Telefono - nvarchar(60)
        N'',       -- Cellulare - nvarchar(60)
        N'',       -- Fax - nvarchar(60)
        N'',       -- TipoCliente - nvarchar(10)
        N'',       -- Agente - nvarchar(60)
        N'',       -- MotivoDisdetta - nvarchar(120)
        N'',       -- Cognome - nvarchar(60)
        N''        -- Nome - nvarchar(60)
    ),
    (   -101,         -- PKCliente - int
        -101,         -- IDSoggettoCommerciale - int
        N'',       -- Email - nvarchar(60)
        N'???',       -- CodiceCliente - nvarchar(10)
        N'<???>',       -- RagioneSociale - nvarchar(120)
        N'',       -- CodiceFiscale - nvarchar(20)
        N'',       -- PartitaIVA - nvarchar(20)
        N'',       -- Indirizzo - nvarchar(120)
        N'',       -- CAP - nvarchar(10)
        N'',       -- Localita - nvarchar(60)
        N'',       -- Provincia - nvarchar(10)
        N'',       -- Regione - nvarchar(60)
        N'',       -- Macroregione - nvarchar(60)
        N'',       -- Nazione - nvarchar(60)
        N'',       -- Telefono - nvarchar(60)
        N'',       -- Cellulare - nvarchar(60)
        N'',       -- Fax - nvarchar(60)
        N'',       -- TipoCliente - nvarchar(10)
        N'',       -- Agente - nvarchar(60)
        N'',       -- MotivoDisdetta - nvarchar(120)
        N'',       -- Cognome - nvarchar(60)
        N''        -- Nome - nvarchar(60)
    );

    ALTER SEQUENCE dbo.seq_Dim_Cliente RESTART WITH 1;

END;
GO

IF OBJECT_ID(N'Dim.usp_Merge_Cliente', N'P') IS NULL EXEC('CREATE PROCEDURE Dim.usp_Merge_Cliente AS RETURN 0;');
GO

ALTER PROCEDURE Dim.usp_Merge_Cliente
AS
BEGIN

    SET NOCOUNT ON;

    BEGIN TRANSACTION 

    DECLARE @provider_name NVARCHAR(60) = N'MyDatamartReporting';
    DECLARE @full_table_name sysname = N'Landing.COMETA_SoggettoCommerciale';

    MERGE INTO Dim.Cliente AS TGT
    USING Staging.Cliente (nolock) AS SRC
    ON SRC.IDSoggettoCommerciale = TGT.IDSoggettoCommerciale

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.IsDeleted = SRC.IsDeleted,
        
        TGT.Email = SRC.Email,
        TGT.IDAnagraficaCometa = SRC.IDAnagraficaCometa,
        TGT.HasAnagraficaCometa = TGT.HasAnagraficaCometa,
        TGT.HasAnagraficaNopCommerce = TGT.HasAnagraficaNopCommerce,
        TGT.HasAnagraficaMySolution = TGT.HasAnagraficaMySolution,
        TGT.ProvenienzaAnagrafica = TGT.ProvenienzaAnagrafica,
        TGT.CodiceCliente = SRC.CodiceCliente,
        TGT.TipoSoggettoCommerciale = SRC.TipoSoggettoCommerciale,
        TGT.RagioneSociale = SRC.RagioneSociale,
        TGT.CodiceFiscale = SRC.CodiceFiscale,
        TGT.PartitaIVA = SRC.PartitaIVA,
        TGT.Indirizzo = SRC.Indirizzo,
        TGT.CAP = SRC.CAP,
        TGT.Localita = SRC.Localita,
        TGT.Provincia = SRC.Provincia,
        TGT.Regione = SRC.Regione,
        TGT.Macroregione = SRC.Macroregione,
        TGT.Nazione = SRC.Nazione,
        TGT.Telefono = SRC.Telefono,
        TGT.Cellulare = SRC.Cellulare,
        TGT.Fax = SRC.Fax,
        TGT.TipoCliente = SRC.TipoCliente,
        TGT.Agente = SRC.Agente,
        TGT.PKDataInizioContratto = SRC.PKDataInizioContratto,
        TGT.PKDataFineContratto = SRC.PKDataFineContratto,
        TGT.PKDataDisdetta = SRC.PKDataDisdetta,
        TGT.MotivoDisdetta = SRC.MotivoDisdetta,
        TGT.PKGruppoAgenti = SRC.PKGruppoAgenti,
        TGT.Cognome = SRC.Cognome,
        TGT.Nome = SRC.Nome,
        TGT.IsAttivo = CAST(0 AS BIT),
        TGT.IsAbbonato = SRC.IsAbbonato,
        TGT.IDSoggettoCommerciale_migrazione = SRC.IDSoggettoCommerciale_migrazione,
        TGT.IDSoggettoCommerciale_migrazione_old = SRC.IDSoggettoCommerciale_migrazione_old,
        TGT.IDProvincia = SRC.IDProvincia,
        TGT.CapoAreaDefault = SRC.CapoAreaDefault,
        TGT.AgenteDefault = SRC.AgenteDefault,
        TGT.HasRoleMySolutionDemo = SRC.HasRoleMySolutionDemo

    WHEN NOT MATCHED
      THEN INSERT (
        IDSoggettoCommerciale,
        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
        Email,
        IDAnagraficaCometa,
        HasAnagraficaCometa,
        HasAnagraficaNopCommerce,
        HasAnagraficaMySolution,
        ProvenienzaAnagrafica,
        CodiceCliente,
        TipoSoggettoCommerciale,
        RagioneSociale,
        CodiceFiscale,
        PartitaIVA,
        Indirizzo,
        CAP,
        Localita,
        Provincia,
        Regione,
        Macroregione,
        Nazione,
        Telefono,
        Cellulare,
        Fax,
        TipoCliente,
        Agente,
        PKDataInizioContratto,
        PKDataFineContratto,
        PKDataDisdetta,
        MotivoDisdetta,
        PKGruppoAgenti,
        Cognome,
        Nome,
        IsAbbonato,
        IDSoggettoCommerciale_migrazione,
        IDSoggettoCommerciale_migrazione_old,
        IDProvincia,
        CapoAreaDefault,
        AgenteDefault,
        HasRoleMySolutionDemo
      )
      VALUES (
        SRC.IDSoggettoCommerciale,
        SRC.HistoricalHashKey,
        SRC.ChangeHashKey,
        SRC.HistoricalHashKeyASCII,
        SRC.ChangeHashKeyASCII,
        SRC.InsertDatetime,
        SRC.UpdateDatetime,
        SRC.IsDeleted,
        SRC.Email,
        SRC.IDAnagraficaCometa,
        SRC.HasAnagraficaCometa,
        SRC.HasAnagraficaNopCommerce,
        SRC.HasAnagraficaMySolution,
        SRC.ProvenienzaAnagrafica,
        SRC.CodiceCliente,
        SRC.TipoSoggettoCommerciale,
        SRC.RagioneSociale,
        SRC.CodiceFiscale,
        SRC.PartitaIVA,
        SRC.Indirizzo,
        SRC.CAP,
        SRC.Localita,
        SRC.Provincia,
        SRC.Regione,
        SRC.Macroregione,
        SRC.Nazione,
        SRC.Telefono,
        SRC.Cellulare,
        SRC.Fax,
        SRC.TipoCliente,
        SRC.Agente,
        SRC.PKDataInizioContratto,
        SRC.PKDataFineContratto,
        SRC.PKDataDisdetta,
        SRC.MotivoDisdetta,
        SRC.PKGruppoAgenti,
        SRC.Cognome,
        SRC.Nome,
        SRC.IsAbbonato,
        SRC.IDSoggettoCommerciale_migrazione,
        SRC.IDSoggettoCommerciale_migrazione_old,
        SRC.IDProvincia,
        SRC.CapoAreaDefault,
        SRC.AgenteDefault,
        SRC.HasRoleMySolutionDemo
      )

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        $action AS merge_action,
        'Staging.Cliente' AS full_olap_table_name,
        'IDSoggettoCommerciale = ' + CAST(COALESCE(inserted.IDSoggettoCommerciale, deleted.IDSoggettoCommerciale) AS NVARCHAR(1000)) AS primary_key_description
    INTO audit.merge_log_details;

    --DELETE FROM Dim.Cliente
    --WHERE IsDeleted = CAST(1 AS BIT);

    -- Ricalcolo flag IsAttivo
    IF OBJECT_ID('Fact.Accessi', 'U') IS NOT NULL
    BEGIN

        UPDATE C
        SET C.IsAttivo = CAST(1 AS BIT)
        FROM Dim.Cliente C
        WHERE CURRENT_TIMESTAMP BETWEEN C.PKDataInizioContratto AND C.PKDataFineContratto
            OR EXISTS (SELECT TOP (1) A.PKCliente FROM Fact.Accessi A WHERE A.PKCliente = C.PKCliente AND A.PKData >= DATEADD(MONTH, -1, CAST(CURRENT_TIMESTAMP AS DATE)))

    END;

    -- Verifica migrazioni da NOPCOMMERCE a COMETA
    UPDATE T
    SET T.IsDeleted = CAST(1 AS BIT)

    FROM Dim.Cliente T
    INNER JOIN Staging.Cliente SC ON SC.Email = T.Email
        AND SC.ProvenienzaAnagrafica IN (N'COMETA')
    WHERE T.ProvenienzaAnagrafica IN (N'NOPCOMMERCE');

    UPDATE CNew
    SET CNew.IDSoggettoCommerciale_migrazione_old = COld.IDSoggettoCommerciale

    FROM Dim.Cliente CNew
    INNER JOIN Staging.Cliente SC ON SC.Email = CNew.Email
        AND SC.ProvenienzaAnagrafica IN (N'COMETA')
    INNER JOIN Dim.Cliente COld ON COld.Email = CNew.Email
        AND COld.ProvenienzaAnagrafica IN (N'NOPCOMMERCE')
    WHERE CNew.ProvenienzaAnagrafica IN (N'COMETA');

    -- Aggiornamento flag IsClienteFormazione
    WITH ClientiFormazione
    AS (
        SELECT DISTINCT PKCliente
        FROM Fact.Documenti
        WHERE IsProfiloValidoPerStatisticaFatturatoFormazione = CAST(1 AS BIT)
            AND IsDeleted = CAST(0 AS BIT)
    )
    UPDATE C
    SET C.IsClienteFormazione = CASE WHEN CF.PKCliente IS NOT NULL THEN 1 ELSE 0 END
    FROM Dim.Cliente C
    LEFT JOIN ClientiFormazione CF ON CF.PKCliente = C.PKCliente;

    -- Aggiornamento CapoAreaDefault
    WITH CapoAreaDefaultByCAP
    AS (
        SELECT
            CCA.IDProvincia,
            CCA.CAP,
            MAX(CCA.CapoArea) AS CapoAreaDefault,
            MAX(CCA.Agente) AS AgenteDefault

        FROM Import.ComuneCAPAgente CCA
        GROUP BY CCA.IDProvincia,
            CCA.CAP
        HAVING COUNT(DISTINCT CCA.CapoArea) = 1
    ),
    CapoAreaDefaultByLocalita
    AS (
        SELECT
            CCA.IDProvincia,
            CCA.Comune AS Localita,
            MAX(CCA.CapoArea) AS CapoAreaDefault,
            MAX(CCA.Agente) AS AgenteDefault

        FROM Import.ComuneCAPAgente CCA
        GROUP BY CCA.IDProvincia,
            CCA.Comune
        HAVING COUNT(1) = 1
    )
    UPDATE C
    SET C.CapoAreaDefault = COALESCE(CADBL.CapoAreaDefault, CADBCAP.CapoAreaDefault, PA.CapoArea, N''),
        C.AgenteDefault = COALESCE(CADBL.AgenteDefault, CADBCAP.AgenteDefault, PA.Agente, N'')

    FROM Dim.Cliente C
    LEFT JOIN Import.ProvinciaAgente PA ON PA.IDProvincia = C.IDProvincia
    LEFT JOIN CapoAreaDefaultByCAP CADBCAP ON CADBCAP.IDProvincia = C.IDProvincia AND CADBCAP.CAP = C.CAP
    LEFT JOIN CapoAreaDefaultByLocalita CADBL ON CADBL.IDProvincia = C.IDProvincia AND CADBL.Localita = C.Localita;

    -- Aggiornamento PKDataDisdetta
    WITH DocumentiMySolution
    AS (
        SELECT DISTINCT
            D.IDDocumento,
            D.PKCliente,
            D.PKDataFineContratto,
            D.PKDataDisdetta

        FROM Fact.Documenti D
        WHERE D.IDProfilo = N'ORDCLI'
    ),
    DocumentiMySolutionNumerati
    AS (
        SELECT
            DMS.IDDocumento,
            DMS.PKCliente,
            DMS.PKDataFineContratto,
            DMS.PKDataDisdetta,
            ROW_NUMBER() OVER (PARTITION BY DMS.PKCliente ORDER BY DMS.PKDataFineContratto DESC) AS rn

        FROM DocumentiMySolution DMS
    )
    UPDATE C
    SET C.PKDataDisdetta = DMSN.PKDataDisdetta
    FROM Dim.Cliente C
    INNER JOIN DocumentiMySolutionNumerati DMSN ON DMSN.PKCliente = C.PKCliente
        AND DMSN.rn = 1;

    UPDATE audit.tables
    SET lastupdated_local = lastupdated_staging
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    COMMIT TRANSACTION;

END;
GO

EXEC Dim.usp_Merge_Cliente;
GO
