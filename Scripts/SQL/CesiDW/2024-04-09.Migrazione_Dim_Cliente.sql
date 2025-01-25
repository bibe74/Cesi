/*
SET NOEXEC OFF;
--*/ SET NOEXEC ON;
GO

/* Restore database from backup - 11'30"

USE master;
GO

RESTORE DATABASE CesiDW_dev
FROM DISK = 'C:\temp\EPISQL2021\CesiDW\FULL\EPISQL2021_CesiDW_FULL_20240414_080002.bak'
WITH REPLACE, RECOVERY;
GO

*/

USE CesiDW_dev;
GO

DROP TABLE IF EXISTS Dim.ClienteNEW;
DROP TABLE IF EXISTS Staging.CometaCustomer;
DROP TABLE IF EXISTS Staging.CometaVendor;
DROP TABLE IF EXISTS Staging.MySolutionCustomer;
DROP PROCEDURE IF EXISTS Dim.usp_Merge_ClienteNEW;
DROP PROCEDURE IF EXISTS Fact.usp_ReportFatturatoFormazioneDettaglioNEW;
DROP PROCEDURE IF EXISTS Fact.usp_ReportFatturatoFormazioneDettaglioCorsiNEW;
GO

DELETE FROM Fact.Scadenze;
DELETE FROM Fact.Documenti;
DELETE FROM Fact.Accessi;
DELETE FROM Fact.Corsi;
GO

DELETE FROM Dim.Utente;
DELETE FROM Dim.Cliente;
GO

ALTER TABLE Dim.Cliente DROP CONSTRAINT DFT_Dim_Cliente_PKInizioContratto;
ALTER TABLE Dim.Cliente ADD CONSTRAINT DFT_Dim_Cliente_PKDataInizioContratto DEFAULT (CAST('19000101' AS DATE)) FOR PKDataInizioContratto;
ALTER TABLE Dim.Cliente DROP CONSTRAINT DFT_Dim_Cliente_PKFineContratto;
ALTER TABLE Dim.Cliente ADD CONSTRAINT DFT_Dim_Cliente_PKDataFineContratto DEFAULT (CAST('19000101' AS DATE)) FOR PKDataFineContratto;
ALTER TABLE Dim.Cliente ADD CONSTRAINT DFT_Dim_Cliente_AgenteDefault DEFAULT (N'') FOR AgenteDefault;
GO

/**
 * @table Staging.CometaCustomer
 * @description 

 * @depends Landing.COMETA_SoggettoCommerciale
 * @depends Landing.COMETA_Anagrafica
 * @depends Landing.COMETA_Telefono

SELECT TOP (1) * FROM Landing.COMETA_SoggettoCommerciale;
SELECT TOP (1) * FROM Landing.COMETA_Anagrafica;
SELECT TOP (1) * FROM Landing.COMETA_Telefono;
*/

CREATE OR ALTER VIEW Staging.CometaCustomerView
AS
WITH AbbonamentiAttivi
AS (
    SELECT DISTINCT
        D.id_sog_commerciale,
        D.id_documento,
        D.num_documento,
        D.data_documento,
        D.data_inizio_contratto,
        DATEADD(DAY, 1, D.data_fine_contratto) AS data_fine_contratto

    FROM Landing.COMETA_Documento D
    INNER JOIN Landing.COMETA_SoggettoCommerciale SC ON SC.id_sog_commerciale = D.id_sog_commerciale
        AND SC.tipo = 'C'
        AND SC.IsDeleted = CAST(0 AS BIT)
    INNER JOIN Landing.COMETA_Documento_Riga DR ON DR.id_documento = D.id_documento
        AND DR.IsDeleted = CAST(0 AS BIT)
    INNER JOIN Landing.COMETA_Articolo A ON A.id_articolo = DR.id_articolo
        AND A.IsDeleted = CAST(0 AS BIT)
    INNER JOIN Landing.COMETA_MySolutionTrascodifica MST ON MST.codice = A.codice
        AND MST.IsDeleted = CAST(0 AS BIT)
    WHERE D.id_prof_documento IN (1, 43) -- 1: ORDINE CLIENTE, 43: ORDINE CLIENTE
        AND CONVERT(DATE, CURRENT_TIMESTAMP) BETWEEN D.data_inizio_contratto AND DATEADD(DAY, 1, D.data_fine_contratto)
        AND D.IsDeleted = CAST(0 AS BIT)
),
AbbonamentiDettaglio
AS (
    SELECT
        AA.id_sog_commerciale,
        AA.id_documento,
        AA.num_documento,
        AA.data_documento,
        AA.data_inizio_contratto,
        AA.data_fine_contratto,
        CAST(1 AS BIT) AS HasSconto,
        ROW_NUMBER() OVER (PARTITION BY AA.id_sog_commerciale ORDER BY AA.data_fine_contratto DESC, AA.data_inizio_contratto DESC) AS rnDESC

    FROM AbbonamentiAttivi AA
),
SoggettoCommercialeMailDettaglio
AS (
    SELECT
        SC.id_sog_commerciale,
        LTRIM(RTRIM(E.num_riferimento)) AS Email,
        E.nome,
        E.cognome,
        COALESCE(E.ruolo, 1) as Quote,
        COALESCE(E.descrizione, N'') as telefono_descrizione,
        E.id_telefono,
        ROW_NUMBER() OVER (PARTITION BY SC.id_sog_commerciale ORDER BY E.id_telefono) AS rn

    FROM Landing.COMETA_SoggettoCommerciale SC
    INNER JOIN Landing.COMETA_Anagrafica A ON A.id_anagrafica = SC.id_anagrafica
        AND A.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Landing.COMETA_Telefono E ON E.id_anagrafica = A.id_anagrafica
        AND E.tipo = 'E'
        AND E.descrizione = N'ABBONATO'
        AND E.num_riferimento LIKE N'%@%'
        AND E.IsDeleted = CAST(0 AS BIT)
    WHERE SC.tipo = 'C'
        AND SC.IsDeleted = CAST(0 AS BIT)
),
Disdette
AS (
    SELECT DISTINCT
        D.id_sog_commerciale,
        D.id_documento,
        D.num_documento,
        D.data_documento,
        D.data_inizio_contratto,
        DATEADD(DAY, 1, D.data_fine_contratto) AS data_fine_contratto,
        D.data_disdetta,
        D.motivo_disdetta

    FROM Landing.COMETA_Documento D
    INNER JOIN Landing.COMETA_SoggettoCommerciale SC ON SC.id_sog_commerciale = D.id_sog_commerciale
        AND SC.tipo = 'C'
        AND SC.IsDeleted = CAST(0 AS BIT)
    INNER JOIN Landing.COMETA_Documento_Riga DR ON DR.id_documento = D.id_documento
        AND DR.IsDeleted = CAST(0 AS BIT)
    INNER JOIN Landing.COMETA_Articolo A ON A.id_articolo = DR.id_articolo
        AND A.IsDeleted = CAST(0 AS BIT)
    INNER JOIN Landing.COMETA_MySolutionTrascodifica MST ON MST.codice = A.codice
        AND MST.IsDeleted = CAST(0 AS BIT)
    WHERE D.id_prof_documento IN (1, 43) -- 1: ORDINE CLIENTE, 43: ORDINE CLIENTE
        --AND CONVERT(DATE, CURRENT_TIMESTAMP) BETWEEN D.data_inizio_contratto AND DATEADD(DAY, 1, D.data_fine_contratto)
        AND D.IsDeleted = CAST(0 AS BIT)
        AND D.data_disdetta IS NOT NULL
),
DisdetteDettaglio
AS (
    SELECT
        D.id_sog_commerciale,
        D.id_documento,
        D.num_documento,
        D.data_documento,
        D.data_inizio_contratto,
        D.data_fine_contratto,
        D.data_disdetta,
        D.motivo_disdetta,
        ROW_NUMBER() OVER (PARTITION BY D.id_sog_commerciale ORDER BY D.data_fine_contratto DESC, D.data_inizio_contratto DESC) AS rnDESC
    FROM Disdette D
),
TelefonoDettaglio
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
TableData
AS (
    SELECT
        SC.id_sog_commerciale,

        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            SC.id_sog_commerciale,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            SC.codice,
            SC.id_anagrafica,
            SC.tipo,
            SC.id_gruppo_agenti,
            A.rag_soc_1,
            A.rag_soc_2,
            A.indirizzo,
            A.cap,
            A.localita,
            A.provincia,
            A.nazione,
            A.cod_fiscale,
            A.par_iva,
            SCMD.Email,
            SCMD.nome,
            SCMD.cognome,
            SCMD.Quote,
            SCMD.telefono_descrizione,
            SCMD.id_telefono,
            AD.id_documento,
            AD.num_documento,
            AD.data_documento,
            AD.data_inizio_contratto,
            AD.data_fine_contratto,
            AD.HasSconto,
            DD.data_disdetta,
            DD.motivo_disdetta,
            TDT.num_riferimento,
            TDC.num_riferimento,
            TDF.num_riferimento,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,

        COALESCE(SC.codice, N'') AS codice,
        COALESCE(SC.id_anagrafica, -1) AS id_anagrafica,
        COALESCE(SC.tipo, 'C') AS tipo,
        COALESCE(SC.id_gruppo_agenti, -1) AS id_gruppo_agenti,
        COALESCE(A.rag_soc_1, N'') + COALESCE(A.rag_soc_2, N'') AS RagioneSociale,
        COALESCE(A.indirizzo, N'') AS indirizzo,
        COALESCE(A.cap, N'') AS cap,
        COALESCE(A.localita, N'') AS localita,
        COALESCE(A.provincia, N'') AS provincia,
        COALESCE(A.nazione, N'') AS nazione,
        COALESCE(A.cod_fiscale, N'') AS cod_fiscale,
        COALESCE(A.par_iva, N'') AS par_iva,
        COALESCE(SCMD.Email, N'') AS Email,
        COALESCE(SCMD.nome, N'') AS nome,
        COALESCE(SCMD.cognome, N'') AS cognome,
        COALESCE(SCMD.Quote, 0) AS Quote,
        COALESCE(SCMD.telefono_descrizione, N'') AS telefono_descrizione,
        COALESCE(SCMD.id_telefono, -1) AS id_telefono,
        COALESCE(AD.id_documento, -1) AS id_documento,
        COALESCE(AD.num_documento, N'') AS num_documento,
        AD.data_documento,
        AD.data_inizio_contratto,
        AD.data_fine_contratto,
        COALESCE(AD.HasSconto, 0) AS HasSconto,
        DD.data_disdetta,
        COALESCE(DD.motivo_disdetta, N'') AS motivo_disdetta,
        COALESCE(TDT.num_riferimento, N'') AS Telefono,
        COALESCE(TDC.num_riferimento, N'') AS Cellulare,
        COALESCE(TDF.num_riferimento, N'') AS Fax

    FROM Landing.COMETA_SoggettoCommerciale SC
    INNER JOIN Landing.COMETA_Anagrafica A ON A.id_anagrafica = SC.id_anagrafica
        AND A.IsDeleted = CAST(0 AS BIT)
    INNER JOIN SoggettoCommercialeMailDettaglio SCMD ON SCMD.id_sog_commerciale = SC.id_sog_commerciale
        AND SCMD.rn = 1
    LEFT JOIN AbbonamentiDettaglio AD ON AD.id_sog_commerciale = SC.id_sog_commerciale
        AND AD.rnDESC = 1
    LEFT JOIN DisdetteDettaglio DD ON DD.id_sog_commerciale = SC.id_sog_commerciale
        AND DD.rnDESC = 1
    LEFT JOIN TelefonoDettaglio TDT ON TDT.id_anagrafica = SC.id_anagrafica
        AND TDT.tipo = 'T'
        AND TDT.rn = 1
    LEFT JOIN TelefonoDettaglio TDC ON TDC.id_anagrafica = SC.id_anagrafica
        AND TDC.tipo = 'C'
        AND TDC.rn = 1
    LEFT JOIN TelefonoDettaglio TDF ON TDF.id_anagrafica = SC.id_anagrafica
        AND TDF.tipo = 'F'
        AND TDF.rn = 1
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

    -- Altri campi
    TD.codice,
    TD.id_anagrafica,
    TD.tipo,
    TD.id_gruppo_agenti,
    TD.RagioneSociale,
    TD.indirizzo,
    TD.cap,
    TD.localita,
    TD.provincia,
    TD.nazione,
    TD.cod_fiscale,
    TD.par_iva,
    TD.Email,
    TD.nome,
    TD.cognome,
    TD.Quote,
    TD.telefono_descrizione,
    TD.id_telefono,
    TD.id_documento,
    TD.num_documento,
    TD.data_documento,
    TD.data_inizio_contratto,
    TD.data_fine_contratto,
    TD.HasSconto,
    TD.data_disdetta,
    TD.motivo_disdetta,
    TD.Telefono,
    TD.Cellulare,
    TD.Fax

FROM TableData TD;
GO

--DROP TABLE IF EXISTS Staging.CometaCustomer;
GO

IF OBJECT_ID(N'Staging.CometaCustomer', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Staging.CometaCustomer FROM Staging.CometaCustomerView;

    ALTER TABLE Staging.CometaCustomer ADD CONSTRAINT PK_Staging_CometaCustomer PRIMARY KEY CLUSTERED (UpdateDatetime, id_sog_commerciale);

    --ALTER TABLE Staging.CometaCustomer ALTER COLUMN Email NVARCHAR(60) NOT NULL;

    CREATE UNIQUE NONCLUSTERED INDEX IX_Staging_CometaCustomer_BusinessKey ON Staging.CometaCustomer (id_sog_commerciale);
END;
GO

CREATE OR ALTER PROCEDURE Staging.usp_Reload_CometaCustomer
AS
BEGIN

    SET NOCOUNT ON;

    BEGIN TRANSACTION 

    TRUNCATE TABLE Staging.CometaCustomer;

    INSERT INTO Staging.CometaCustomer SELECT * FROM Staging.CometaCustomerView;

    COMMIT TRANSACTION 

END;
GO

EXEC Staging.usp_Reload_CometaCustomer;
GO

/**
 * @table Staging.CometaVendor
 * @description 

 * @depends Landing.COMETA_SoggettoCommerciale
 * @depends Landing.COMETA_Anagrafica

SELECT TOP (1) * FROM Landing.COMETA_SoggettoCommerciale;
SELECT TOP (1) * FROM Landing.COMETA_Anagrafica;
*/

CREATE OR ALTER VIEW Staging.CometaVendorView
AS
WITH TableData
AS (
    SELECT
        SC.id_sog_commerciale,

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
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,

        SC.codice,
        SC.id_anagrafica,
        SC.tipo,
        COALESCE(A.rag_soc_1, N'') + COALESCE(A.rag_soc_2, N'') AS RagioneSociale,
        A.indirizzo,
        A.cap,
        A.localita,
        A.provincia,
        A.nazione,
        A.cod_fiscale,
        A.par_iva

    FROM Landing.COMETA_SoggettoCommerciale SC
    INNER JOIN Landing.COMETA_Anagrafica A ON A.id_anagrafica = SC.id_anagrafica
        AND A.IsDeleted = CAST(0 AS BIT)
    WHERE SC.tipo = 'F'
        AND SC.IsDeleted = CAST(0 AS BIT)
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

    -- Altri campi
    TD.codice,
    TD.id_anagrafica,
    TD.tipo,
    TD.RagioneSociale,
    TD.indirizzo,
    TD.cap,
    TD.localita,
    TD.provincia,
    TD.nazione,
    TD.cod_fiscale,
    TD.par_iva

FROM TableData TD;
GO

--DROP TABLE IF EXISTS Staging.CometaVendor;
GO

IF OBJECT_ID(N'Staging.CometaVendor', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Staging.CometaVendor FROM Staging.CometaVendorView;

    ALTER TABLE Staging.CometaVendor ADD CONSTRAINT PK_Staging_CometaVendor PRIMARY KEY CLUSTERED (UpdateDatetime, id_sog_commerciale);

    --ALTER TABLE Staging.CometaVendor ALTER COLUMN Email NVARCHAR(60) NOT NULL;

    CREATE UNIQUE NONCLUSTERED INDEX IX_Staging_CometaVendor_BusinessKey ON Staging.CometaVendor (id_sog_commerciale);
END;
GO

CREATE OR ALTER PROCEDURE Staging.usp_Reload_CometaVendor
AS
BEGIN

    SET NOCOUNT ON;

    BEGIN TRANSACTION 

    TRUNCATE TABLE Staging.CometaVendor;

    INSERT INTO Staging.CometaVendor SELECT * FROM Staging.CometaVendorView;

    COMMIT TRANSACTION 

END;
GO

EXEC Staging.usp_Reload_CometaVendor;
GO

/**
 * @table Staging.MySolutionCustomer
 * @description

 * @depends Landing.MYSOLUTION_Customer
 * @depends Landing.MYSOLUTION_CustomerAddresses
 * @depends Landing.MYSOLUTION_Address
 * @depends Landing.MYSOLUTION_GenericAttribute
 * @depends Landing.MYSOLUTION_Country
 * @depends Landing.MYSOLUTION_StateProvince
 * @depends Landing.MYSOLUTION_Customer_CustomerRole_Mapping

SELECT TOP (1) * FROM Landing.MYSOLUTION_Customer;
SELECT TOP (1) * FROM Landing.MYSOLUTION_CustomerAddresses;
SELECT TOP (1) * FROM Landing.MYSOLUTION_Address;
SELECT TOP (1) * FROM Landing.MYSOLUTION_GenericAttribute;
SELECT TOP (1) * FROM Landing.MYSOLUTION_Country;
SELECT TOP (1) * FROM Landing.MYSOLUTION_StateProvince;
SELECT TOP (1) * FROM Landing.MYSOLUTION_Customer_CustomerRole_Mapping;
*/

CREATE OR ALTER VIEW Staging.MySolutionCustomerView
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
            CC.id_sog_commerciale,
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
        CASE WHEN CCRM11.Customer_Id IS NOT NULL THEN 1 ELSE 0 END AS HasRoleMySolutionDemo,
        CC.id_sog_commerciale

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
    LEFT JOIN Staging.CometaCustomer CC ON CC.id_anagrafica = C.IdCometa
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
    TDD.HasRoleMySolutionDemo,
    TDD.id_sog_commerciale

FROM TableDataDetail TDD
WHERE TDD.rnAddressDESC = 1;
GO

--DROP TABLE IF EXISTS Staging.MySolutionCustomer;
GO

IF OBJECT_ID(N'Staging.MySolutionCustomer', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Staging.MySolutionCustomer FROM Staging.MySolutionCustomerView;

    ALTER TABLE Staging.MySolutionCustomer ADD CONSTRAINT PK_Staging_MySolutionCustomer PRIMARY KEY CLUSTERED (UpdateDatetime, Id);

    ALTER TABLE Staging.MySolutionCustomer ALTER COLUMN HasRoleMySolutionDemo BIT NOT NULL;

    CREATE UNIQUE NONCLUSTERED INDEX IX_Staging_MySolutionCustomer_BusinessKey ON Staging.MySolutionCustomer (Id);
END;
GO

CREATE OR ALTER PROCEDURE Staging.usp_Reload_MySolutionCustomer
AS
BEGIN

    SET NOCOUNT ON;

    BEGIN TRANSACTION 

    TRUNCATE TABLE Staging.MySolutionCustomer;

    INSERT INTO Staging.MySolutionCustomer SELECT * FROM Staging.MySolutionCustomerView;

    COMMIT TRANSACTION 

END;
GO

EXEC Staging.usp_Reload_MySolutionCustomer;
GO

/**
 * @table Staging.AccessiUsername
 * @description
*/

CREATE OR ALTER VIEW Staging.AccessiUsernameView
AS
SELECT DISTINCT
    LFR.Username AS UsernameAccessi,
    LTRIM(RTRIM(LOWER(LFR.Username))) AS Username

FROM Landing.MYSOLUTION_LogsForReport LFR
WHERE COALESCE(LTRIM(RTRIM(LOWER(LFR.Username))), N'') LIKE N'%@%';
GO

--DROP TABLE IF EXISTS Staging.AccessiUsername;
GO

IF OBJECT_ID(N'Staging.AccessiUsername', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Staging.AccessiUsername FROM Staging.AccessiUsernameView;

    ALTER TABLE Staging.AccessiUsername ALTER COLUMN Username NVARCHAR(60) NOT NULL;

    ALTER TABLE Staging.AccessiUsername ADD CONSTRAINT PK_Staging_AccessiUsername PRIMARY KEY CLUSTERED (UsernameAccessi);

    CREATE UNIQUE NONCLUSTERED INDEX IX_Staging_AccessiUsername_BusinessKey ON Staging.AccessiUsername (UsernameAccessi);
END;
GO

CREATE OR ALTER PROCEDURE Staging.usp_Reload_AccessiUsername
AS
BEGIN

    SET NOCOUNT ON;

    BEGIN TRANSACTION 

    TRUNCATE TABLE Staging.AccessiUsername;

    INSERT INTO Staging.AccessiUsername SELECT * FROM Staging.AccessiUsernameView;

    COMMIT TRANSACTION 

END;
GO

EXEC Staging.usp_Reload_AccessiUsername;
GO

/**
 * @table Staging.AccessiCustomer
 * @description

 * @depends Landing.MYSOLUTION_LogsForReport

SELECT TOP (1) * FROM Landing.MYSOLUTION_LogsForReport;
*/

CREATE OR ALTER VIEW Staging.AccessiCustomerView
AS
WITH TableDataDetail
AS (
    SELECT
        AU.Username,

        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            AU.Username,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            CC.id_sog_commerciale,
            MSC.Id,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,

        CC.id_sog_commerciale AS IDSoggettoCommerciale,
        MSC.Id AS IDMySolution,
        ROW_NUMBER() OVER (PARTITION BY AU.Username ORDER BY CC.id_sog_commerciale DESC, MSC.Id DESC) AS rn

    FROM Staging.AccessiUsername AU
    LEFT JOIN Staging.CometaCustomer CC ON CC.Email = AU.Username
    LEFT JOIN Staging.MySolutionCustomer MSC ON MSC.Email = AU.Username
)
SELECT
    -- Chiavi
    TDD.Username,

    -- Campi per sincronizzazione
    TDD.HistoricalHashKey,
    TDD.ChangeHashKey,
    CONVERT(VARCHAR(34), TDD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TDD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TDD.InsertDatetime,
    TDD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Altri campi
    TDD.IDSoggettoCommerciale,
    TDD.IDMySolution

FROM TableDataDetail TDD
WHERE TDD.rn = 1;
GO

--DROP TABLE IF EXISTS Staging.AccessiCustomer;
GO

IF OBJECT_ID(N'Staging.AccessiCustomer', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Staging.AccessiCustomer FROM Staging.AccessiCustomerView;

    ALTER TABLE Staging.AccessiCustomer ALTER COLUMN Username NVARCHAR(60) NOT NULL;

    ALTER TABLE Staging.AccessiCustomer ADD CONSTRAINT PK_Staging_AccessiCustomer PRIMARY KEY CLUSTERED (UpdateDatetime, Username);

    CREATE UNIQUE NONCLUSTERED INDEX IX_Staging_AccessiCustomer_BusinessKey ON Staging.AccessiCustomer (Username);
END;
GO

CREATE OR ALTER PROCEDURE Staging.usp_Reload_AccessiCustomer
AS
BEGIN

    SET NOCOUNT ON;

    BEGIN TRANSACTION 

    TRUNCATE TABLE Staging.AccessiCustomer;

    INSERT INTO Staging.AccessiCustomer SELECT * FROM Staging.AccessiCustomerView;

    COMMIT TRANSACTION 

END;
GO

EXEC Staging.usp_Reload_AccessiCustomer;
GO

/**
 * @table Dim.Cliente
 * @description 
*/

CREATE OR ALTER VIEW Dim.ClienteCometaView
AS
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
),
TableData
AS (
    SELECT
        CC.id_sog_commerciale AS IDSoggettoCommerciale,

        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            CC.id_sog_commerciale,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            CC.Email,
            CC.id_anagrafica,
            CC.codice,
            CC.tipo,
            CC.RagioneSociale,
            CC.cod_fiscale,
            CC.par_iva,
            CC.indirizzo,
            CC.cap,
            CC.localita,
            CC.provincia,
            P.DescrProvincia,
            P.DescrRegione,
            P.DescrMacroregione,
            P.DescrNazione,
            CC.nazione,
            CC.Telefono,
            CC.Cellulare,
            CC.Fax,
            MSU.tipo,
            GA.CapoArea,
            DIC.PKData,
            DFC.PKData,
            CC.motivo_disdetta,
            GA.PKGruppoAgenti,
            CC.cognome,
            CC.nome,
            CAST(
            CASE
              WHEN CURRENT_TIMESTAMP BETWEEN CC.data_inizio_contratto AND CC.data_fine_contratto THEN 1
              -- TODO: aggiungere accessi nell'ultimo mese
              ELSE 0
            END AS BIT),
            CAST(
                CASE
                  WHEN CURRENT_TIMESTAMP BETWEEN CC.data_inizio_contratto AND CC.data_fine_contratto THEN 1
                  ELSE 0
                END AS BIT),
            CAST(CASE WHEN EXISTS (SELECT TOP (1) D.IDDocumento FROM Fact.Documenti D WHERE D.IsProfiloValidoPerStatisticaFatturatoFormazione = CAST(1 AS BIT) AND IsDeleted = CAST(0 AS BIT)) THEN 1 ELSE 0 END AS BIT),
            CADBL.CapoAreaDefault,
            CADBCAP.CapoAreaDefault,
            PA.CapoArea,
            CADBL.AgenteDefault,
            CADBCAP.AgenteDefault,
            PA.Agente,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,

        CC.Email,
        CC.id_anagrafica AS IDAnagraficaCometa,
        CAST(1 AS BIT) AS HasAnagraficaCometa,
        CAST(0 AS BIT) AS HasAnagraficaNopCommerce,
        CAST(0 AS BIT) AS HasAnagraficaMySolution,
        N'COMETA' AS ProvenienzaAnagrafica,
        CC.codice AS CodiceCliente,
        CC.tipo AS TipoSoggettoCommerciale,
        CC.RagioneSociale,
        CC.cod_fiscale AS CodiceFiscale,
        CC.par_iva AS PartitaIVA,
        CC.indirizzo AS Indirizzo,
        CC.cap AS CAP,
        CC.localita AS Localita,
        CC.provincia AS IDProvincia,
        COALESCE(P.DescrProvincia, CASE WHEN COALESCE(CC.provincia, N'') = N'' THEN N'' ELSE N'<???>' END) AS Provincia,
        COALESCE(P.DescrRegione, CASE WHEN COALESCE(CC.provincia, N'') = N'' THEN N'' ELSE N'<???>' END) AS Regione,
        COALESCE(P.DescrMacroregione, CASE WHEN COALESCE(CC.provincia, N'') = N'' THEN N'' ELSE N'<???>' END) AS Macroregione,
        COALESCE(P.DescrNazione, CC.nazione, N'') AS Nazione,
        CC.Telefono,
        CC.Cellulare,
        CC.Fax,
        COALESCE(MSU.tipo, N'') AS TipoCliente,
        COALESCE(GA.CapoArea, N'') AS Agente,
        --CC.data_inizio_contratto,
        COALESCE(DIC.PKData, CAST('19000101' AS DATE)) AS PKDataInizioContratto,
        --CC.data_fine_contratto,
        COALESCE(DFC.PKData, CAST('19000101' AS DATE)) AS PKDataFineContratto,
        CASE
          WHEN DFC.PKData IS NOT NULL THEN CAST('19000101' AS DATE)
          ELSE COALESCE(DD.PKData, CAST('19000101' AS DATE))
        END AS PKDataDisdetta,
        CASE
          WHEN DFC.PKData IS NOT NULL THEN N''
          ELSE COALESCE(CC.motivo_disdetta, N'')
        END AS MotivoDisdetta,
        --CC.id_gruppo_agenti AS IDGruppoAgenti,
        COALESCE(GA.PKGruppoAgenti, -1) AS PKGruppoAgenti,
        CC.cognome AS Cognome,
        CC.nome AS Nome,
        CAST(
            CASE
              WHEN CURRENT_TIMESTAMP BETWEEN CC.data_inizio_contratto AND CC.data_fine_contratto THEN 1
              -- TODO: aggiungere accessi nell'ultimo mese
              ELSE 0
            END AS BIT) AS IsAttivo,
        CAST(
            CASE
              WHEN CURRENT_TIMESTAMP BETWEEN CC.data_inizio_contratto AND CC.data_fine_contratto THEN 1
              ELSE 0
            END AS BIT) AS IsAbbonato,
        --IDSoggettoCommerciale_migrazione INT NULL,
        --IDSoggettoCommerciale_migrazione_old INT NULL,
        CAST(CASE WHEN EXISTS (SELECT TOP (1) D.IDDocumento FROM Fact.Documenti D WHERE D.IsProfiloValidoPerStatisticaFatturatoFormazione = CAST(1 AS BIT) AND IsDeleted = CAST(0 AS BIT)) THEN 1 ELSE 0 END AS BIT) AS IsClienteFormazione,
        COALESCE(CADBL.CapoAreaDefault, CADBCAP.CapoAreaDefault, PA.CapoArea, N'') AS CapoAreaDefault,
        COALESCE(CADBL.AgenteDefault, CADBCAP.AgenteDefault, PA.Agente, N'') AS AgenteDefault

    FROM Staging.CometaCustomer CC
    LEFT JOIN Import.Provincia P ON P.CodSiglaProvincia = CC.Provincia
    LEFT JOIN Dim.Data DIC ON DIC.PKData = CC.data_inizio_contratto
    LEFT JOIN Dim.Data DFC ON DFC.PKData = CC.data_fine_contratto
    LEFT JOIN Dim.Data DD ON DD.PKData = CC.data_disdetta
    LEFT JOIN Landing.COMETA_MySolutionUsers MSU ON MSU.EMail = CC.Email
        AND CC.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Dim.GruppoAgenti GA ON GA.id_gruppo_agenti = CC.id_gruppo_agenti
        AND GA.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Import.ProvinciaAgente PA ON PA.IDProvincia = CC.provincia
    LEFT JOIN CapoAreaDefaultByCAP CADBCAP ON CADBCAP.IDProvincia = CC.provincia AND CADBCAP.CAP = CC.cap
    LEFT JOIN CapoAreaDefaultByLocalita CADBL ON CADBL.IDProvincia = CC.provincia AND CADBL.Localita = CC.localita
    WHERE CC.IsDeleted = CAST(0 AS BIT)
)
SELECT
    TD.IDSoggettoCommerciale,

    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

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
    TD.IDProvincia,
    TD.Provincia,
    TD.Regione,
    TD.Macroregione,
    TD.Nazione,
    TD.Telefono,
    TD.Cellulare,
    TD.Fax,
    TD.TipoCliente,
    TD.Agente,
    TD.PKDataInizioContratto,
    TD.PKDataFineContratto,
    TD.PKDataDisdetta,
    TD.MotivoDisdetta,
    TD.PKGruppoAgenti,
    TD.Cognome,
    TD.Nome,
    TD.IsAttivo,
    TD.IsAbbonato,
    TD.IsClienteFormazione,
    TD.CapoAreaDefault,
    TD.AgenteDefault

FROM TableData TD;
GO

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
GO

ALTER SEQUENCE dbo.seq_Dim_Cliente RESTART WITH 1;
GO

CREATE OR ALTER PROCEDURE Dim.usp_Merge_Cliente
AS
BEGIN

    SET NOCOUNT ON;

    BEGIN TRANSACTION 

    DECLARE @provider_name NVARCHAR(60) = N'MyDatamartReporting';
    DECLARE @full_table_name sysname = N'Landing.COMETA_SoggettoCommerciale';

    WITH TargetTable
    AS (
        SELECT
            *
        FROM Dim.Cliente
        WHERE PKCliente > 0
    )
    MERGE INTO TargetTable AS TGT
    USING Dim.ClienteCometaView (nolock) AS SRC
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
        TGT.IsAttivo = SRC.IsAttivo,
        TGT.IsAbbonato = SRC.IsAbbonato,
        --TGT.IDSoggettoCommerciale_migrazione = SRC.IDSoggettoCommerciale_migrazione,
        --TGT.IDSoggettoCommerciale_migrazione_old = SRC.IDSoggettoCommerciale_migrazione_old,
        TGT.IDProvincia = SRC.IDProvincia,
        TGT.CapoAreaDefault = SRC.CapoAreaDefault,
        TGT.AgenteDefault = SRC.AgenteDefault,
        TGT.HasRoleMySolutionDemo = CAST(0 AS BIT) -- SRC.HasRoleMySolutionDemo

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
        --IDSoggettoCommerciale_migrazione,
        --IDSoggettoCommerciale_migrazione_old,
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
        --SRC.IDSoggettoCommerciale_migrazione,
        --SRC.IDSoggettoCommerciale_migrazione_old,
        SRC.IDProvincia,
        SRC.CapoAreaDefault,
        SRC.AgenteDefault,
        0 --SRC.HasRoleMySolutionDemo
      )

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        $action AS merge_action,
        'Dim.Cliente' AS full_olap_table_name,
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

    ------ Verifica migrazioni da NOPCOMMERCE a COMETA
    ----UPDATE T
    ----SET T.IsDeleted = CAST(1 AS BIT)

    ----FROM Dim.Cliente T
    ----INNER JOIN Staging.Cliente SC ON SC.Email = T.Email
    ----    AND SC.ProvenienzaAnagrafica IN (N'COMETA')
    ----WHERE T.ProvenienzaAnagrafica IN (N'NOPCOMMERCE');

    ----UPDATE CNew
    ----SET CNew.IDSoggettoCommerciale_migrazione_old = COld.IDSoggettoCommerciale

    ----FROM Dim.Cliente CNew
    ----INNER JOIN Staging.Cliente SC ON SC.Email = CNew.Email
    ----    AND SC.ProvenienzaAnagrafica IN (N'COMETA')
    ----INNER JOIN Dim.Cliente COld ON COld.Email = CNew.Email
    ----    AND COld.ProvenienzaAnagrafica IN (N'NOPCOMMERCE')
    ----WHERE CNew.ProvenienzaAnagrafica IN (N'COMETA');

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

    ------ Aggiornamento CapoAreaDefault
    ----WITH CapoAreaDefaultByCAP
    ----AS (
    ----    SELECT
    ----        CCA.IDProvincia,
    ----        CCA.CAP,
    ----        MAX(CCA.CapoArea) AS CapoAreaDefault,
    ----        MAX(CCA.Agente) AS AgenteDefault

    ----    FROM Import.ComuneCAPAgente CCA
    ----    GROUP BY CCA.IDProvincia,
    ----        CCA.CAP
    ----    HAVING COUNT(DISTINCT CCA.CapoArea) = 1
    ----),
    ----CapoAreaDefaultByLocalita
    ----AS (
    ----    SELECT
    ----        CCA.IDProvincia,
    ----        CCA.Comune AS Localita,
    ----        MAX(CCA.CapoArea) AS CapoAreaDefault,
    ----        MAX(CCA.Agente) AS AgenteDefault

    ----    FROM Import.ComuneCAPAgente CCA
    ----    GROUP BY CCA.IDProvincia,
    ----        CCA.Comune
    ----    HAVING COUNT(1) = 1
    ----)
    ----UPDATE C
    ----SET C.CapoAreaDefault = COALESCE(CADBL.CapoAreaDefault, CADBCAP.CapoAreaDefault, PA.CapoArea, N''),
    ----    C.AgenteDefault = COALESCE(CADBL.AgenteDefault, CADBCAP.AgenteDefault, PA.Agente, N'')

    ----FROM Dim.Cliente C
    ----LEFT JOIN Import.ProvinciaAgente PA ON PA.IDProvincia = C.IDProvincia
    ----LEFT JOIN CapoAreaDefaultByCAP CADBCAP ON CADBCAP.IDProvincia = C.IDProvincia AND CADBCAP.CAP = C.CAP
    ----LEFT JOIN CapoAreaDefaultByLocalita CADBL ON CADBL.IDProvincia = C.IDProvincia AND CADBL.Localita = C.Localita;

    -- Aggiornamento PKDataDisdetta
    ----WITH DocumentiMySolution
    ----AS (
    ----    SELECT DISTINCT
    ----        D.IDDocumento,
    ----        D.PKCliente,
    ----        D.PKDataFineContratto,
    ----        D.PKDataDisdetta

    ----    FROM Fact.Documenti D
    ----    WHERE D.IDProfilo = N'ORDCLI'
    ----),
    ----DocumentiMySolutionNumerati
    ----AS (
    ----    SELECT
    ----        DMS.IDDocumento,
    ----        DMS.PKCliente,
    ----        DMS.PKDataFineContratto,
    ----        DMS.PKDataDisdetta,
    ----        ROW_NUMBER() OVER (PARTITION BY DMS.PKCliente ORDER BY DMS.PKDataFineContratto DESC) AS rn

    ----    FROM DocumentiMySolution DMS
    ----)
    ----UPDATE C
    ----SET C.PKDataDisdetta = DMSN.PKDataDisdetta
    ----FROM Dim.Cliente C
    ----INNER JOIN DocumentiMySolutionNumerati DMSN ON DMSN.PKCliente = C.PKCliente
    ----    AND DMSN.rn = 1;

    UPDATE audit.tables
    SET lastupdated_local = lastupdated_staging
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    COMMIT TRANSACTION;

END;
GO

EXEC Dim.usp_Merge_Cliente;
GO

-- Eseguire script CesiDW_40.03_Fact_Documenti (con DROP e CREATE)
-- Eseguire script CesiDW_40.04_Fact_Scadenze (con DROP e CREATE)
-- Eseguire script CesiDW_40.05_Fact_Budget (con DROP e CREATE)
-- Eseguire script CesiDW_30.06_Dim_Utente (con DROP e CREATE)
-- Eseguire script CesiDW_40.06_Fact_Corsi (con DROP e CREATE)

/**
 * @table Dim.ClienteAccessi
 * @description

*/

IF OBJECT_ID('dbo.seq_Dim_ClienteAccessi', 'SO') IS NULL
BEGIN

    CREATE SEQUENCE dbo.seq_Dim_ClienteAccessi START WITH 1;

END;
GO

CREATE OR ALTER VIEW Dim.ClienteAccessiView
AS
WITH TableData
AS (
    SELECT
        AC.Username,

        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            AC.Username,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            AC.IDSoggettoCommerciale,
            C.PKCliente,
            C.Email,
            C.IDAnagraficaCometa,
            C.HasAnagraficaCometa,
            C.HasAnagraficaNopCommerce,
            C.HasAnagraficaMySolution,
            C.ProvenienzaAnagrafica,
            C.CodiceCliente,
            C.TipoSoggettoCommerciale,
            C.RagioneSociale,
            C.CodiceFiscale,
            C.PartitaIVA,
            C.Indirizzo,
            C.CAP,
            C.Localita,
            C.Provincia,
            C.Regione,
            C.Macroregione,
            C.Nazione,
            C.Telefono,
            C.Cellulare,
            C.Fax,
            C.TipoCliente,
            C.Agente,
            C.PKDataInizioContratto,
            C.PKDataFineContratto,
            C.PKDataDisdetta,
            C.MotivoDisdetta,
            C.PKGruppoAgenti,
            C.Cognome,
            C.Nome,
            C.IsAttivo,
            C.IsAbbonato,
            C.IDSoggettoCommerciale_migrazione,
            C.IDSoggettoCommerciale_migrazione_old,
            C.IDProvincia,
            C.IsClienteFormazione,
            C.CapoAreaDefault,
            C.AgenteDefault,
            C.HasRoleMySolutionDemo,
            AC.IDMySolution,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,

        AC.IDSoggettoCommerciale,
        COALESCE(C.PKCliente, -101) AS PKCliente,
        COALESCE(C.Email, N'') AS Email,
        COALESCE(C.IDAnagraficaCometa, -1) AS IDAnagraficaCometa,
        COALESCE(C.HasAnagraficaCometa, 0) AS HasAnagraficaCometa,
        COALESCE(C.HasAnagraficaNopCommerce, 0) AS HasAnagraficaNopCommerce,
        COALESCE(C.HasAnagraficaMySolution, 0) AS HasAnagraficaMySolution,
        COALESCE(C.ProvenienzaAnagrafica, N'ACCESSI') AS ProvenienzaAnagrafica,
        COALESCE(C.CodiceCliente, N'') AS CodiceCliente,
        COALESCE(C.TipoSoggettoCommerciale, N'') AS TipoSoggettoCommerciale,
        COALESCE(C.RagioneSociale, N'') AS RagioneSociale,
        COALESCE(C.CodiceFiscale, N'') AS CodiceFiscale,
        COALESCE(C.PartitaIVA, N'') AS PartitaIVA,
        COALESCE(C.Indirizzo, N'') AS Indirizzo,
        COALESCE(C.CAP, N'') AS CAP,
        COALESCE(C.Localita, N'') AS Localita,
        COALESCE(C.Provincia, N'') AS Provincia,
        COALESCE(C.Regione, N'') AS Regione,
        COALESCE(C.Macroregione, N'') AS Macroregione,
        COALESCE(C.Nazione, N'') AS Nazione,
        COALESCE(C.Telefono, N'') AS Telefono,
        COALESCE(C.Cellulare, N'') AS Cellulare,
        COALESCE(C.Fax, N'') AS Fax,
        COALESCE(C.TipoCliente, N'') AS TipoCliente,
        COALESCE(C.Agente, N'') AS Agente,
        COALESCE(C.PKDataInizioContratto, CAST('19000101' AS DATE)) AS PKDataInizioContratto,
        COALESCE(C.PKDataFineContratto, CAST('19000101' AS DATE)) AS PKDataFineContratto,
        COALESCE(C.PKDataDisdetta, CAST('19000101' AS DATE)) AS PKDataDisdetta,
        COALESCE(C.MotivoDisdetta, N'') AS MotivoDisdetta,
        COALESCE(C.PKGruppoAgenti, -1) AS PKGruppoAgenti,
        COALESCE(C.Cognome, N'') AS Cognome,
        COALESCE(C.Nome, N'') AS Nome,
        COALESCE(C.IsAttivo, 0) AS IsAttivo,
        COALESCE(C.IsAbbonato, 0) AS IsAbbonato,
        COALESCE(C.IDSoggettoCommerciale_migrazione, -1) AS IDSoggettoCommerciale_migrazione,
        COALESCE(C.IDSoggettoCommerciale_migrazione_old, -1) AS IDSoggettoCommerciale_migrazione_old,
        COALESCE(C.IDProvincia, N'') AS IDProvincia,
        COALESCE(C.IsClienteFormazione, 0) AS IsClienteFormazione,
        COALESCE(C.CapoAreaDefault, N'') AS CapoAreaDefault,
        COALESCE(C.AgenteDefault, N'') AS AgenteDefault,
        COALESCE(C.HasRoleMySolutionDemo, 0) AS HasRoleMySolutionDemo,
        AC.IDMySolution

    FROM Staging.AccessiCustomer AC
    LEFT JOIN Dim.Cliente C ON C.IDSoggettoCommerciale = AC.IDSoggettoCommerciale
)
SELECT
    TD.Username,

    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    TD.IDSoggettoCommerciale,
    TD.PKCliente,
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
    TD.Telefono,
    TD.Cellulare,
    TD.Fax,
    TD.TipoCliente,
    TD.Agente,
    TD.PKDataInizioContratto,
    TD.PKDataFineContratto,
    TD.PKDataDisdetta,
    TD.MotivoDisdetta,
    TD.PKGruppoAgenti,
    TD.Cognome,
    TD.Nome,
    TD.IsAttivo,
    TD.IsAbbonato,
    TD.IDSoggettoCommerciale_migrazione,
    TD.IDSoggettoCommerciale_migrazione_old,
    TD.IDProvincia,
    TD.IsClienteFormazione,
    TD.CapoAreaDefault,
    TD.AgenteDefault,
    TD.HasRoleMySolutionDemo,
    TD.IDMySolution

FROM TableData TD;
GO

--DROP TABLE IF EXISTS Dim.ClienteAccessi;
GO

IF OBJECT_ID('Dim.ClienteAccessi', 'U') IS NULL
BEGIN

    CREATE TABLE Dim.ClienteAccessi (
        PKClienteAccessi INT NOT NULL CONSTRAINT PK_Dim_ClienteAccessi PRIMARY KEY CLUSTERED CONSTRAINT DFT_Dim_ClienteAccessi_PKClienteAccessi DEFAULT (NEXT VALUE FOR dbo.seq_Dim_ClienteAccessi),
        Username NVARCHAR(60) NOT NULL,

	    HistoricalHashKey VARBINARY(20) NULL,
	    ChangeHashKey VARBINARY(20) NULL,
	    HistoricalHashKeyASCII VARCHAR(34) NULL,
	    ChangeHashKeyASCII VARCHAR(34) NULL,
	    InsertDatetime DATETIME NOT NULL,
	    UpdateDatetime DATETIME NOT NULL,
	    IsDeleted BIT NOT NULL,

        IDSoggettoCommerciale INT NULL,
        PKCliente INT NOT NULL CONSTRAINT FK_Dim_ClienteAccessi_PKCliente FOREIGN KEY REFERENCES Dim.Cliente (PKCliente),
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
        PKDataInizioContratto DATE NOT NULL CONSTRAINT FK_Dim_ClienteAccessi_PKDataInizioContratto REFERENCES Dim.Data (PKData),
        PKDataFineContratto DATE NOT NULL CONSTRAINT FK_Dim_ClienteAccessi_PKDataFineContratto REFERENCES Dim.Data (PKData),
        PKDataDisdetta DATE NOT NULL CONSTRAINT FK_Dim_ClienteAccessi_PKDataDisdetta REFERENCES Dim.Data (PKData),
        MotivoDisdetta NVARCHAR(120) NOT NULL,
        PKGruppoAgenti INT NOT NULL CONSTRAINT FK_Dim_ClienteAccessi_PKGruppoAgenti REFERENCES Dim.GruppoAgenti (PKGruppoAgenti),
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
        HasRoleMySolutionDemo BIT NOT NULL,
        IDMySolution INT NULL
    );

    ALTER TABLE Dim.ClienteAccessi ADD CONSTRAINT DFT_Dim_ClienteAccessi_InsertDatetime DEFAULT (CURRENT_TIMESTAMP) FOR InsertDatetime;
    ALTER TABLE Dim.ClienteAccessi ADD CONSTRAINT DFT_Dim_ClienteAccessi_UpdateDatetime DEFAULT (CURRENT_TIMESTAMP) FOR UpdateDatetime;
    ALTER TABLE Dim.ClienteAccessi ADD CONSTRAINT DFT_Dim_ClienteAccessi_IsDeleted DEFAULT (0) FOR IsDeleted;

    ----ALTER TABLE Dim.ClienteAccessi ADD CONSTRAINT DFT_Dim_ClienteAccessi_IsApproved DEFAULT (0) FOR IsApproved;
    ----ALTER TABLE Dim.ClienteAccessi ADD CONSTRAINT DFT_Dim_ClienteAccessi_IsLockedOut DEFAULT (0) FOR IsLockedOut;
    ----ALTER TABLE Dim.ClienteAccessi ADD CONSTRAINT DFT_Dim_ClienteAccessi_PKCreazione DEFAULT (CAST('19000101' AS DATE)) FOR PKDataCreazione;
    ----ALTER TABLE Dim.ClienteAccessi ADD CONSTRAINT DFT_Dim_ClienteAccessi_PKUltimoLogin DEFAULT (CAST('19000101' AS DATE)) FOR PKDataUltimoLogin;
    ALTER TABLE Dim.ClienteAccessi ADD CONSTRAINT DFT_Dim_ClienteAccessi_IDAnagraficaCometa DEFAULT (-1) FOR IDAnagraficaCometa;
    ALTER TABLE Dim.ClienteAccessi ADD CONSTRAINT DFT_Dim_ClienteAccessi_HasAnagraficaCometa DEFAULT (0) FOR HasAnagraficaCometa;
    ALTER TABLE Dim.ClienteAccessi ADD CONSTRAINT DFT_Dim_ClienteAccessi_HasAnagraficaNopCommerce DEFAULT (0) FOR HasAnagraficaNopCommerce;
    ALTER TABLE Dim.ClienteAccessi ADD CONSTRAINT DFT_Dim_ClienteAccessi_HasAnagraficaMySolution DEFAULT (0) FOR HasAnagraficaMySolution;
    ALTER TABLE Dim.ClienteAccessi ADD CONSTRAINT DFT_Dim_ClienteAccessi_ProvenienzaAnagrafica DEFAULT (N'') FOR ProvenienzaAnagrafica;
    ALTER TABLE Dim.ClienteAccessi ADD CONSTRAINT DFT_Dim_ClienteAccessi_TipoSoggettoCommerciale DEFAULT (N'') FOR TipoSoggettoCommerciale;
    ALTER TABLE Dim.ClienteAccessi ADD CONSTRAINT DFT_Dim_ClienteAccessi_PKInizioContratto DEFAULT (CAST('19000101' AS DATE)) FOR PKDataInizioContratto;
    ALTER TABLE Dim.ClienteAccessi ADD CONSTRAINT DFT_Dim_ClienteAccessi_PKFineContratto DEFAULT (CAST('19000101' AS DATE)) FOR PKDataFineContratto;
    ALTER TABLE Dim.ClienteAccessi ADD CONSTRAINT DFT_Dim_ClienteAccessi_PKDataDisdetta DEFAULT (CAST('19000101' AS DATE)) FOR PKDataDisdetta;
    ALTER TABLE Dim.ClienteAccessi ADD CONSTRAINT DFT_Dim_ClienteAccessi_PKGruppoAgenti DEFAULT (-1) FOR PKGruppoAgenti;
    ALTER TABLE Dim.ClienteAccessi ADD CONSTRAINT DFT_Dim_ClienteAccessi_IsAttivo DEFAULT (0) FOR IsAttivo;
    ALTER TABLE Dim.ClienteAccessi ADD CONSTRAINT DFT_Dim_ClienteAccessi_IsAbbonato DEFAULT (0) FOR IsAbbonato;
    ALTER TABLE Dim.ClienteAccessi ADD CONSTRAINT DFT_Dim_ClienteAccessi_IsClienteFormazione DEFAULT (0) FOR IsClienteFormazione;
    ALTER TABLE Dim.ClienteAccessi ADD CONSTRAINT DFT_Dim_ClienteAccessi_CapoAreaDefault DEFAULT (N'') FOR CapoAreaDefault;
    ALTER TABLE Dim.ClienteAccessi ADD CONSTRAINT DFT_Dim_ClienteAccessi_AgenteDefault DEFAULT (N'') FOR AgenteDefault;
    ALTER TABLE Dim.ClienteAccessi ADD CONSTRAINT DFT_Dim_ClienteAccessi_HasRoleMySolutionDemo DEFAULT (0) FOR HasRoleMySolutionDemo;

    ALTER SEQUENCE dbo.seq_Dim_ClienteAccessi RESTART WITH 1;

END;
GO

CREATE OR ALTER PROCEDURE Dim.usp_Merge_ClienteAccessi
AS
BEGIN

    SET NOCOUNT ON;

    BEGIN TRANSACTION 

    --DECLARE @provider_name NVARCHAR(60) = N'MyDatamartReporting';
    --DECLARE @full_table_name sysname = N'Landing.COMETA_SoggettoCommerciale';

    MERGE INTO Dim.ClienteAccessi AS TGT
    USING Dim.ClienteAccessiView (nolock) AS SRC
    ON SRC.Username = TGT.Username

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.IsDeleted = SRC.IsDeleted,
        TGT.IDSoggettoCommerciale = SRC.IDSoggettoCommerciale,
        TGT.PKCliente = SRC.PKCliente,
        TGT.Email = SRC.Email,
        TGT.IDAnagraficaCometa = SRC.IDAnagraficaCometa,
        TGT.HasAnagraficaCometa = SRC.HasAnagraficaCometa,
        TGT.HasAnagraficaNopCommerce = SRC.HasAnagraficaNopCommerce,
        TGT.HasAnagraficaMySolution = SRC.HasAnagraficaMySolution,
        TGT.ProvenienzaAnagrafica = SRC.ProvenienzaAnagrafica,
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
        TGT.IsAttivo = SRC.IsAttivo,
        TGT.IsAbbonato = SRC.IsAbbonato,
        TGT.IDSoggettoCommerciale_migrazione = SRC.IDSoggettoCommerciale_migrazione,
        TGT.IDSoggettoCommerciale_migrazione_old = SRC.IDSoggettoCommerciale_migrazione_old,
        TGT.IDProvincia = SRC.IDProvincia,
        TGT.IsClienteFormazione = SRC.IsClienteFormazione,
        TGT.CapoAreaDefault = SRC.CapoAreaDefault,
        TGT.AgenteDefault = SRC.AgenteDefault,
        TGT.HasRoleMySolutionDemo = SRC.HasRoleMySolutionDemo,
        TGT.IDMySolution = SRC.IDMySolution

    WHEN NOT MATCHED
      THEN INSERT (
        Username,
        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
        IDSoggettoCommerciale,
        PKCliente,
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
        IsAttivo,
        IsAbbonato,
        IDSoggettoCommerciale_migrazione,
        IDSoggettoCommerciale_migrazione_old,
        IDProvincia,
        IsClienteFormazione,
        CapoAreaDefault,
        AgenteDefault,
        HasRoleMySolutionDemo,
        IDMySolution
      )
      VALUES (
        SRC.Username,
        SRC.HistoricalHashKey,
        SRC.ChangeHashKey,
        SRC.HistoricalHashKeyASCII,
        SRC.ChangeHashKeyASCII,
        SRC.InsertDatetime,
        SRC.UpdateDatetime,
        SRC.IsDeleted,
        SRC.IDSoggettoCommerciale,
        SRC.PKCliente,
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
        SRC.IsAttivo,
        SRC.IsAbbonato,
        SRC.IDSoggettoCommerciale_migrazione,
        SRC.IDSoggettoCommerciale_migrazione_old,
        SRC.IDProvincia,
        SRC.IsClienteFormazione,
        SRC.CapoAreaDefault,
        SRC.AgenteDefault,
        SRC.HasRoleMySolutionDemo,
        SRC.IDMySolution
      )

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        $action AS merge_action,
        'Dim.ClienteAccessi' AS full_olap_table_name,
        'Username = ' + CAST(COALESCE(inserted.Username, deleted.Username) AS NVARCHAR(1000)) AS primary_key_description
    INTO audit.merge_log_details;

    --UPDATE audit.tables
    --SET lastupdated_local = lastupdated_staging
    --WHERE provider_name = @provider_name
    --    AND full_table_name = @full_table_name;

    COMMIT TRANSACTION;

END;
GO

EXEC Dim.usp_Merge_ClienteAccessi;
GO

CREATE OR ALTER VIEW Staging.AccessiView
AS
WITH ClientiAccessi
AS (
    SELECT
        AC.Username,
        C.PKCliente,
        1 AS rn

    FROM Staging.AccessiCustomer AC
    INNER JOIN Dim.Cliente C ON C.IDSoggettoCommerciale = AC.IDSoggettoCommerciale

    UNION ALL

    SELECT
        AC.Username,
        COALESCE(CEmail.PKCliente, -101) AS PKCliente,
        ROW_NUMBER() OVER (PARTITION BY AC.Username ORDER BY CEmail.PKCliente DESC) AS rn

    FROM Staging.AccessiCustomer AC
    LEFT JOIN Dim.Cliente C ON C.IDSoggettoCommerciale = AC.IDSoggettoCommerciale
    LEFT JOIN Dim.Cliente CEmail ON CEmail.Email = AC.Username
),
AccessiRiepilogo
AS (
    SELECT
        --LFR.Data,
        COALESCE(D.PKData, CAST('19000101' AS DATE)) AS PKData,
        --LFR.IDUser,
        CA.PKClienteAccessi,
        CA.PKGruppoAgenti,
        SUM(LFR.NumeroAccessi) AS NumeroAccessi,
        SUM(LFR.NumeroPagineVisitate) AS NumeroPagineVisitate

    FROM Landing.MYSOLUTION_LogsForReport LFR
    LEFT JOIN Dim.Data D ON D.PKData = LFR.Data
    INNER JOIN Staging.AccessiUsername AU ON AU.UsernameAccessi = LFR.Username
    INNER JOIN Dim.ClienteAccessi CA ON CA.Username = AU.Username
    WHERE LFR.IsDeleted = CAST(0 AS BIT)
    GROUP BY COALESCE (D.PKData, CAST ('19000101' AS DATE)),
        CA.PKClienteAccessi,
        CA.PKGruppoAgenti
),
TableData
AS (
    SELECT
        AR.PKData,
        AR.PKClienteAccessi AS PKCliente,

        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            AR.PKData,
            AR.PKClienteAccessi,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            GA.PKCapoArea,
            SUM(AR.NumeroAccessi),
            SUM(AR.NumeroPagineVisitate),
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,

        COALESCE(GA.PKCapoArea, -1) AS PKCapoArea,
        SUM(AR.NumeroAccessi) AS NumeroAccessi,
        SUM(AR.NumeroPagineVisitate) AS NumeroPagineVisitate

    FROM AccessiRiepilogo AR
    LEFT JOIN Dim.ClienteAccessi CA ON CA.PKCliente = AR.PKClienteAccessi
    LEFT JOIN Dim.GruppoAgenti GA ON GA.PKGruppoAgenti = CA.PKGruppoAgenti
    GROUP BY AR.PKData,
        AR.PKClienteAccessi,
        GA.PKCapoArea
)
SELECT
    -- Chiavi
    TD.PKData,
    TD.PKCliente,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Dimensioni
    TD.PKCapoArea,

    -- Misure
    TD.NumeroAccessi,
    TD.NumeroPagineVisitate

FROM TableData TD;
GO

DROP TABLE IF EXISTS Staging.Accessi; DELETE FROM audit.tables WHERE provider_name = N'MyDatamartReporting' AND full_table_name = N'Landing.MYSOLUTION_LogsForReport';
GO

IF OBJECT_ID(N'Staging.Accessi', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Staging.Accessi FROM Staging.AccessiView;

    ALTER TABLE Staging.Accessi ALTER COLUMN PKData DATE NOT NULL;
    ALTER TABLE Staging.Accessi ALTER COLUMN PKCliente INT NOT NULL;

    ALTER TABLE Staging.Accessi ADD CONSTRAINT PK_Staging_Accessi PRIMARY KEY CLUSTERED (PKData, PKCliente);

    ALTER TABLE Staging.Accessi ALTER COLUMN NumeroAccessi INT NOT NULL;
    ALTER TABLE Staging.Accessi ALTER COLUMN NumeroPagineVisitate INT NOT NULL;

    CREATE UNIQUE NONCLUSTERED INDEX IX_Staging_Accessi_PKCliente_PKData ON Staging.Accessi (PKCliente, PKData);
END;
GO

CREATE OR ALTER PROCEDURE Staging.usp_Reload_Accessi
AS
BEGIN

    SET NOCOUNT ON;

    DECLARE @lastupdated_staging DATETIME;
    DECLARE @provider_name NVARCHAR(60) = N'MyDatamartReporting';
    DECLARE @full_table_name sysname = N'Landing.MYSOLUTION_LogsForReport';

    SELECT TOP 1 @lastupdated_staging = lastupdated_staging
    FROM audit.tables
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    IF (@lastupdated_staging IS NULL) SET @lastupdated_staging = CAST('19000101' AS DATETIME);

    BEGIN TRANSACTION

    TRUNCATE TABLE Staging.Accessi;

    INSERT INTO Staging.Accessi
    SELECT * FROM Staging.AccessiView
    WHERE UpdateDatetime > @lastupdated_staging;

    SELECT @lastupdated_staging = MAX(UpdateDatetime) FROM Staging.Accessi;

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

EXEC Staging.usp_Reload_Accessi;
GO

--DROP TABLE IF EXISTS Fact.Accessi; DROP SEQUENCE IF EXISTS dbo.seq_Fact_Accessi;
GO

IF OBJECT_ID('dbo.seq_Fact_Accessi', 'SO') IS NULL
BEGIN

    CREATE SEQUENCE dbo.seq_Fact_Accessi START WITH 1;

END;
GO

IF OBJECT_ID('Fact.Accessi', 'U') IS NULL
BEGIN

    CREATE TABLE Fact.Accessi (
        PKAccessi INT NOT NULL CONSTRAINT PK_Fact_Accessi PRIMARY KEY CLUSTERED CONSTRAINT DFT_Fact_Accessi_PKAccessi DEFAULT (NEXT VALUE FOR dbo.seq_Fact_Accessi),
        PKData DATE NOT NULL CONSTRAINT FK_Fact_Accessi_PKData FOREIGN KEY REFERENCES Dim.Data (PKData),
        PKCliente INT NOT NULL CONSTRAINT FK_Fact_Accessi_PKCliente FOREIGN KEY REFERENCES Dim.ClienteAccessi (PKClienteAccessi),

	    HistoricalHashKey VARBINARY(20) NULL,
	    ChangeHashKey VARBINARY(20) NULL,
	    HistoricalHashKeyASCII VARCHAR(34) NULL,
	    ChangeHashKeyASCII VARCHAR(34) NULL,
	    InsertDatetime DATETIME NOT NULL,
	    UpdateDatetime DATETIME NOT NULL,
	    IsDeleted BIT NOT NULL,

        NumeroAccessi INT NOT NULL,
        NumeroPagineVisitate INT NOT NULL,
        PKCapoArea INT NOT NULL CONSTRAINT FK_Fact_Accessi_PKCapoArea FOREIGN KEY REFERENCES Dim.CapoArea (PKCapoArea)
    );

    CREATE UNIQUE NONCLUSTERED INDEX IX_Fact_Accessi_PKData_PKCliente ON Fact.Accessi (PKData, PKCliente);

    ALTER SEQUENCE dbo.seq_Fact_Accessi RESTART WITH 1;

END;
GO

IF OBJECT_ID(N'Fact.usp_Merge_Accessi', N'P') IS NULL EXEC('CREATE PROCEDURE Fact.usp_Merge_Accessi AS RETURN 0;');
GO

ALTER PROCEDURE Fact.usp_Merge_Accessi
AS
BEGIN

    SET NOCOUNT ON;

    BEGIN TRANSACTION 

    DECLARE @provider_name NVARCHAR(60) = N'MyDatamartReporting';
    DECLARE @full_table_name sysname = N'Landing.MYSOLUTION_LogsForReport';

    MERGE INTO Fact.Accessi AS TGT
    USING Staging.Accessi (nolock) AS SRC
    ON SRC.PKData = TGT.PKData AND SRC.PKCliente = TGT.PKCliente

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.IsDeleted = SRC.IsDeleted,
        TGT.NumeroAccessi = SRC.NumeroAccessi,
        TGT.NumeroPagineVisitate = SRC.NumeroPagineVisitate,
        TGT.PKCapoArea = SRC.PKCapoArea

    WHEN NOT MATCHED
      THEN INSERT (
        PKData,
        PKCliente,
        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
        NumeroAccessi,
        NumeroPagineVisitate,
        PKCapoArea
      )
      VALUES (
        SRC.PKData,
        SRC.PKCliente,
        SRC.HistoricalHashKey,
        SRC.ChangeHashKey,
        SRC.HistoricalHashKeyASCII,
        SRC.ChangeHashKeyASCII,
        SRC.InsertDatetime,
        SRC.UpdateDatetime,
        SRC.IsDeleted,
        SRC.NumeroAccessi,
        SRC.NumeroPagineVisitate,
        SRC.PKCapoArea
      )

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        $action AS merge_action,
        'Staging.Accessi' AS full_olap_table_name,
        'PKData = ' + CAST(COALESCE(inserted.PKData, deleted.PKData) AS NVARCHAR(1000)) + ', PKCliente = ' + CAST(COALESCE(inserted.PKCliente, deleted.PKCliente) AS NVARCHAR(1000)) AS primary_key_description
    INTO audit.merge_log_details;

    --DELETE FROM Fact.Accessi
    --WHERE IsDeleted = CAST(1 AS BIT);

    UPDATE audit.tables
    SET lastupdated_local = lastupdated_staging
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    COMMIT TRANSACTION;

END;
GO

EXEC Fact.usp_Merge_Accessi;
GO

DROP TABLE IF EXISTS dbo.__ClienteCometa_id_sog_commerciale;
DROP TABLE IF EXISTS dbo.__ClienteCometa_id_sog_commerciale_fattura;
DROP SEQUENCE IF EXISTS dbo.seq_Dim_ClienteNEW;
DROP VIEW IF EXISTS Staging.ClienteCometaNEWView;
GO
