/* Verifica email duplicate

Landing.COMETA_SoggettoCommerciale di tipo 'C': #12804 record
INNER JOIN con Landing.COMETA_Anagrafica: #12804 record
LEFT JOIN con Landing.COMETA_Telefono di tipo 'E' e descrizione 'ABBONATO', con un carattere @ in num_riferimento: #12818 record > TODO: verificare duplicati

SELECT
    SC.id_sog_commerciale,
    SC.codice,
    COALESCE(a.rag_soc_1, N'') + COALESCE(a.rag_soc_2, N'') AS RagioneSociale,
    a.indirizzo,
    a.cap,
    a.localita,
    a.provincia,
    a.nazione,
    a.cod_fiscale,
    a.par_iva,

    A.id_anagrafica,
    MIN(E.id_telefono) AS id_telefono_MIN,
    MAX(E.id_telefono) AS id_telefono_MAX,
    COUNT(1) AS RecordCount,
    MIN(E.num_riferimento) AS num_riferimento_MIN,
    MAX(E.num_riferimento) AS num_riferimento_MAX,
    COUNT(DISTINCT E.num_riferimento) AS num_riferimento_COUNTDISTINCT

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
GROUP BY SC.id_sog_commerciale,
    SC.codice,
    COALESCE(a.rag_soc_1, N'') + COALESCE(a.rag_soc_2, N''),
    a.indirizzo,
    a.cap,
    a.localita,
    a.provincia,
    a.nazione,
    a.cod_fiscale,
    a.par_iva,
    A.id_anagrafica
HAVING COUNT(1) > 1
ORDER BY SC.codice;
GO
*/

/* Verifica abbonamenti duplicati

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
)
SELECT
    AD.id_sog_commerciale,
    AD.id_documento,
    AD.num_documento,
    AD.data_documento,
    AD.data_inizio_contratto,
    AD.data_fine_contratto,
    AD.HasSconto,
    AD.rnDESC,
    CC.codice,
    CC.RagioneSociale,
    CC.Email,
    CC.nome,
    CC.cognome,
    CC.Quote

FROM AbbonamentiDettaglio AD
INNER JOIN Staging.CometaCustomer CC ON CC.id_sog_commerciale = AD.id_sog_commerciale
WHERE AD.id_sog_commerciale IN
(
8697,
9111,
9161,
14047,
14109,
14569,
16427,
16697,
17443
)
ORDER BY AD.id_sog_commerciale, AD.rnDESC;
GO

*/

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
        ROW_NUMBER() OVER (PARTITION BY SC.id_sog_commerciale ORDER BY CASE E.descrizione WHEN N'ABBONATO' THEN 0 WHEN N'PRINCIPALE' THEN 1 ELSE 2 END, E.id_telefono) AS rn

    FROM Landing.COMETA_SoggettoCommerciale SC
    INNER JOIN Landing.COMETA_Anagrafica A ON A.id_anagrafica = SC.id_anagrafica
        AND A.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Landing.COMETA_Telefono E ON E.id_anagrafica = A.id_anagrafica
        AND E.tipo = 'E'
        --AND E.descrizione = N'ABBONATO'
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
            CCRM12.Customer_Id,
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
        CASE WHEN CCRM12.Customer_Id IS NOT NULL THEN 1 ELSE 0 END AS HasRoleMySolutionInterno

    FROM Landing.MYSOLUTION_Customer C
    LEFT JOIN Landing.MYSOLUTION_CustomerAddresses CA ON CA.Customer_Id = C.Id
        AND CA.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Landing.MYSOLUTION_Address A ON A.Id = CA.Address_Id
        AND A.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Landing.MYSOLUTION_GenericAttribute GA1 ON GA1.EntityId = C.Id
        AND GA1.[Key] = N'Company'
        AND GA1.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Landing.MYSOLUTION_GenericAttribute GA2 ON GA2.EntityId = C.Id
        AND GA2.[Key] = N'CodiceFiscale'
        AND GA2.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Landing.MYSOLUTION_GenericAttribute GA3 ON GA3.EntityId = C.Id
        AND GA3.[Key] = N'VATNumber'
        AND GA3.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Landing.MYSOLUTION_GenericAttribute GA4 ON GA4.EntityId = C.Id
        AND GA4.[Key] = N'FirstName'
        AND GA4.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Landing.MYSOLUTION_GenericAttribute GA5 ON GA5.EntityId = C.Id
        AND GA5.[Key] = N'LastName'
        AND GA5.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Landing.MYSOLUTION_GenericAttribute GA6 ON GA6.EntityId = C.Id
        AND GA6.[Key] = N'StreetAddress'
        AND GA6.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Landing.MYSOLUTION_GenericAttribute GA7 ON GA7.EntityId = C.Id
        AND GA7.[Key] = N'ZipPostalCode'
        AND GA7.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Landing.MYSOLUTION_GenericAttribute GA8 ON GA8.EntityId = C.Id
        AND GA8.[Key] = N'Phone'
        AND GA8.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Landing.MYSOLUTION_GenericAttribute GA9 ON GA9.EntityId = C.Id
        AND GA9.[Key] = N'Cellulare'
        AND GA9.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Landing.MYSOLUTION_GenericAttribute GA10 ON GA10.EntityId = C.Id
        AND GA10.[Key] = N'City'
        AND GA10.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Landing.MYSOLUTION_GenericAttribute GA11 ON GA11.EntityId = C.Id
        AND GA11.[Key] = N'CountryId'
        AND GA11.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Landing.MYSOLUTION_Country CY ON CY.Id = GA11.Value
        AND CY.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Landing.MYSOLUTION_GenericAttribute GA12 ON GA12.EntityId = C.Id
        AND GA12.[Key] = N'StateProvinceId'
        AND GA12.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Landing.MYSOLUTION_StateProvince SP ON SP.Id = GA12.Value
        AND SP.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Landing.MYSOLUTION_Customer_CustomerRole_Mapping CCRM11 ON CCRM11.Customer_Id = C.Id
        AND CCRM11.CustomerRole_Id = 11 -- 11: MySolution.Demo
        AND CCRM11.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Landing.MYSOLUTION_Customer_CustomerRole_Mapping CCRM12 ON CCRM12.Customer_Id = C.Id
        AND CCRM12.CustomerRole_Id = 12 -- 12: MySolution.Interno
        AND CCRM12.IsDeleted = CAST(0 AS BIT)
    WHERE C.IsDeleted = CAST(0 AS BIT)
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
    TDD.HasRoleMySolutionInterno

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
    ALTER TABLE Staging.MySolutionCustomer ALTER COLUMN HasRoleMySolutionInterno BIT NOT NULL;

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
 * @table Dim.Cliente
 * @description 
*/

CREATE OR ALTER VIEW Dim.ClienteCometaView
AS
WITH 
----CapoAreaDefaultByCAP
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
----),
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
            --GA.CapoArea,
            PACA.CapoArea,
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
            ----CADBL.CapoAreaDefault,
            ----CADBCAP.CapoAreaDefault,
            ------PA.CapoArea,
            ----CADBL.AgenteDefault,
            ----CADBCAP.AgenteDefault,
            ----PA.Agente,
            PACA.Agente,
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
        ----COALESCE(GA.CapoArea, N'') AS Agente,
        COALESCE(PACA.CapoArea, N'') AS Agente,
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
        ----COALESCE(CADBL.CapoAreaDefault, CADBCAP.CapoAreaDefault, PA.CapoArea, N'') AS CapoAreaDefault,
        COALESCE(PACA.CapoArea, N'') AS CapoAreaDefault,
        --COALESCE(CADBL.AgenteDefault, CADBCAP.AgenteDefault, PA.Agente, N'') AS AgenteDefault,
        COALESCE(PACA.Agente, N'') AS AgenteDefault

    FROM Staging.CometaCustomer CC
    LEFT JOIN Import.Provincia P ON P.CodSiglaProvincia = CC.Provincia
    LEFT JOIN Dim.Data DIC ON DIC.PKData = CC.data_inizio_contratto
    LEFT JOIN Dim.Data DFC ON DFC.PKData = CC.data_fine_contratto
    LEFT JOIN Dim.Data DD ON DD.PKData = CC.data_disdetta
    LEFT JOIN Landing.COMETA_MySolutionUsers MSU ON MSU.EMail = CC.Email
        AND CC.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Dim.GruppoAgenti GA ON GA.id_gruppo_agenti = CC.id_gruppo_agenti
        AND GA.IsDeleted = CAST(0 AS BIT)
    ----LEFT JOIN Import.ProvinciaAgente PA ON PA.IDProvincia = CC.provincia
    ----LEFT JOIN CapoAreaDefaultByCAP CADBCAP ON CADBCAP.IDProvincia = CC.provincia AND CADBCAP.CAP = CC.cap
    ----LEFT JOIN CapoAreaDefaultByLocalita CADBL ON CADBL.IDProvincia = CC.provincia AND CADBL.Localita = CC.localita
    LEFT JOIN Import.ProvinciaAgenteCapoArea PACA ON PACA.IDProvincia = CC.provincia
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
    TD.AgenteDefault,
    CAST(0 AS BIT) AS HasRoleMySolutionDemo,
    CAST(0 AS BIT) AS HasRoleMySolutionInterno

FROM TableData TD;
GO

IF OBJECT_ID('dbo.seq_Dim_ClienteNEW', 'SO') IS NULL
BEGIN

    CREATE SEQUENCE dbo.seq_Dim_ClienteNEW START WITH 1;

END;
GO

--DROP TABLE IF EXISTS Dim.Cliente;
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
        IDAnagraficaCometa INT NULL,
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
        --IDSoggettoCommerciale_migrazione INT NULL,
        --IDSoggettoCommerciale_migrazione_old INT NULL,
        IDProvincia NVARCHAR(10) NOT NULL,
        IsClienteFormazione BIT NOT NULL,
        CapoAreaDefault NVARCHAR(60) NOT NULL,
        AgenteDefault NVARCHAR(60) NOT NULL,
        HasRoleMySolutionDemo BIT NOT NULL,
        HasRoleMySolutionInterno BIT NOT NULL
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
    ALTER TABLE Dim.Cliente ADD CONSTRAINT DFT_Dim_Cliente_PKDataInizioContratto DEFAULT (CAST('19000101' AS DATE)) FOR PKDataInizioContratto;
    ALTER TABLE Dim.Cliente ADD CONSTRAINT DFT_Dim_Cliente_PKDataFineContratto DEFAULT (CAST('19000101' AS DATE)) FOR PKDataFineContratto;
    ALTER TABLE Dim.Cliente ADD CONSTRAINT DFT_Dim_Cliente_PKDataDisdetta DEFAULT (CAST('19000101' AS DATE)) FOR PKDataDisdetta;
    ALTER TABLE Dim.Cliente ADD CONSTRAINT DFT_Dim_Cliente_PKGruppoAgenti DEFAULT (-1) FOR PKGruppoAgenti;
    ALTER TABLE Dim.Cliente ADD CONSTRAINT DFT_Dim_Cliente_IsAttivo DEFAULT (0) FOR IsAttivo;
    ALTER TABLE Dim.Cliente ADD CONSTRAINT DFT_Dim_Cliente_IsAbbonato DEFAULT (0) FOR IsAbbonato;
    ALTER TABLE Dim.Cliente ADD CONSTRAINT DFT_Dim_Cliente_IDProvincia DEFAULT (N'') FOR IDProvincia;
    ALTER TABLE Dim.Cliente ADD CONSTRAINT DFT_Dim_Cliente_IsClienteFormazione DEFAULT (0) FOR IsClienteFormazione;
    ALTER TABLE Dim.Cliente ADD CONSTRAINT DFT_Dim_Cliente_CapoAreaDefault DEFAULT (N'') FOR CapoAreaDefault;
    ALTER TABLE Dim.Cliente ADD CONSTRAINT DFT_Dim_Cliente_AgenteDefault DEFAULT (N'') FOR AgenteDefault;
    ALTER TABLE Dim.Cliente ADD CONSTRAINT DFT_Dim_Cliente_HasRoleMySolutionDemo DEFAULT (0) FOR HasRoleMySolutionDemo;
    ALTER TABLE Dim.Cliente ADD CONSTRAINT DFT_Dim_Cliente_HasRoleMySolutionInterno DEFAULT (0) FOR HasRoleMySolutionInterno;

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
        Nome,
        IDProvincia
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
        N'',       -- Nome - nvarchar(60)
        N''        -- IDProvincia - nvarchar(10)
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
        N'',       -- Nome - nvarchar(60)
        N''        -- IDProvincia - nvarchar(10)
    );

    ALTER SEQUENCE dbo.seq_Dim_Cliente RESTART WITH 1;

END;
GO

CREATE OR ALTER VIEW Dim.ClienteNOPView
AS
WITH 
----CapoAreaDefaultByCAP
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
----),
----TrascodificaCapiArea
----AS (
----    SELECT
----        N'MASSIMO LORI' AS CapoAreaDefault,
----        N'LORI MASSIMO' AS CapoArea
----),
----CapoAreaAgenteDettaglio
----AS (
----    SELECT
----        GA.CapoArea,
----        GA.PKGruppoAgenti,
----        GA.GruppoAgenti,
----        ROW_NUMBER() OVER (PARTITION BY GA.CapoArea ORDER BY CASE WHEN GA.GruppoAgenti LIKE N'%50 E 50' THEN 1 ELSE 0 END, GA.GruppoAgenti) AS rn

----    FROM Dim.GruppoAgenti GA
----    WHERE GA.IsDeleted = CAST(0 AS BIT)
----),
IscrizioniCorsi
AS (
    SELECT DISTINCT
        PartecipantEmail

    FROM Landing.MYSOLUTION_Courses
    WHERE IsDeleted = CAST(0 AS BIT)

    UNION

    SELECT DISTINCT
        RootPartecipantEmail

    FROM Landing.MYSOLUTION_Courses
    WHERE IsDeleted = CAST(0 AS BIT)
),
TableData
AS (
    SELECT
        -1000-MSC.Id AS IDSoggettoCommerciale,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            -1000-MSC.Id,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            MSC.Email,
            MSC.Company,
            MSC.CodiceFiscale,
            MSC.VATNumber,
            MSC.StreetAddress,
            MSC.ZipPostalCode,
            MSC.City,
            --MSC.StateProvinceId,
            MSC.StateProvince,
            P.DescrRegione,
            P.DescrMacroregione,
            --MSC.CountryId,
            MSC.Country,
            MSC.Phone,
            MSC.Cellulare,
            ----COALESCE(CADBL.AgenteDefault, CADBCAP.AgenteDefault, PA.Agente, N''),
            PACA.CapoArea,
            ----COALESCE(GA.PKGruppoAgenti, -1),
            MSC.LastName,
            MSC.FirstName,
            SP.Abbreviation,
            CASE WHEN IC.PartecipantEmail IS NOT NULL THEN 1 ELSE 0 END,
            ----COALESCE(CADBL.CapoAreaDefault, CADBCAP.CapoAreaDefault, PA.CapoArea, N''),
            PACA.Agente,
            MSC.HasRoleMySolutionDemo,
            MSC.Username,
            MSC.HasRoleMySolutionInterno,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,

        MSC.Email,
        NULL AS IDAnagraficaCometa,
        0 AS HasAnagraficaCometa,
        1 AS HasAnagraficaNopCommerce,
        0 AS HasAnagraficaMySolution,
        N'NOP' AS ProvenienzaAnagrafica,
        N'' AS CodiceCliente,
        'C' AS TipoSoggettoCommerciale,
        MSC.Company AS RagioneSociale,
        MSC.CodiceFiscale,
        LEFT(MSC.VATNumber, 20) AS PartitaIVA,
        MSC.StreetAddress AS Indirizzo,
        LEFT(MSC.ZipPostalCode, 10) AS CAP,
        MSC.City AS Localita,
        --MSC.StateProvinceId,
        COALESCE(MSC.StateProvince, N'') AS Provincia,
        COALESCE(P.DescrRegione, N'') AS Regione,
        COALESCE(P.DescrMacroregione, N'') AS MacroRegione,
        --MSC.CountryId,
        MSC.Country AS Nazione,
        MSC.Phone AS Telefono,
        MSC.Cellulare,
        N'' AS Fax,
        N'' AS TipoCliente,
        ----COALESCE(CADBL.AgenteDefault, CADBCAP.AgenteDefault, PA.Agente, N'') AS Agente,
        COALESCE(PACA.CapoArea, N'') AS Agente,
        CAST('19000101' AS DATE) AS PKDataInizioContratto,
        CAST('19000101' AS DATE) AS PKDataFineContratto,
        CAST('19000101' AS DATE) AS PKDataDisdetta,
        N'' AS MotivoDisdetta,
        ----COALESCE(GA.PKGruppoAgenti, -1) AS PKGruppoAgenti,
        -1 AS PKGruppoAgenti,
        MSC.LastName AS Cognome,
        MSC.FirstName AS Nome,
        0 AS IsAttivo,
        0 AS IsAbbonato,
        NULL AS IDSoggettoCommerciale_migrazione,
        NULL AS IDSoggettoCommerciale_migrazione_old,
        COALESCE(SP.Abbreviation, N'') AS IDProvincia,
        CASE WHEN IC.PartecipantEmail IS NOT NULL THEN 1 ELSE 0 END AS IsClienteFormazione,
        ----COALESCE(CADBL.CapoAreaDefault, CADBCAP.CapoAreaDefault, PA.CapoArea, N'') AS CapoAreaDefault,
        COALESCE(PACA.CapoArea, N'') AS CapoAreaDefault,
        ----COALESCE(CADBL.AgenteDefault, CADBCAP.AgenteDefault, PA.Agente, N'') AS AgenteDefault,
        COALESCE(PACA.CapoArea, N'') AS AgenteDefault,
        MSC.HasRoleMySolutionDemo,
        MSC.Username,
        --MSC.IdCometa,
        --MSC.rnCustomerDESC,
        N'TODO' AS id_sog_commerciale,
        MSC.HasRoleMySolutionInterno

    FROM Staging.MySolutionCustomer MSC
    LEFT JOIN Dim.Cliente CC ON CC.Email = MSC.Email
        AND CC.ProvenienzaAnagrafica = N'COMETA'
        AND CC.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN MYSOLUTION.StateProvince SP ON SP.Id = MSC.StateProvinceId
    LEFT JOIN Import.Provincia P ON P.CodSiglaProvincia = SP.Abbreviation
    ----LEFT JOIN Import.ProvinciaAgente PA ON PA.IDProvincia = SP.Abbreviation
    ----LEFT JOIN CapoAreaDefaultByCAP CADBCAP ON CADBCAP.IDProvincia = P.CodSiglaProvincia AND CADBCAP.CAP = MSC.ZipPostalCode
    ----LEFT JOIN CapoAreaDefaultByLocalita CADBL ON CADBL.IDProvincia = P.CodSiglaProvincia AND CADBL.Localita = MSC.City
    LEFT JOIN Import.ProvinciaAgenteCapoArea PACA ON PACA.IDProvincia = SP.Abbreviation
    LEFT JOIN IscrizioniCorsi IC ON IC.PartecipantEmail = MSC.Email
    ----LEFT JOIN TrascodificaCapiArea TCA ON TCA.CapoAreaDefault = COALESCE(CADBL.CapoAreaDefault, CADBCAP.CapoAreaDefault, PA.CapoArea, N'')
    ----LEFT JOIN CapoAreaAgenteDettaglio CAAD ON CAAD.CapoArea = COALESCE(TCA.CapoArea, CADBL.CapoAreaDefault, CADBCAP.CapoAreaDefault, PA.CapoArea, N'')
    ----    AND CAAD.rn = 1
    ----LEFT JOIN Dim.GruppoAgenti GA ON GA.PKGruppoAgenti = CAAD.PKGruppoAgenti
    ----    AND GA.IsDeleted = CAST(0 AS BIT)
    WHERE CC.PKCliente IS NULL
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
    TD.AgenteDefault,
    TD.HasRoleMySolutionDemo,
    TD.HasRoleMySolutionInterno

FROM TableData TD;
GO

CREATE OR ALTER PROCEDURE Dim.usp_Merge_Cliente
AS
BEGIN

    SET NOCOUNT ON;

    BEGIN TRANSACTION 

    DECLARE @provider_name NVARCHAR(60) = N'MyDatamartReporting';
    DECLARE @full_table_name sysname = N'Landing.COMETA_SoggettoCommerciale';

    -- Aggiornamento clienti NOP passati in COMETA
    TRUNCATE TABLE Staging.ClientiNOPInCometa;

    INSERT INTO Staging.ClientiNOPInCometa SELECT * FROM Staging.ClientiNOPInCometaView;

    UPDATE T
    SET T.PKCliente = CNIC.PKClienteCometa
    FROM Dim.Utente T
    INNER JOIN Staging.ClientiNOPInCometa CNIC ON CNIC.PKClienteNOP = T.PKCliente;

    UPDATE T
    SET T.PKCliente = CNIC.PKClienteCometa
    FROM Dim.ClienteAccessi T
    INNER JOIN Staging.ClientiNOPInCometa CNIC ON CNIC.PKClienteNOP = T.PKCliente;

    UPDATE T
    SET T.PKCliente = CNIC.PKClienteCometa
    FROM Fact.Accessi T
    INNER JOIN Staging.ClientiNOPInCometa CNIC ON CNIC.PKClienteNOP = T.PKCliente;

    UPDATE T
    SET T.PKCliente = CNIC.PKClienteCometa
    FROM Fact.AccessiUltimi3Mesi T
    INNER JOIN Staging.ClientiNOPInCometa CNIC ON CNIC.PKClienteNOP = T.PKCliente;

    UPDATE T
    SET T.PKCliente = CNIC.PKClienteCometa
    FROM Fact.Documenti T
    INNER JOIN Staging.ClientiNOPInCometa CNIC ON CNIC.PKClienteNOP = T.PKCliente;

    UPDATE T
    SET T.PKClienteFattura = CNIC.PKClienteCometa
    FROM Fact.Documenti T
    INNER JOIN Staging.ClientiNOPInCometa CNIC ON CNIC.PKClienteNOP = T.PKClienteFattura;

    UPDATE T
    SET T.PKCliente = CNIC.PKClienteCometa
    FROM Fact.Scadenze T
    INNER JOIN Staging.ClientiNOPInCometa CNIC ON CNIC.PKClienteNOP = T.PKCliente;

    DELETE C
    FROM Dim.Cliente C
    INNER JOIN Staging.ClientiNOPInCometa CNIC ON CNIC.PKClienteNOP = C.PKCliente;

    -- Verifica clienti non-COMETA passati in COMETA
    UPDATE C
    SET C.IDSoggettoCommerciale = CC.IDSoggettoCommerciale,
        C.ProvenienzaAnagrafica = CC.ProvenienzaAnagrafica

    FROM Dim.ClienteCometaView CC
    INNER JOIN Dim.Cliente C ON C.Email = CC.Email
        AND C.ProvenienzaAnagrafica <> N'COMETA'
        AND C.PKCliente > 0;

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
        TGT.HasRoleMySolutionDemo = SRC.HasRoleMySolutionDemo,
        TGT.HasRoleMySolutionInterno = SRC.HasRoleMySolutionInterno

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
        HasRoleMySolutionDemo,
        HasRoleMySolutionInterno
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
        SRC.HasRoleMySolutionDemo,
        SRC.HasRoleMySolutionInterno
      )

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        $action AS merge_action,
        'Dim.Cliente' AS full_olap_table_name,
        'IDSoggettoCommerciale = ' + CAST(COALESCE(inserted.IDSoggettoCommerciale, deleted.IDSoggettoCommerciale) AS NVARCHAR(1000)) AS primary_key_description
    INTO audit.merge_log_details;

    WITH TargetTable
    AS (
        SELECT
            *
        FROM Dim.Cliente
        WHERE IDSoggettoCommerciale < -1000
    )
    MERGE INTO TargetTable AS TGT
    USING Dim.ClienteNOPView (nolock) AS SRC
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
        TGT.HasRoleMySolutionDemo = SRC.HasRoleMySolutionDemo,
        TGT.HasRoleMySolutionInterno = SRC.HasRoleMySolutionInterno

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
        HasRoleMySolutionDemo,
        HasRoleMySolutionInterno
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
        SRC.HasRoleMySolutionDemo,
        SRC.HasRoleMySolutionInterno
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

CREATE OR ALTER VIEW Staging.ClientiNOPInCometaView
AS
WITH ClientiNOPInCometaDettaglio
AS (
    SELECT DISTINCT
        NOP.PKCliente AS PKClienteNOP,
        NOP.Email,
        COM.PKCliente AS PKClienteCometa

    FROM Dim.Cliente NOP
    INNER JOIN Dim.Cliente COM ON COM.Email = NOP.Email
        AND COM.ProvenienzaAnagrafica = N'COMETA'
        AND COM.IsDeleted = CAST(0 AS BIT)
    WHERE NOP.ProvenienzaAnagrafica = N'NOP'
        AND NOP.IsDeleted = CAST(0 AS BIT)
),
ClientiNOPInCometa
AS (
    SELECT
        CNICD.PKClienteNOP,
        CNICD.Email,
        CNICD.PKClienteCometa,
        ROW_NUMBER() OVER (PARTITION BY CNICD.PKClienteNOP ORDER BY CNICD.PKClienteCometa DESC) AS rn

    FROM ClientiNOPInCometaDettaglio CNICD
)
SELECT
    CNIC.PKClienteNOP,
    CNIC.Email,
    CNIC.PKClienteCometa

FROM ClientiNOPInCometa CNIC
WHERE CNIC.rn = 1;
GO

--DROP TABLE IF EXISTS Staging.ClientiNOPInCometa;
GO

IF OBJECT_ID('Staging.ClientiNOPInCometa', 'U') IS NULL
BEGIN

    SELECT TOP (0) * INTO Staging.ClientiNOPInCometa FROM Staging.ClientiNOPInCometaView;

    ALTER TABLE Staging.ClientiNOPInCometa ADD CONSTRAINT PK_Staging_ClientiNOPInCometa PRIMARY KEY CLUSTERED (PKClienteNOP);

END;
GO

TRUNCATE TABLE Staging.ClientiNOPInCometa;
GO

INSERT INTO Staging.ClientiNOPInCometa SELECT * FROM Staging.ClientiNOPInCometaView;
GO

EXEC Dim.usp_Merge_Cliente;
GO

-- OK fin qui

SELECT
    ProvenienzaAnagrafica,
    HasRoleMySolutionDemo,
    HasRoleMySolutionInterno,
    COUNT(1)
FROM Dim.Cliente
GROUP BY ProvenienzaAnagrafica,
    HasRoleMySolutionDemo,
    HasRoleMySolutionInterno
ORDER BY ProvenienzaAnagrafica,
    HasRoleMySolutionDemo,
    HasRoleMySolutionInterno;
GO

SELECT DISTINCT
       U.Email
FROM Fact.Corsi C
    LEFT JOIN Dim.Utente U
        ON U.PKUtente = C.PKUtente
    LEFT JOIN Dim.Cliente CL
        ON CL.Email = C.Utente
WHERE C.IsDeleted = 0
      AND C.ImportoTotaleOrdine > 0.0
      AND CL.CodiceCliente IS NULL;

SELECT * FROM Landing.COMETA_Telefono WHERE num_riferimento = N'a.menchinelli@sofat.it';

SELECT * FROM Dim.Cliente WHERE 


SELECT * FROM Dim.Utente WHERE Email = N'a.menchinelli@sofat.it';






SELECT
    MSC.StateProvinceId,
    MSC.StateProvince,
    P.CodSiglaProvincia,
    P.Provincia,
    COUNT(1)
FROM Staging.MySolutionCustomer MSC
WHERE P.CodSiglaProvincia IS NULL
GROUP BY MSC.StateProvinceId,
    MSC.StateProvince,
    P.CodSiglaProvincia,
    P.Provincia
ORDER BY MSC.StateProvince;


SELECT * FROM Import.Provincia ORDER BY DescrProvincia;

ALTER TABLE Import.Provincia ADD StateProvince NVARCHAR(100) NULL;
GO

WITH StateProvince
AS (
    SELECT
        *
    FROM MYSOLUTION.StateProvince
    WHERE CountryId = 46 -- 46: Italia
)

UPDATE Import.Provincia SET StateProvince = DescrProvincia WHERE StateProvince IS NULL;



EXEC Staging.usp_Reload_MySolutionCustomer;
GO
EXEC Staging.usp_Reload_CometaCustomer;
GO
EXEC Staging.usp_Reload_CometaVendor;
GO
EXEC Dim.usp_Merge_Cliente;
GO

/**
 * @storedprocedure Fact.usp_ReportFatturatoFormazioneDettaglioCorsiNEW
*/

CREATE OR ALTER PROCEDURE Fact.usp_ReportFatturatoFormazioneDettaglioCorsiNEW (
    @DataInizio DATE = NULL,
    @DataFine DATE = NULL,
    @CapoArea NVARCHAR(60) = NULL
)
AS
BEGIN

    SET NOCOUNT ON;

    IF (@DataInizio IS NULL) SELECT @DataInizio = DATEADD(DAY, -7, CONVERT(DATE, CURRENT_TIMESTAMP));
    IF (@DataFine IS NULL) SELECT @DataFine = DATEADD(DAY, 6, @DataInizio);

    --SELECT @DataInizio, @DataFine;

    WITH OrdinamentoCorsi
    AS (
        SELECT
            A.PKArticolo,
            SUM(D.ImportoTotale) AS ImportoTotale,
            ROW_NUMBER() OVER (ORDER BY SUM(D.ImportoTotale)) AS rn

        FROM Fact.Documenti D
        INNER JOIN Dim.Data DC ON DC.PKData = D.PKDataCompetenza
            AND DC.PKData BETWEEN @DataInizio AND @DataFine
        INNER JOIN Dim.Articolo A ON A.PKArticolo = D.PKArticolo
        INNER JOIN Dim.GruppoAgenti GAR ON GAR.PKGruppoAgenti = D.PKGruppoAgenti_Riga
            AND (
                @CapoArea IS NULL
                OR GAR.CapoArea = @CapoArea
            )
        WHERE D.IsProfiloValidoPerStatisticaFatturatoFormazione = CAST(1 AS BIT)
            AND D.IsDeleted = CAST(0 AS BIT)
        GROUP BY A.PKArticolo
    )
    SELECT
        @DataInizio AS DataInizio,
        @DataFine AS DataFine,
        A.PKArticolo,
        A.Codice AS CodiceCorso,
        A.CategoriaMaster,
        A.Descrizione AS DescrizioneCorso,
        CASE C.IsAbbonato WHEN CAST(1 AS BIT) THEN N'Abbonati' WHEN CAST(0 AS BIT) THEN N'Non abbonati' END AS IsAbbonato,
        COUNT(1) AS Quantita,
        SUM(D.ImportoTotale) AS ImportoTotale,
        COALESCE(OC.rn, 0) AS rn

    FROM Fact.Documenti D
    INNER JOIN Dim.Data DC ON DC.PKData = D.PKDataCompetenza
        AND DC.PKData BETWEEN @DataInizio AND @DataFine
    INNER JOIN Dim.Articolo A ON A.PKArticolo = D.PKArticolo
    INNER JOIN Dim.GruppoAgenti GAR ON GAR.PKGruppoAgenti = D.PKGruppoAgenti_Riga
        AND (
            @CapoArea IS NULL
            OR GAR.CapoArea = @CapoArea
        )
    --INNER JOIN Dim.Cliente C ON C.PKCliente = D.PKCliente
    INNER JOIN Landing.COMETA_Documento CD ON CD.id_documento = D.IDDocumento
    INNER JOIN Dim.ClienteNEW C ON C.IDSoggettoCommerciale = CD.id_sog_commerciale
    LEFT JOIN OrdinamentoCorsi OC ON OC.PKArticolo = D.PKArticolo
    WHERE D.IsProfiloValidoPerStatisticaFatturatoFormazione = CAST(1 AS BIT)
        AND D.IsDeleted = CAST(0 AS BIT)
    GROUP BY A.PKArticolo,
        A.Codice,
        A.CategoriaMaster,
        A.Descrizione,
        CASE C.IsAbbonato WHEN CAST(1 AS BIT) THEN N'Abbonati' WHEN CAST(0 AS BIT) THEN N'Non abbonati' END,
        OC.rn
    ORDER BY A.Codice,
        IsAbbonato;

END;
GO

GRANT EXECUTE ON Fact.usp_ReportFatturatoFormazioneDettaglioCorsiNEW TO cesidw_reader;
GO

EXEC Fact.usp_ReportFatturatoFormazioneDettaglioCorsiNEW @DataInizio = NULL, -- date
                                                      @DataFine = NULL, -- date
                                                      @CapoArea = NULL  -- nvarchar(60)
GO

CREATE OR ALTER PROCEDURE Fact.usp_ReportFatturatoFormazioneDettaglioNEW (
    @AnnoCorrente INT = NULL,
    @CapoAreaDefault NVARCHAR(60) = NULL,
    @AgenteDefault NVARCHAR(40) = NULL
)
AS
BEGIN

    SET NOCOUNT ON;

    DECLARE @CodiceEsercizioMasterCorrente NVARCHAR(10),
            @CodiceEsercizioMasterPrecedente NVARCHAR(10);

    SELECT @AnnoCorrente = YEAR(DATEADD(DAY, -1, CURRENT_TIMESTAMP));

    SELECT @CodiceEsercizioMasterCorrente = CONVERT(NVARCHAR(4), @AnnoCorrente) + N'/' + CONVERT(NVARCHAR(4), @AnnoCorrente + 1),
        @CodiceEsercizioMasterPrecedente = CONVERT(NVARCHAR(4), @AnnoCorrente - 1) + N'/' + CONVERT(NVARCHAR(4), @AnnoCorrente);

    WITH IscrizioniMaster
    AS (
        SELECT
            C.PKCliente,
            ACM.CategoriaMaster,
            ACM.CodiceEsercizioMaster AS CodiceEsercizio,
            MAX(D.PKDataDocumento) AS DataUltimaFattura,
            COUNT(1) AS NumeroIscritti,
            SUM(D.ImportoTotale * ACM.Percentuale) AS ImportoTotale

        FROM Fact.Documenti D
        INNER JOIN Landing.COMETA_Documento CD ON CD.id_documento = D.IDDocumento
        INNER JOIN Dim.ClienteNEW C ON C.IDSoggettoCommerciale = CD.id_sog_commerciale
        --INNER JOIN Dim.Cliente C ON C.PKCliente = D.PKCliente
            AND (
                @CapoAreaDefault IS NULL
                OR C.CapoAreaDefault = @CapoAreaDefault
            )
            AND (
                @AgenteDefault IS NULL
                OR C.AgenteDefault = @AgenteDefault
            )
        INNER JOIN Dim.Articolo A ON A.PKArticolo = D.PKArticolo
            AND A.CodiceEsercizioMaster IN (@CodiceEsercizioMasterCorrente, @CodiceEsercizioMasterPrecedente)
        INNER JOIN Import.ArticoloCategoriaMaster ACM ON ACM.id_articolo = A.id_articolo
        WHERE D.IDProfilo = N'ORDSEM'
            AND D.IsDeleted = CAST(0 AS BIT)
        GROUP BY C.PKCliente,
            ACM.CategoriaMaster,
            ACM.CodiceEsercizioMaster
    ),
    ClientiMaster
    AS (
        SELECT DISTINCT
            IM.PKCliente
        FROM IscrizioniMaster IM
    )
    SELECT
        C.PKCliente,
        C.CodiceCliente,
        C.RagioneSociale,
        C.IsAbbonato,
        C.CapoAreaDefault,
        C.AgenteDefault,
        C.Email,
        C.Telefono,
        C.Cellulare,
        @CodiceEsercizioMasterPrecedente AS CodiceEsercizioPrecedente,
        @CodiceEsercizioMasterCorrente AS CodiceEsercizioCorrente,
        COALESCE(CONVERT(NVARCHAR(10), IMAP.DataUltimaFattura, 103), N'') AS DataUltimaFatturaEsercizioPrecedente,
        IMAP.NumeroIscritti AS NumeroIscrittiAnnoPrecedente,
        IMAP.ImportoTotale AS ImportoTotaleAnnoPrecedente,
        COALESCE(CONVERT(NVARCHAR(10), IMAC.DataUltimaFattura, 103), N'') AS DataUltimaFatturaEsercizioCorrente,
        IMAC.NumeroIscritti AS NumeroIscrittiAnnoCorrente,
        IMAC.ImportoTotale AS ImportoTotaleAnnoCorrente,
        COALESCE(CONVERT(NVARCHAR(10), IMMAP.DataUltimaFattura, 103), N'') AS DataUltimaFatturaMiniMasterEsercizioPrecedente,
        IMMAP.NumeroIscritti AS NumeroIscrittiMiniMasterAnnoPrecedente,
        IMMAP.ImportoTotale AS ImportoTotaleMiniMasterAnnoPrecedente,
        COALESCE(CONVERT(NVARCHAR(10), IMMAC.DataUltimaFattura, 103), N'') AS DataUltimaFatturaMiniMasterEsercizioCorrente,
        IMMAC.NumeroIscritti AS NumeroIscrittiMiniMasterAnnoCorrente,
        IMMAC.ImportoTotale AS ImportoTotaleMiniMasterAnnoCorrente,
        COALESCE(CONVERT(NVARCHAR(2), MONTH(IMAP.DataUltimaFattura)) + N'. ' + DATENAME(MONTH, IMAP.DataUltimaFattura), N'') AS MeseUltimaFatturaEsercizioPrecedente,
        COALESCE(CONVERT(NVARCHAR(2), MONTH(IMAC.DataUltimaFattura)) + N'. ' + DATENAME(MONTH, IMAC.DataUltimaFattura), N'') AS MeseUltimaFatturaEsercizioCorrente,
        COALESCE(CONVERT(NVARCHAR(2), MONTH(IMMAP.DataUltimaFattura)) + N'. ' + DATENAME(MONTH, IMMAP.DataUltimaFattura), N'') AS MeseUltimaFatturaMiniMasterEsercizioPrecedente,
        COALESCE(CONVERT(NVARCHAR(2), MONTH(IMMAC.DataUltimaFattura)) + N'. ' + DATENAME(MONTH, IMMAC.DataUltimaFattura), N'') AS MeseUltimaFatturaMiniMasterEsercizioCorrente,
        C.Provincia

    FROM ClientiMaster CM
    INNER JOIN Dim.ClienteNEW C ON C.PKCliente = CM.PKCliente
    LEFT JOIN IscrizioniMaster IMAC ON IMAC.PKCliente = CM.PKCliente
        AND IMAC.CategoriaMaster = N'Master MySolution'
        AND IMAC.CodiceEsercizio = @CodiceEsercizioMasterCorrente
    LEFT JOIN IscrizioniMaster IMAP ON IMAP.PKCliente = CM.PKCliente
        AND IMAP.CategoriaMaster = N'Master MySolution'
        AND IMAP.CodiceEsercizio = @CodiceEsercizioMasterPrecedente
    LEFT JOIN IscrizioniMaster IMMAC ON IMMAC.PKCliente = CM.PKCliente
        AND IMMAC.CategoriaMaster = N'Mini Master Revisione'
        AND IMMAC.CodiceEsercizio = @CodiceEsercizioMasterCorrente
    LEFT JOIN IscrizioniMaster IMMAP ON IMMAP.PKCliente = CM.PKCliente
        AND IMMAP.CategoriaMaster = N'Mini Master Revisione'
        AND IMMAP.CodiceEsercizio = @CodiceEsercizioMasterPrecedente

    ORDER BY C.CodiceCliente;

END;
GO


SELECT * FROM Dim.ClienteNEW WHERE PKCliente = 9068;



SELECT
    InsertDatetime,
    UpdateDatetime,
    COUNT(1)
FROM Dim.ClienteNEW
GROUP BY InsertDatetime,
    UpdateDatetime
ORDER BY InsertDatetime,
    UpdateDatetime;
GO


EXEC Fact.usp_ReportFatturatoFormazioneDettaglioCorsi
    @DataInizio = NULL, -- date
    @DataFine = NULL,   -- date
    @CapoArea = NULL             -- nvarchar(60)






select distinct
a.[id_anagrafica],
s.codice,
isNull(a.rag_soc_1, '') + IsNull(a.rag_soc_2, '') as RagioneSociale,
a.indirizzo,
a.cap,
a.localita,
a.provincia,
a.nazione,
a.cod_fiscale,
a.par_iva,??
IsNull(t.num_riferimento, '[DA INSERIRE]') as EMail,
d.num_progressivo,
d.num_documento,
d.data_documento,
d.data_inizio_contratto,
dateAdd( d, 1, d.data_fine_contratto) as data_fine_contratto,
cast(1 as bit) as HaSconto,
t.nome as Nome,
t.cognome as Cognome,
IsNull(t.ruolo, 1) as Quote,
IsNull(t.descrizione, '') as telefono_descrizione,
IsNull(t.id_telefono, '') as id_telefono,
IsNull(s.id_sog_commerciale, '') as id_sog_commerciale,
trans.tipo,
d.id_documento

from COMETA_documento d
inner join COMETA_sog_commerciale s
on s.id_sog_commerciale = d.id_sog_commerciale
inner join COMETA_anagrafica a
on a.id_anagrafica= s.id_anagrafica
inner join COMETA_riga_documento r
on r.id_documento = d.id_documento
inner join cometa_articolo art
on art.id_articolo = r.id_articolo
inner join [COMETA_idArticolo_MySolution_transcodifica] trans
on art.codice = trans.codice
left join COMETA_telefono t
on a.id_anagrafica = t.id_anagrafica
and t.tipo = 'E'
and t.descrizione = 'ABBONATO'

where data_inizio_contratto is not null
and getDate() between data_inizio_contratto and dateAdd(d,1, data_fine_contratto) -- contratto (cioè fattura) in corso di validità
and s.tipo =  'C' -- cliente
and d.id_prof_documento in( 1,43) -- ordine cliente

GO


CREATE OR ALTER VIEW Staging.ClienteMySolutionNEWView
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



DROP TABLE IF EXISTS #Clienti;
GO

SELECT 
    U.Email,
    N'MYSOLUTION' AS Provenienza,
    CAST(0 AS BIT) AS IsAbbonato

INTO #Clienti

FROM Landing.MYSOLUTION_Users U
WHERE U.Email IS NOT NULL
GROUP BY U.Email
HAVING COUNT(1) = 1;
GO

SELECT * FROM #Clienti;

SELECT
    SC.id_sog_commerciale,
    SC.codice,
    A.*

FROM Landing.COMETA_SoggettoCommerciale SC
INNER JOIN Landing.COMETA_Anagrafica A ON A.id_anagrafica = SC.id_anagrafica
WHERE SC.tipo = 'C'

SELECT *  FROM [CesiDW].[Dim].[Cliente]  where email like '%a.bellinzona@fioribellinzona.it%'

SELECT *
FROM Fact.Corsi C

-- Corsi a pagamento

SELECT DISTINCT
       cl.ProvenienzaAnagrafica,
       c.OrderItemId,
       c.Partecipant_Id,
       c.Utente,
       c.EmailPartecipanteRoot,
       c.NumeroOrdine,
       c.ImportoTotaleOrdine,
       c.IDCorso,
       c.TipoCorso,
       c.Corso,
       c.CognomePartecipante,
       c.NomePartecipante,
       c.EmailPartecipante,
       c.UpdateDatetime
FROM Fact.Corsi c
LEFT JOIN Dim.Cliente cl ON cl.Email = c.Utente -- oppure EmailPartecipanteRoot ma sono quasi uguali

WHERE c.ImportoTotaleOrdine > 0
ORDER BY cl.ProvenienzaAnagrafica,
         c.Utente;

SELECT DISTINCT
       c.Utente
       
FROM Fact.Corsi c
LEFT JOIN Dim.Cliente cl ON cl.Email = c.Utente -- oppure EmailPartecipanteRoot ma sono quasi uguali

WHERE c.ImportoTotaleOrdine > 0
    AND cl.ProvenienzaAnagrafica IS NULL;
ORDER BY cl.ProvenienzaAnagrafica,
         c.Utente;


CREATE OR ALTER VIEW Staging.ClienteCometaNEWView
AS
WITH AnagraficaEmailDettaglio
AS (
    SELECT
        T.id_anagrafica,
        T.num_riferimento AS Email,
        T.descrizione,
        CAST(CASE WHEN T.descrizione = N'ABBONATO' THEN 1 ELSE 0 END AS BIT) AS IsAbbonato,
        ROW_NUMBER() OVER (PARTITION BY T.id_anagrafica ORDER BY CASE WHEN T.descrizione = N'ABBONATO' THEN 1 ELSE 0 END DESC, COALESCE(T.num_riferimento, N'ZZZ')) AS rn

    FROM Landing.COMETA_Telefono T
    INNER JOIN Landing.COMETA_SoggettoCommerciale SC ON SC.id_anagrafica = T.id_anagrafica
        AND SC.tipo = 'C'
    WHERE T.tipo = N'E'
        AND T.num_riferimento LIKE N'%@%'
        AND T.IsDeleted = CAST(0 AS BIT)
)
SELECT
    SC.id_sog_commerciale AS IDSoggettoCommerciale,
    AED.EMail,
    AED.IsAbbonato,
    A.id_anagrafica AS IDAnagraficaCometa,
    CAST(1 AS BIT) AS HasAnagraficaCometa,
    CONVERT(NVARCHAR(20), N'COMETA') AS ProvenienzaAnagrafica,
    SC.codice AS CodiceCliente,
    SC.tipo AS TipoSoggettoCommerciale,
    RTRIM(LTRIM(RTRIM(LTRIM(COALESCE(A.rag_soc_1, N'')) + N' ' + RTRIM(LTRIM(COALESCE(A.rag_soc_2, N'')))))) AS RagioneSociale,
    COALESCE(A.cod_fiscale, N'') AS CodiceFiscale,
    COALESCE(A.par_iva, N'') AS PartitaIVA,
    RTRIM(LTRIM(RTRIM(LTRIM(COALESCE(A.indirizzo, N'')) + N' ' + RTRIM(LTRIM(COALESCE(A.indirizzo2, N'')))))) AS Indirizzo,
    COALESCE(A.cap, N'') AS CAP,
    COALESCE(A.localita, N'') AS Localita,
    COALESCE(A.provincia, N'') AS IDProvincia,
    COALESCE(P.DescrProvincia, CASE WHEN COALESCE(A.provincia, N'') = N'' THEN N'' ELSE N'<???>' END) AS Provincia,
    COALESCE(P.DescrRegione, CASE WHEN COALESCE(A.provincia, N'') = N'' THEN N'' ELSE N'<???>' END) AS Regione,
    COALESCE(P.DescrMacroregione, CASE WHEN COALESCE(A.provincia, N'') = N'' THEN N'' ELSE N'<???>' END) AS Macroregione,
    COALESCE(P.DescrNazione, A.nazione, N'') AS Nazione,

            --COALESCE(MSU.TipoCliente, N'') AS TipoCliente,
            COALESCE(GA.CapoArea, N'') AS Agente,
            --COALESCE(MSU.PKDataInizioContratto, CAST('19000101' AS DATE)) AS PKDataInizioContratto,
            --COALESCE(MSU.PKDataFineContratto, CAST('19000101' AS DATE)) AS PKDataFineContratto,
            --COALESCE(DD.PKData, CAST('19000101' AS DATE)) AS PKDataDisdetta,
            --COALESCE(D.motivo_disdetta, N'') AS MotivoDisdetta,
            COALESCE(GA.PKGruppoAgenti, -1) AS PKGruppoAgenti,
            --COALESCE(MSU.Cognome, N'') AS Cognome,
            --COALESCE(MSU.Nome, N'') AS Nome,
            --COALESCE(TDT.num_riferimento, N'') AS Telefono,
            --COALESCE(TDC.num_riferimento, N'') AS Cellulare,
            --COALESCE(TDF.num_riferimento, N'') AS Fax,
            --CASE WHEN MSU.Email IS NOT NULL THEN 1 ELSE 0 END AS IsAbbonato,
            --COALESCE(NCD.HasRoleMySolutionDemo, 0) AS HasRoleMySolutionDemo,

            1 AS todo

FROM Landing.COMETA_SoggettoCommerciale SC
INNER JOIN Landing.COMETA_Anagrafica A ON A.id_anagrafica = SC.id_anagrafica
LEFT JOIN AnagraficaEmailDettaglio AED ON AED.id_anagrafica = SC.id_anagrafica
    AND AED.rn = 1
LEFT JOIN Dim.GruppoAgenti GA ON GA.id_gruppo_agenti = SC.id_gruppo_agenti
LEFT JOIN Import.Provincia P ON P.CodSiglaProvincia = A.provincia
WHERE SC.tipo = 'C';
GO

SELECT * FROM Staging.ClienteCometaNEWView;
GO

SELECT
    C.EMail

FROM Staging.ClienteCometaNEWView C
WHERE C.IsAbbonato = CAST(1 AS BIT)
GROUP BY C.EMail
HAVING COUNT(1) > 1
ORDER BY C.EMail;
GO
