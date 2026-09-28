
CREATE VIEW Staging.ClienteAccessiView
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

