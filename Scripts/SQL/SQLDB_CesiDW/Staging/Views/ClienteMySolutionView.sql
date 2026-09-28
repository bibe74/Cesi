CREATE VIEW Staging.ClienteMySolutionView
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

