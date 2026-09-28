CREATE VIEW Staging.ClienteNopCommerceView
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

