
CREATE   VIEW Dim.ClienteNOPView
AS
WITH IscrizioniCorsi
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
            MSC.CreatedOnUtc,
            MSC.DateExpiration,
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
        LEFT(MSC.StreetAddress, 120) AS Indirizzo,
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
        COALESCE(MSC.CreatedOnUtc, CAST('19000101' AS DATE)) AS PKDataInizioContratto,
        COALESCE(MSC.DateExpiration, CAST('19000101' AS DATE)) AS PKDataFineContratto,
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
        MSC.HasRoleMySolutionInterno,
        COALESCE(PACA.CapoArea, N'') AS AgenteZoho

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
    TD.HasRoleMySolutionInterno,
    CAST(0 AS BIT) AS HasAbbonamentoMySolution,
    CAST(0 AS BIT) AS HasAbbonamentoMIA,
    TD.AgenteZoho,
    -1 AS IDProfessione,
    N'' AS Professione

FROM TableData TD;

GO

