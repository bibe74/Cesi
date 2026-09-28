CREATE VIEW Staging.ClienteCometaView
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

