CREATE   VIEW Dim.ClienteCometaView
AS
WITH TableData
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
            CASE WHEN COALESCE(GA.CapoArea, N'') = N'' THEN COALESCE(PACA.CapoArea, N'') ELSE GA.CapoArea END,
            CC.IDProfessione,
            CC.Professione,
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
        COALESCE(PACA.Agente, N'') AS AgenteDefault,
        CASE WHEN COALESCE(GA.CapoArea, N'') = N'' THEN COALESCE(PACA.CapoArea, N'') ELSE GA.CapoArea END AS AgenteZoho,
        CC.IDProfessione,
        CC.Professione

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
    CAST(0 AS BIT) AS HasRoleMySolutionInterno,
    CAST(0 AS BIT) AS HasAbbonamentoMySolution,
    CAST(0 AS BIT) AS HasAbbonamentoMIA,
    TD.AgenteZoho,
    TD.IDProfessione,
    TD.Professione

FROM TableData TD;

GO

