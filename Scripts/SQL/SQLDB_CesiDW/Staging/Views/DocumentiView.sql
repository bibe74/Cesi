CREATE VIEW Staging.DocumentiView
AS
WITH TableData
AS (
    SELECT
        DR.id_riga_documento AS IDDocumento_Riga,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            DR.id_riga_documento,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            D.id_documento,
            --D.id_prof_documento,
            PD.codice,
            PD.descrizione,
            --D.id_registro,
            R.tipo_registro,
            R.numero,
            R.descrizione,
            --R.id_esercizio,
            E.codice,
            DIE.PKData,
            DFE.PKData,
            DReg.PKData,
            D.num_documento,
            DD.PKData,
            DIC.PKData,
            DC.PKData,
            --D.id_sog_commerciale,
            ----DSC.tipo,
            ----DSC.descr_sog_com,
            C.TipoSoggettoCommerciale,
            --A.id_anagrafica,
            C.PKCliente,
            --D.id_sog_commerciale_fattura,
            ----DSCF.tipo,
            ----DSCF.descr_sog_com,
            CF.TipoSoggettoCommerciale,
            --AF.id_anagrafica,
            CF.PKCliente,
            --D.id_gruppo_agenti,
            GA.PKGruppoAgenti,
            GA.PKCapoArea,
            --D.data_fine_contratto,
            DFC.PKData,
            D.libero_4,
            --D.data_inizio_contratto,
            DIC.PKData,
            --D.id_libero_1,
            L1.codice,
            L1.descrizione,
            --D.id_libero_2,
            L2.codice,
            L2.descrizione,
            --D.id_libero_3,
            L3.codice,
            L3.descrizione,
            --D.id_tipo_fatturazione,
            TF.codice,
            TF.descrizione,
            GAR.PKGruppoAgenti,
            GAR.PKCapoArea,
            DR.num_riga,
            --DR.id_articolo,
            ART.PKArticolo,
            --DR.descrizione,
            DR.totale_riga,
            --D.id_con_pagamento,
            CP.codice,
            CP.descrizione,
            D.rinnovo_automatico,
            D.note_intestazione,
            IPD.IsProfiloValidoPerStatisticaFatturato,
            IPD.IsProfiloValidoPerStatisticaFatturatoFormazione,
            MT.PKMacroTipologia,
            DR.provv_calcolata_carea,
            DR.provv_calcolata_agente,
            DR.provv_calcolata_subagente,
            MSC.num_progressivo,
			MSC.Quote,
            DR.id_riga_doc_provenienza,
            D.note_decisionali,
            --D.data_disdetta,
            DDD.PKData,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,

        -- COMETA_Documento
        D.id_documento AS IDDocumento,
        --D.id_prof_documento,
        PD.codice AS IDProfilo,
        PD.descrizione AS Profilo,
        --D.id_registro,
        COALESCE(R.tipo_registro, CASE WHEN D.id_registro IS NULL THEN N'' ELSE N'???' END) AS TipoRegistro,
        COALESCE(R.numero, CASE WHEN D.id_registro IS NULL THEN -1 ELSE -101 END) AS NumeroRegistro,
        COALESCE(R.descrizione, CASE WHEN D.id_registro IS NULL THEN N'' ELSE N'<???>' END) AS Registro,
        --R.id_esercizio,
        COALESCE(E.codice, CASE WHEN R.id_esercizio IS NULL THEN N'' ELSE N'???' END) AS CodiceEsercizio,
        COALESCE(DIE.PKData, CAST('19000101' AS DATE)) AS PKDataInizioEsercizio,
        COALESCE(DFE.PKData, CAST('19000101' AS DATE)) AS PKDataFineEsercizio,
        --D.data_registrazione,
        COALESCE(DReg.PKData, CAST('19000101' AS DATE)) AS PKDataRegistrazione,
        COALESCE(D.num_documento, N'') AS NumeroDocumento,
        --D.data_documento,
        COALESCE(DD.PKData, DIC.PKData, CAST('19000101' AS DATE)) AS PKDataDocumento,
        --D.data_competenza,
        COALESCE(DC.PKData, CAST('19000101' AS DATE)) AS PKDataCompetenza,
        --D.id_sog_commerciale,
        ----DSC.tipo AS IDTipoSoggettoCommerciale,
        ----DSC.descr_sog_com AS TipoSoggettoCommerciale,
        C.TipoSoggettoCommerciale,
        --A.id_anagrafica,
        C.PKCliente,
        --D.id_sog_commerciale_fattura,
        ----DSCF.tipo AS IDTipoSoggettoCommercialeFattura,
        ----DSCF.descr_sog_com AS TipoSoggettoCommercialeFattura,
        CF.TipoSoggettoCommerciale AS TipoSoggettoCommercialeFattura,
        --AF.id_anagrafica,
        CF.PKCliente AS PKClienteFattura,
        --D.id_gruppo_agenti,
        COALESCE(GA.PKGruppoAgenti, CASE WHEN D.id_gruppo_agenti IS NULL THEN -1 ELSE -101 END) AS PKGruppoAgenti,
        COALESCE(GA.PKCapoArea, CASE WHEN D.id_gruppo_agenti IS NULL THEN -1 ELSE -101 END) AS PKCapoArea,
        --D.data_fine_contratto,
        COALESCE(DFC.PKData, CAST('19000101' AS DATE)) AS PKDataFineContratto,
        COALESCE(D.libero_4, N'') AS Libero4,
        TRY_CAST(D.libero_4 AS INT) AS IDDocumentoRinnovato,
        --D.data_inizio_contratto,
        COALESCE(DIC.PKData, CAST('19000101' AS DATE)) AS PKDataInizioContratto,
        --D.id_libero_1,
        COALESCE(L1.codice, CASE WHEN D.id_libero_1 IS NULL THEN N'' ELSE N'???' END) AS IDLibero1,
        COALESCE(L1.descrizione, CASE WHEN D.id_libero_1 IS NULL THEN N'' ELSE N'<???>' END) AS Libero1,
        --D.id_libero_2,
        COALESCE(L2.codice, CASE WHEN D.id_libero_2 IS NULL THEN N'' ELSE N'???' END) AS IDLibero2,
        COALESCE(L2.descrizione, CASE WHEN D.id_libero_2 IS NULL THEN N'' ELSE N'<???>' END) AS Libero2,
        --D.id_libero_3,
        COALESCE(L3.codice, CASE WHEN D.id_libero_3 IS NULL THEN N'' ELSE N'???' END) AS IDLibero3,
        COALESCE(L3.descrizione, CASE WHEN D.id_libero_3 IS NULL THEN N'' ELSE N'<???>' END) AS Libero3,
        --D.id_tipo_fatturazione,
        COALESCE(TF.codice, CASE WHEN D.id_tipo_fatturazione IS NULL THEN N'' ELSE N'???' END) AS IDTipoFatturazione,
        COALESCE(TF.descrizione, CASE WHEN D.id_tipo_fatturazione IS NULL THEN N'' ELSE N'<???>' END) AS TipoFatturazione,

        -- COMETA_Documento_Riga
        --DR.id_gruppo_agenti,
        COALESCE(GAR.PKGruppoAgenti, CASE WHEN DR.id_gruppo_agenti IS NULL THEN -1 ELSE -101 END) AS PKGruppoAgenti_Riga,
        COALESCE(GAR.PKCapoArea, CASE WHEN DR.id_gruppo_agenti IS NULL THEN -1 ELSE -101 END) AS PKCapoArea_Riga,
        DR.num_riga AS NumeroRiga,
        --DR.id_articolo,
        COALESCE(ART.PKArticolo, CASE WHEN COALESCE(DR.id_articolo, 0) = 0 THEN -1 ELSE -101 END) AS PKArticolo,
        --DR.descrizione,
        COALESCE(DR.totale_riga, 0.0) AS ImportoTotale,

        D.id_con_pagamento,
        COALESCE(CP.codice, N'') AS CodiceCondizioniPagamento,
        COALESCE(CP.descrizione, N'') AS CondizioniPagamento,
        COALESCE(D.rinnovo_automatico, '') AS RinnovoAutomatico,
        COALESCE(D.note_intestazione, N'') AS NoteIntestazione,
        COALESCE(IPD.IsProfiloValidoPerStatisticaFatturato, 0) AS IsProfiloValidoPerStatisticaFatturato,
        COALESCE(IPD.IsProfiloValidoPerStatisticaFatturatoFormazione, 0) AS IsProfiloValidoPerStatisticaFatturatoFormazione,
        COALESCE(MT.PKMacroTipologia, -1) AS PKMacroTipologia,

        ROW_NUMBER() OVER (PARTITION BY DR.id_riga_documento ORDER BY D.id_documento) AS rn,

        COALESCE(DR.provv_calcolata_carea, 0.0) AS ImportoProvvigioneCapoArea,
        COALESCE(DR.provv_calcolata_agente, 0.0) AS ImportoProvvigioneAgente,
        COALESCE(DR.provv_calcolata_subagente, 0.0) AS ImportoProvvigioneSubagente,
        COALESCE(MSC.num_progressivo, 0) AS Progressivo,
		COALESCE(MSC.Quote, 0) AS Quote,
        DR.id_riga_doc_provenienza AS IDDocumento_Riga_Provenienza,
        CASE WHEN D.IsDeleted = CAST(0 AS BIT) AND DR.IsDeleted = CAST(0 AS BIT) THEN 0 ELSE 1 END AS IsDeleted,
        COALESCE(D.note_decisionali, N'') AS NoteDecisionali,
        --D.data_disdetta,
        COALESCE(DDD.PKData, CAST('19000101' AS DATE)) AS PKDataDisdetta

    FROM Landing.COMETA_Documento_Riga DR
    INNER JOIN Landing.COMETA_Documento D ON D.id_documento = DR.id_documento
        AND D.IsDeleted = CAST(0 AS BIT)
    INNER JOIN Landing.COMETA_Profilo_Documento PD ON PD.id_prof_documento = D.id_prof_documento
        AND PD.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Landing.COMETA_Registro R ON R.id_registro = D.id_registro
        AND R.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Landing.COMETA_Esercizio E ON E.id_esercizio = R.id_esercizio
        AND E.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Dim.Data DIE ON DIE.PKData = E.data_inizio
    LEFT JOIN Dim.Data DFE ON DFE.PKData = E.data_fine
    LEFT JOIN Dim.Data DReg ON DReg.PKData = D.data_registrazione
    LEFT JOIN Dim.Data DD ON DD.PKData = D.data_documento
    LEFT JOIN Dim.Data DC ON DC.PKData = D.data_competenza
    INNER JOIN Dim.Cliente C ON C.IDSoggettoCommerciale = D.id_sog_commerciale
        AND C.IsDeleted = CAST(0 AS BIT)
    INNER JOIN Dim.Cliente CF ON CF.IDSoggettoCommerciale = D.id_sog_commerciale
        AND CF.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Dim.GruppoAgenti GA ON GA.id_gruppo_agenti = D.id_gruppo_agenti
        AND GA.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Dim.Data DFC ON DFC.PKData = D.data_fine_contratto
    LEFT JOIN Dim.Data DIC ON DIC.PKData = D.data_inizio_contratto
    LEFT JOIN Landing.COMETA_Libero_1 L1 ON L1.id_libero_1 = D.id_libero_1
        AND L1.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Landing.COMETA_Libero_2 L2 ON L2.id_libero_2 = D.id_libero_2
        AND L2.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Landing.COMETA_Libero_3 L3 ON L3.id_libero_3 = D.id_libero_3
        AND L3.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Landing.COMETA_Tipo_Fatturazione TF ON TF.id_tipo_fatturazione = D.id_tipo_fatturazione
        AND TF.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Dim.GruppoAgenti GAR ON GAR.id_gruppo_agenti = DR.id_gruppo_agenti
        AND GAR.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Dim.Articolo ART ON ART.id_articolo = DR.id_articolo
        AND ART.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Landing.COMETA_CondizioniPagamento CP ON CP.id_con_pagamento = D.id_con_pagamento
        AND CP.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Import.ProfiliDocumento IPD ON IPD.id_prof_documento = D.id_prof_documento
    LEFT JOIN Import.Libero2MacroTipologia L2MT ON L2MT.IDLibero2 = L2.codice
    LEFT JOIN Dim.MacroTipologia MT ON MT.MacroTipologia = L2MT.MacroTipologia
        AND MT.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Landing.COMETA_MySolutionContracts MSC ON MSC.id_riga_documento = DR.id_riga_documento
        AND MSC.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Dim.Data DDD ON DDD.PKData = D.data_disdetta
    WHERE DR.IsDeleted = CAST(0 AS BIT)
)
SELECT
    -- Chiavi
    TD.IDDocumento_Riga,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    --CAST(0 AS BIT) AS IsDeleted,
    TD.IsDeleted,

    -- Attributi
    TD.IDDocumento,
    TD.IDProfilo,
    TD.Profilo,
    TD.TipoRegistro,
    TD.NumeroRegistro,
    TD.Registro,
    TD.CodiceEsercizio,
    TD.PKDataInizioEsercizio,
    TD.PKDataFineEsercizio,
    TD.PKDataRegistrazione,
    TD.NumeroDocumento,
    TD.PKDataDocumento,
    TD.PKDataCompetenza,
    TD.TipoSoggettoCommerciale,
    TD.PKCliente,
    TD.TipoSoggettoCommercialeFattura,
    TD.PKClienteFattura,
    TD.PKGruppoAgenti,
    TD.PKCapoArea,
    TD.PKDataFineContratto,
    TD.Libero4,
    TD.PKDataInizioContratto,
    TD.IDLibero1,
    TD.Libero1,
    TD.IDLibero2,
    TD.Libero2,
    TD.IDLibero3,
    TD.Libero3,
    TD.IDTipoFatturazione,
    TD.TipoFatturazione,
    TD.PKGruppoAgenti_Riga,
    TD.PKCapoArea_Riga,
    TD.NumeroRiga,
    TD.PKArticolo,
    TD.CodiceCondizioniPagamento,
    TD.CondizioniPagamento,
    TD.RinnovoAutomatico,
    TD.NoteIntestazione,
    TD.IsProfiloValidoPerStatisticaFatturato,
    TD.IsProfiloValidoPerStatisticaFatturatoFormazione,
    TD.PKMacroTipologia,

    -- Misure
    TD.ImportoTotale,
    TD.ImportoProvvigioneCapoArea,
    TD.ImportoProvvigioneAgente,
    TD.ImportoProvvigioneSubagente,

    TD.Progressivo,
	TD.Quote,
    TD.IDDocumento_Riga_Provenienza,
    TD.NoteDecisionali,
    TD.PKDataDisdetta,
    DR.id_documento AS IDDocumentoRinnovato

FROM TableData TD
LEFT JOIN Landing.COMETA_Documento DR ON DR.id_documento = TD.IDDocumentoRinnovato
WHERE TD.rn = 1;

GO

