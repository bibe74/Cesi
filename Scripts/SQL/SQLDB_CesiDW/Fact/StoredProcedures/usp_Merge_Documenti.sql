
CREATE PROCEDURE Fact.usp_Merge_Documenti
AS
BEGIN

    SET NOCOUNT ON;

    BEGIN TRANSACTION 

    DECLARE @provider_name NVARCHAR(60) = N'MyDatamartReporting';
    DECLARE @full_table_name sysname = N'Landing.COMETA_Documento_Riga';

    MERGE INTO Fact.Documenti AS TGT
    USING Staging.Documenti (nolock) AS SRC
    ON SRC.IDDocumento_Riga = TGT.IDDocumento_Riga

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.IsDeleted = SRC.IsDeleted,
        
        TGT.PKDataInizioEsercizio = SRC.PKDataInizioEsercizio,
        TGT.PKDataFineEsercizio = SRC.PKDataFineEsercizio,
        TGT.PKDataRegistrazione = SRC.PKDataRegistrazione,
        TGT.PKDataDocumento = SRC.PKDataDocumento,
        TGT.PKDataCompetenza = SRC.PKDataCompetenza,
        TGT.PKCliente = SRC.PKCliente,
        TGT.PKClienteFattura = SRC.PKClienteFattura,
        TGT.PKGruppoAgenti = SRC.PKGruppoAgenti,
        TGT.PKCapoArea = SRC.PKCapoArea,
        TGT.PKDataFineContratto = SRC.PKDataFineContratto,
        TGT.PKDataInizioContratto = SRC.PKDataInizioContratto,
        TGT.PKGruppoAgenti_Riga = SRC.PKGruppoAgenti_Riga,
        TGT.PKCapoArea_Riga = SRC.PKCapoArea_Riga,
        TGT.PKArticolo = SRC.PKArticolo,
        TGT.PKMacroTipologia = SRC.PKMacroTipologia,

        TGT.IDDocumento = SRC.IDDocumento,
        TGT.IDProfilo = SRC.IDProfilo,
        TGT.Profilo = SRC.Profilo,
        TGT.TipoRegistro = SRC.TipoRegistro,
        TGT.NumeroRegistro = SRC.NumeroRegistro,
        TGT.Registro = SRC.Registro,
        TGT.CodiceEsercizio = SRC.CodiceEsercizio,
        TGT.NumeroDocumento = SRC.NumeroDocumento,
        TGT.TipoSoggettoCommerciale = SRC.TipoSoggettoCommerciale,
        TGT.TipoSoggettoCommercialeFattura = SRC.TipoSoggettoCommercialeFattura,
        TGT.Libero4 = SRC.Libero4,
        TGT.IDLibero1 = SRC.IDLibero1,
        TGT.Libero1 = SRC.Libero1,
        TGT.IDLibero2 = SRC.IDLibero2,
        TGT.Libero2 = SRC.Libero2,
        TGT.IDLibero3 = SRC.IDLibero3,
        TGT.Libero3 = SRC.Libero3,
        TGT.IDTipoFatturazione = SRC.IDTipoFatturazione,
        TGT.TipoFatturazione = SRC.TipoFatturazione,
        TGT.NumeroRiga = SRC.NumeroRiga,
        TGT.CodiceCondizioniPagamento = SRC.CodiceCondizioniPagamento,
        TGT.CondizioniPagamento = SRC.CondizioniPagamento,
        TGT.RinnovoAutomatico = SRC.RinnovoAutomatico,
        TGT.NoteIntestazione = SRC.NoteIntestazione,
        TGT.IsProfiloValidoPerStatisticaFatturato = SRC.IsProfiloValidoPerStatisticaFatturato,
        TGT.IsProfiloValidoPerStatisticaFatturatoFormazione = SRC.IsProfiloValidoPerStatisticaFatturatoFormazione,

        TGT.ImportoTotale = SRC.ImportoTotale,
        TGT.ImportoProvvigioneCapoArea = SRC.ImportoProvvigioneCapoArea,
        TGT.ImportoProvvigioneAgente = SRC.ImportoProvvigioneAgente,
        TGT.ImportoProvvigioneSubagente = SRC.ImportoProvvigioneSubagente,
        TGT.Progressivo = SRC.Progressivo,
		TGT.Quote = SRC.Quote,
        TGT.IDDocumento_Riga_Provenienza = SRC.IDDocumento_Riga_Provenienza,
        TGT.NoteDecisionali = SRC.NoteDecisionali,
        TGT.PKDataDisdetta = SRC.PKDataDisdetta,
        TGT.IDDocumentoRinnovato = SRC.IDDocumentoRinnovato

    WHEN NOT MATCHED
      THEN INSERT (
        IDDocumento_Riga,
        PKDataInizioEsercizio,
        PKDataFineEsercizio,
        PKDataRegistrazione,
        PKDataDocumento,
        PKDataCompetenza,
        PKCliente,
        PKClienteFattura,
        PKGruppoAgenti,
        PKCapoArea,
        PKDataFineContratto,
        PKDataInizioContratto,
        PKGruppoAgenti_Riga,
        PKCapoArea_Riga,
        PKArticolo,
        PKMacroTipologia,
        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
        IDDocumento,
        IDProfilo,
        Profilo,
        TipoRegistro,
        NumeroRegistro,
        Registro,
        CodiceEsercizio,
        NumeroDocumento,
        TipoSoggettoCommerciale,
        TipoSoggettoCommercialeFattura,
        Libero4,
        IDLibero1,
        Libero1,
        IDLibero2,
        Libero2,
        IDLibero3,
        Libero3,
        IDTipoFatturazione,
        TipoFatturazione,
        NumeroRiga,
        CodiceCondizioniPagamento,
        CondizioniPagamento,
        RinnovoAutomatico,
        NoteIntestazione,
        IsProfiloValidoPerStatisticaFatturato,
        IsProfiloValidoPerStatisticaFatturatoFormazione,
        ImportoTotale,
        ImportoProvvigioneCapoArea,
        ImportoProvvigioneAgente,
        ImportoProvvigioneSubagente,
        Progressivo,
		Quote,
        IDDocumento_Riga_Provenienza,
        NoteDecisionali,
        PKDataDisdetta,
        IDDocumentoRinnovato
      )
      VALUES (
        SRC.IDDocumento_Riga,
        SRC.PKDataInizioEsercizio,
        SRC.PKDataFineEsercizio,
        SRC.PKDataRegistrazione,
        SRC.PKDataDocumento,
        SRC.PKDataCompetenza,
        SRC.PKCliente,
        SRC.PKClienteFattura,
        SRC.PKGruppoAgenti,
        SRC.PKCapoArea,
        SRC.PKDataFineContratto,
        SRC.PKDataInizioContratto,
        SRC.PKGruppoAgenti_Riga,
        SRC.PKCapoArea_Riga,
        SRC.PKArticolo,
        SRC.PKMacroTipologia,
        SRC.HistoricalHashKey,
        SRC.ChangeHashKey,
        SRC.HistoricalHashKeyASCII,
        SRC.ChangeHashKeyASCII,
        SRC.InsertDatetime,
        SRC.UpdateDatetime,
        SRC.IsDeleted,
        SRC.IDDocumento,
        SRC.IDProfilo,
        SRC.Profilo,
        SRC.TipoRegistro,
        SRC.NumeroRegistro,
        SRC.Registro,
        SRC.CodiceEsercizio,
        SRC.NumeroDocumento,
        SRC.TipoSoggettoCommerciale,
        SRC.TipoSoggettoCommercialeFattura,
        SRC.Libero4,
        SRC.IDLibero1,
        SRC.Libero1,
        SRC.IDLibero2,
        SRC.Libero2,
        SRC.IDLibero3,
        SRC.Libero3,
        SRC.IDTipoFatturazione,
        SRC.TipoFatturazione,
        SRC.NumeroRiga,
        SRC.CodiceCondizioniPagamento,
        SRC.CondizioniPagamento,
        SRC.RinnovoAutomatico,
        SRC.NoteIntestazione,
        SRC.IsProfiloValidoPerStatisticaFatturato,
        SRC.IsProfiloValidoPerStatisticaFatturatoFormazione,
        SRC.ImportoTotale,
        SRC.ImportoProvvigioneCapoArea,
        SRC.ImportoProvvigioneAgente,
        SRC.ImportoProvvigioneSubagente,
        SRC.Progressivo,
		SRC.Quote,
        SRC.IDDocumento_Riga_Provenienza,
        SRC.NoteDecisionali,
        SRC.PKDataDisdetta,
        SRC.IDDocumentoRinnovato
      )

    WHEN NOT MATCHED BY SOURCE
      THEN UPDATE
        SET TGT.ChangeHashKey = CONVERT(VARBINARY(20), ''),
            TGT.ChangeHashKeyASCII = '',
            TGT.UpdateDatetime = CURRENT_TIMESTAMP,
            TGT.IsDeleted = CAST(1 AS BIT)

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        $action AS merge_action,
        'Staging.Documenti' AS full_olap_table_name,
        'IDDocumento_Riga = ' + CAST(COALESCE(inserted.IDDocumento_Riga, deleted.IDDocumento_Riga) AS NVARCHAR(1000)) AS primary_key_description
    INTO audit.merge_log_details;

    --DELETE FROM Fact.Documenti
    --WHERE IsDeleted = CAST(1 AS BIT);

    UPDATE audit.tables
    SET lastupdated_local = lastupdated_staging
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    COMMIT TRANSACTION;

END;

GO

