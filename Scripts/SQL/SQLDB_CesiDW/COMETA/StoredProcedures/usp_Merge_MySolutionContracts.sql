
CREATE PROCEDURE COMETA.usp_Merge_MySolutionContracts
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @IsCometaExportRunning BIT = 1;

    SELECT TOP (1) @IsCometaExportRunning = IsCometaExportRunning FROM COMETA.Semaforo;

    IF (COALESCE(@IsCometaExportRunning, 1) = 1) RETURN -1;

    MERGE INTO Landing.COMETA_MySolutionContracts AS TGT
    USING Landing.COMETA_MySolutionContractsView (nolock) AS SRC
    ON SRC.id_riga_documento = TGT.id_riga_documento

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.IsDeleted = SRC.IsDeleted,
        TGT.id_anagrafica = SRC.id_anagrafica,
        TGT.codice = SRC.codice,
        TGT.RagioneSociale = SRC.RagioneSociale,
        TGT.indirizzo = SRC.indirizzo,
        TGT.cap = SRC.cap,
        TGT.localita = SRC.localita,
        TGT.provincia = SRC.provincia,
        TGT.nazione = SRC.nazione,
        TGT.cod_fiscale = SRC.cod_fiscale,
        TGT.par_iva = SRC.par_iva,
        TGT.EMail = SRC.EMail,
        TGT.num_progressivo = SRC.num_progressivo,
        TGT.num_documento = SRC.num_documento,
        TGT.data_documento = SRC.data_documento,
        TGT.data_inizio_contratto = SRC.data_inizio_contratto,
        TGT.data_fine_contratto = SRC.data_fine_contratto,
        TGT.Nome = SRC.Nome,
        TGT.Cognome = SRC.Cognome,
        TGT.Quote = SRC.Quote,
        TGT.id_sog_commerciale = SRC.id_sog_commerciale,
        TGT.tipo = SRC.tipo,
        TGT.id_documento = SRC.id_documento,
        TGT.descrizione = SRC.descrizione,
        TGT.id_articolo = SRC.id_articolo,
        TGT.prezzo = SRC.prezzo,
        TGT.sconto = SRC.sconto,
        TGT.prezzo_netto = SRC.prezzo_netto,
        TGT.prezzo_netto_ivato = SRC.prezzo_netto_ivato,
        TGT.note_intestazione = SRC.note_intestazione,
        TGT.data_disdetta = SRC.data_disdetta,
        TGT.motivo_disdetta = SRC.motivo_disdetta,
        TGT.pec = SRC.pec,
        TGT.CodiceArticolo = SRC.CodiceArticolo

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        id_riga_documento,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,

        id_anagrafica,
        codice,
        RagioneSociale,
        indirizzo,
        cap,
        localita,
        provincia,
        nazione,
        cod_fiscale,
        par_iva,
        EMail,
        num_progressivo,
        num_documento,
        data_documento,
        data_inizio_contratto,
        data_fine_contratto,
        Nome,
        Cognome,
        Quote,
        id_sog_commerciale,
        tipo,
        id_documento,
        descrizione,
        id_articolo,
        prezzo,
        sconto,
        prezzo_netto,
        prezzo_netto_ivato,
        note_intestazione,
        data_disdetta,
        motivo_disdetta,
        pec,
        CodiceArticolo
      )

    WHEN NOT MATCHED BY SOURCE
        AND TGT.IsDeleted = CAST(0 AS BIT)
      THEN UPDATE
        SET TGT.IsDeleted = CAST(1 AS BIT),
        TGT.UpdateDatetime = CURRENT_TIMESTAMP,
        TGT.ChangeHashKey = CONVERT(VARBINARY(20), ''),
        TGT.ChangeHashKeyASCII = ''

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        CASE WHEN Inserted.IsDeleted = CAST(1 AS BIT) THEN N'DELETE' ELSE $action END AS merge_action,
        'Landing.COMETA_MySolutionContracts' AS full_olap_table_name,
        'id_riga_documento = ' + CAST(COALESCE(inserted.id_riga_documento, deleted.id_riga_documento) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;

GO

