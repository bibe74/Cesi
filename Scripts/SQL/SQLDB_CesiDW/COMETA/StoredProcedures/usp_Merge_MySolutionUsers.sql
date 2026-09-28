
CREATE PROCEDURE COMETA.usp_Merge_MySolutionUsers
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @IsCometaExportRunning BIT = 1;

    SELECT TOP (1) @IsCometaExportRunning = IsCometaExportRunning FROM COMETA.Semaforo;

    IF (COALESCE(@IsCometaExportRunning, 1) = 1) RETURN -1;

    MERGE INTO Landing.COMETA_MySolutionUsers AS TGT
    USING Landing.COMETA_MySolutionUsersView (nolock) AS SRC
    ON SRC.Email = TGT.Email

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
        TGT.num_progressivo = SRC.num_progressivo,
        TGT.num_documento = SRC.num_documento,
        TGT.data_documento = SRC.data_documento,
        TGT.data_inizio_contratto = SRC.data_inizio_contratto,
        TGT.data_fine_contratto = SRC.data_fine_contratto,
        TGT.HaSconto = SRC.HaSconto,
        TGT.Nome = SRC.Nome,
        TGT.Cognome = SRC.Cognome,
        TGT.Quote = SRC.Quote,
        TGT.telefono_descrizione = SRC.telefono_descrizione,
        TGT.id_telefono = SRC.id_telefono,
        TGT.id_sog_commerciale = SRC.id_sog_commerciale,
        TGT.tipo = SRC.tipo,
        TGT.id_documento = SRC.id_documento

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        EMail,

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
        num_progressivo,
        num_documento,
        data_documento,
        data_inizio_contratto,
        data_fine_contratto,
        HaSconto,
        Nome,
        Cognome,
        Quote,
        telefono_descrizione,
        id_telefono,
        id_sog_commerciale,
        tipo,
        id_documento
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
        'Landing.COMETA_MySolutionUsers' AS full_olap_table_name,
        'EMail = ' + CAST(COALESCE(inserted.EMail, deleted.EMail) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;

GO

