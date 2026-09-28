
CREATE PROCEDURE COMETA.usp_Merge_Anagrafica
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @IsCometaExportRunning BIT = 1;

    SELECT TOP (1) @IsCometaExportRunning = IsCometaExportRunning FROM COMETA.Semaforo;

    IF (COALESCE(@IsCometaExportRunning, 1) = 1) RETURN -1;

    MERGE INTO Landing.COMETA_Anagrafica AS TGT
    USING Landing.COMETA_AnagraficaView (nolock) AS SRC
    ON SRC.id_anagrafica = TGT.id_anagrafica

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.IsDeleted = SRC.IsDeleted,
        TGT.rag_soc_1 = SRC.rag_soc_1,
        TGT.rag_soc_2 = SRC.rag_soc_2,
        TGT.indirizzo = SRC.indirizzo,
        TGT.cap = SRC.cap,
        TGT.localita = SRC.localita,
        TGT.provincia = SRC.provincia,
        TGT.nazione = SRC.nazione,
        TGT.cod_fiscale = SRC.cod_fiscale,
        TGT.par_iva = SRC.par_iva,
        TGT.indirizzo2 = SRC.indirizzo2

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        id_anagrafica,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        rag_soc_1,
        rag_soc_2,
        indirizzo,
        cap,
        localita,
        provincia,
        nazione,
        cod_fiscale,
        par_iva,
        indirizzo2
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
        'Landing.COMETA_Anagrafica' AS full_olap_table_name,
        'id_anagrafica = ' + CAST(COALESCE(inserted.id_anagrafica, deleted.id_anagrafica) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;

GO

