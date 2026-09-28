
CREATE PROCEDURE COMETA.usp_Merge_Telefono
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @IsCometaExportRunning BIT = 1;

    SELECT TOP (1) @IsCometaExportRunning = IsCometaExportRunning FROM COMETA.Semaforo;

    IF (COALESCE(@IsCometaExportRunning, 1) = 1) RETURN -1;

    MERGE INTO Landing.COMETA_Telefono AS TGT
    USING Landing.COMETA_TelefonoView (nolock) AS SRC
    ON SRC.id_telefono = TGT.id_telefono

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.IsDeleted = SRC.IsDeleted,
        TGT.id_anagrafica = SRC.id_anagrafica,
        TGT.tipo = SRC.tipo,
        TGT.num_riferimento = SRC.num_riferimento,
        TGT.descrizione = SRC.descrizione,
        TGT.interlocutore = SRC.interlocutore,
        TGT.nome = SRC.nome,
        TGT.cognome = SRC.cognome,
        TGT.ruolo = SRC.ruolo

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        id_telefono,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        id_anagrafica,
        tipo,
        num_riferimento,
        descrizione,
        interlocutore,
        nome,
        cognome,
        ruolo
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
        'Landing.COMETA_Telefono' AS full_olap_table_name,
        'id_telefono = ' + CAST(COALESCE(inserted.id_telefono, deleted.id_telefono) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;

GO

