
CREATE PROCEDURE COMETA.usp_Merge_Gruppo_Agenti
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @IsCometaExportRunning BIT = 1;

    SELECT TOP (1) @IsCometaExportRunning = IsCometaExportRunning FROM COMETA.Semaforo;

    IF (COALESCE(@IsCometaExportRunning, 1) = 1) RETURN -1;

    MERGE INTO Landing.COMETA_Gruppo_Agenti AS TGT
    USING Landing.COMETA_Gruppo_AgentiView (nolock) AS SRC
    ON SRC.id_gruppo_agenti = TGT.id_gruppo_agenti

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.IsDeleted = SRC.IsDeleted,
        TGT.codice = SRC.codice,
        TGT.descrizione = SRC.descrizione,
        TGT.id_sog_com_capo_area = SRC.id_sog_com_capo_area,
        TGT.id_sog_com_agente = SRC.id_sog_com_agente,
        TGT.id_sog_com_sub_agente = SRC.id_sog_com_sub_agente

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        id_gruppo_agenti,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        codice,
        descrizione,
        id_sog_com_capo_area,
        id_sog_com_agente,
        id_sog_com_sub_agente
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
        'Landing.COMETA_Gruppo_Agenti' AS full_olap_table_name,
        'id_gruppo_agenti = ' + CAST(COALESCE(inserted.id_gruppo_agenti, deleted.id_gruppo_agenti) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;

GO

