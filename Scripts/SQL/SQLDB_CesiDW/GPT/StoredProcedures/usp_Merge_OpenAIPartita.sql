
CREATE PROCEDURE GPT.usp_Merge_OpenAIPartita
AS
BEGIN
    SET NOCOUNT ON;

    MERGE INTO Landing.GPT_OpenAIPartita AS TGT
    USING Landing.GPT_OpenAIPartitaView (nolock) AS SRC
    ON SRC.Id = TGT.Id

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.Codice = SRC.Codice,
        TGT.Descrizione = SRC.Descrizione,
        TGT.DataCreazione = SRC.DataCreazione,
        TGT.DataScadenza = SRC.DataScadenza,
        TGT.ClienteId = SRC.ClienteId,
        TGT.Stato = SRC.Stato,
        TGT.Quantita = SRC.Quantita

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        Id,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        Codice,
        Descrizione,
        DataCreazione,
        DataScadenza,
        ClienteId,
        Stato,
        Quantita
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
        'Landing.GPT_OpenAIPartita' AS full_olap_table_name,
        'Id = ' + CAST(COALESCE(inserted.Id, deleted.Id) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;

GO

