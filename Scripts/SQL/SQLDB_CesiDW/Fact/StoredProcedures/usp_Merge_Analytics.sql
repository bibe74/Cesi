
CREATE   PROCEDURE Fact.usp_Merge_Analytics
AS
BEGIN
    SET NOCOUNT ON;

    MERGE INTO Fact.Analytics AS TGT
    USING Fact.AnalyticsView AS SRC ON (
         SRC.PKDataVisita = TGT.PKDataVisita AND SRC.PKCliente = TGT.PKCliente AND SRC.Percorso = TGT.Percorso 
    )

    WHEN MATCHED AND SRC.ChangeHashKey <> TGT.ChangeHashKey
      THEN UPDATE SET TGT.ChangeHashKey = SRC.ChangeHashKey, TGT.UpdateDatetime = SRC.UpdateDatetime, TGT.IsDeleted = SRC.IsDeleted, 
        TGT.NumeroVisite = SRC.NumeroVisite

    WHEN NOT MATCHED BY TARGET
      THEN INSERT (PKDataVisita, PKCliente, Percorso, HistoricalHashKey, ChangeHashKey, InsertDatetime, UpdateDatetime, IsDeleted, NumeroVisite)
        VALUES (PKDataVisita, PKCliente, Percorso, HistoricalHashKey, ChangeHashKey, InsertDatetime, UpdateDatetime, IsDeleted, NumeroVisite)

    WHEN NOT MATCHED BY SOURCE AND TGT.IsDeleted = CAST(0 AS BIT)
      THEN UPDATE SET TGT.ChangeHashKey = CONVERT(VARBINARY(32), 0),
        TGT.UpdateDatetime = CURRENT_TIMESTAMP,
        TGT.IsDeleted = CAST(1 AS BIT)
    
    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        CASE WHEN Inserted.IsDeleted = CAST(1 AS BIT) THEN N'DELETE' ELSE $action END AS merge_action,
        'Fact.Analytics' AS full_olap_table_name,
        'PKDataVisita = ' + CAST(COALESCE(inserted.PKDataVisita, deleted.PKDataVisita) AS NVARCHAR) + 'PKCliente = ' + CAST(COALESCE(inserted.PKCliente, deleted.PKCliente) AS NVARCHAR) + 'Percorso = ' + CAST(COALESCE(inserted.Percorso, deleted.Percorso) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END

GO

