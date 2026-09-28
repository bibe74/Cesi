
CREATE   PROCEDURE Landing.usp_Merge_MYSOLUTION_Analytics
AS
BEGIN
    SET NOCOUNT ON;

    MERGE INTO Landing.MYSOLUTION_Analytics AS TGT
    USING Landing.MYSOLUTION_AnalyticsView AS SRC ON (
         SRC.created_at = TGT.created_at AND SRC.email = TGT.email AND SRC.path = TGT.path 
    )

    WHEN MATCHED AND SRC.ChangeHashKey <> TGT.ChangeHashKey
      THEN UPDATE SET TGT.ChangeHashKey = SRC.ChangeHashKey, TGT.UpdateDatetime = SRC.UpdateDatetime, TGT.IsDeleted = SRC.IsDeleted, 
        TGT.NumeroVisite = SRC.NumeroVisite

    WHEN NOT MATCHED BY TARGET
      THEN INSERT (created_at, email, path, HistoricalHashKey, ChangeHashKey, InsertDatetime, UpdateDatetime, IsDeleted, NumeroVisite)
        VALUES (created_at, email, path, HistoricalHashKey, ChangeHashKey, InsertDatetime, UpdateDatetime, IsDeleted, NumeroVisite)

    WHEN NOT MATCHED BY SOURCE AND TGT.IsDeleted = CAST(0 AS BIT)
      THEN UPDATE SET TGT.ChangeHashKey = CONVERT(VARBINARY(32), 0),
        TGT.UpdateDatetime = CURRENT_TIMESTAMP,
        TGT.IsDeleted = CAST(1 AS BIT)
    
    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        CASE WHEN Inserted.IsDeleted = CAST(1 AS BIT) THEN N'DELETE' ELSE $action END AS merge_action,
        'Landing.MYSOLUTION_Analytics' AS full_olap_table_name,
        'created_at = ' + CAST(COALESCE(inserted.created_at, deleted.created_at) AS NVARCHAR) + 'email = ' + CAST(COALESCE(inserted.email, deleted.email) AS NVARCHAR) + 'path = ' + CAST(COALESCE(inserted.path, deleted.path) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END

GO

