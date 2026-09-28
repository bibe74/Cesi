
CREATE   PROCEDURE COMETAINTEGRATION.usp_Merge_ArticleBIData
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @IsCometaExportRunning BIT = 1;

    SELECT TOP (1) @IsCometaExportRunning = IsCometaExportRunning FROM COMETA.Semaforo;

    IF (COALESCE(@IsCometaExportRunning, 1) = 1) RETURN -1;

    MERGE INTO Landing.COMETAINTEGRATION_ArticleBIData AS TGT
    USING Landing.COMETAINTEGRATION_ArticleBIDataView (nolock) AS SRC
    ON SRC.ArticleID = TGT.ArticleID

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.IsDeleted = SRC.IsDeleted,
        TGT.Data1 = SRC.Data1,
        TGT.Data2 = SRC.Data2,
        TGT.Data3 = SRC.Data3,
        TGT.Data4 = SRC.Data4,
        TGT.Data5 = SRC.Data5,
        TGT.Data6 = SRC.Data6

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        ArticleID,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        Data1,
        Data2,
        Data3,
        Data4,
        Data5,
        Data6
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
        'Landing.COMETAINTEGRATION_ArticleBIData' AS full_olap_table_name,
        'ArticleID = ' + CAST(COALESCE(inserted.ArticleID, deleted.ArticleID) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;

GO

