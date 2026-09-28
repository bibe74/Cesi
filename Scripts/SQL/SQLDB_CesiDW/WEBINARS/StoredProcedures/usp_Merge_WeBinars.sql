
CREATE   PROCEDURE WEBINARS.usp_Merge_WeBinars
AS
BEGIN
    SET NOCOUNT ON;

    MERGE INTO Landing.WEBINARS_WeBinars AS TGT
    USING Landing.WEBINARS_WeBinarsView (nolock) AS SRC
    ON SRC.Source = TGT.Source

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.IsDeleted = SRC.IsDeleted,
        TGT.VideoStartDate = SRC.VideoStartDate,
        TGT.VideoTitle = SRC.VideoTitle,
        TGT.CourseTitle = SRC.CourseTitle,
        TGT.CourseType = SRC.CourseType

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        Source,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        VideoStartDate,
        VideoTitle,
        CourseTitle,
        CourseType
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
        'Landing.WEBINARS_WeBinars' AS full_olap_table_name,
        'Source = ' + CAST(COALESCE(inserted.Source, deleted.Source) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;

GO

