
CREATE PROCEDURE MYSOLUTION.usp_Merge_LogsForReport
AS
BEGIN
    SET NOCOUNT ON;

    MERGE INTO Landing.MYSOLUTION_LogsForReport AS TGT
    USING Landing.MYSOLUTION_LogsForReportView (nolock) AS SRC
    ON SRC.Data = TGT.Data AND SRC.Username = TGT.Username

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.NumeroAccessi = SRC.NumeroAccessi,
        TGT.NumeroPagineVisitate = SRC.NumeroPagineVisitate

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        Data,
        Username,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        NumeroAccessi,
        NumeroPagineVisitate
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
        'Landing.MYSOLUTION_LogsForReport' AS full_olap_table_name,
        'Data/Username = ' + CAST(COALESCE(inserted.Data, deleted.Data) AS NVARCHAR) + N'/'+ CAST(COALESCE(inserted.Username, deleted.Username) AS NVARCHAR(50)) AS primary_key_description
    INTO audit.merge_log_details;

END;

GO

