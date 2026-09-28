
CREATE PROCEDURE MYSOLUTION.usp_Merge_Customer_CustomerRole_Mapping
AS
BEGIN
    SET NOCOUNT ON;

    MERGE INTO Landing.MYSOLUTION_Customer_CustomerRole_Mapping AS TGT
    USING Landing.MYSOLUTION_Customer_CustomerRole_MappingView (nolock) AS SRC
    ON SRC.Customer_Id = TGT.Customer_Id AND SRC.CustomerRole_Id = TGT.CustomerRole_Id

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        Customer_Id,
        CustomerRole_Id,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted
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
        'Landing.MYSOLUTION_Customer_CustomerRole_Mapping' AS full_olap_table_name,
        'Customer_Id = ' + CAST(COALESCE(inserted.Customer_Id, deleted.Customer_Id) AS NVARCHAR)
        + 'CustomerRole_Id = ' + CAST(COALESCE(inserted.CustomerRole_Id, deleted.CustomerRole_Id) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;

GO

