
CREATE PROCEDURE MYSOLUTION.usp_Merge_Courses
AS
BEGIN
    SET NOCOUNT ON;

    MERGE INTO Landing.MYSOLUTION_Courses AS TGT
    USING Landing.MYSOLUTION_CoursesView (NOLOCK) AS SRC
    ON SRC.OrderItemId = TGT.OrderItemId AND SRC.Partecipant_Id = TGT.Partecipant_Id

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.OrderItemId = SRC.OrderItemId,
        TGT.Partecipant_Id = SRC.Partecipant_Id,
        TGT.PartecipantFirstName = SRC.PartecipantFirstName,
        TGT.PartecipantLastName = SRC.PartecipantLastName,
        TGT.PartecipantEmail = SRC.PartecipantEmail,
        TGT.PartecipantFiscalCode = SRC.PartecipantFiscalCode,
        TGT.RootPartecipantEmail = SRC.RootPartecipantEmail,
        TGT.CustomerUserName = SRC.CustomerUserName,
        TGT.CourseName = SRC.CourseName,
        TGT.CourseType = SRC.CourseType,
        TGT.StartDate_text = SRC.StartDate_text,
        TGT.StartDate = SRC.StartDate,
        TGT.OrderNumber = SRC.OrderNumber,
        TGT.OrderCreatedDate = SRC.OrderCreatedDate

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        OrderItemId,
        Partecipant_Id,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        PartecipantFirstName,
        PartecipantLastName,
        PartecipantEmail,
        PartecipantFiscalCode,
        RootPartecipantEmail,
        CustomerUserName,
        CourseName,
        CourseType,
        StartDate_text,
        StartDate,
        OrderNumber,
        OrderCreatedDate
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
        'Landing.MYSOLUTION_Courses' AS full_olap_table_name,
        'OrderItemId = ' + CAST(COALESCE(inserted.OrderItemId, deleted.OrderItemId) AS NVARCHAR)
            + ', Partecipant_Id = ' + CAST(COALESCE(inserted.Partecipant_Id, deleted.Partecipant_Id) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;

GO

