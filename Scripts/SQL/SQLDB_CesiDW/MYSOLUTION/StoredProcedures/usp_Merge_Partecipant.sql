
CREATE PROCEDURE MYSOLUTION.usp_Merge_Partecipant
AS
BEGIN
    SET NOCOUNT ON;

    MERGE INTO Landing.MYSOLUTION_Partecipant AS TGT
    USING Landing.MYSOLUTION_PartecipantView (NOLOCK) AS SRC
    ON SRC.Id = TGT.Id

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.FirstName = SRC.FirstName,
        TGT.LastName = SRC.LastName,
        TGT.Email = SRC.Email,
        TGT.PhoneNumber = SRC.PhoneNumber,
        TGT.Ssn = SRC.Ssn,
        TGT.CreatedOnUtc = SRC.CreatedOnUtc,
        TGT.IdProfession = SRC.IdProfession,
        TGT.IdProfessionDetail = SRC.IdProfessionDetail,
        TGT.MobilePhone = SRC.MobilePhone,
        TGT.OriginalPartecipantId = SRC.OriginalPartecipantId

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
    
        FirstName,
        LastName,
        Email,
        PhoneNumber,
        Ssn,
        CreatedOnUtc,
        IdProfession,
        IdProfessionDetail,
        MobilePhone,
        OriginalPartecipantId
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
        'Landing.MYSOLUTION_Partecipant' AS full_olap_table_name,
        'Id = ' + CAST(COALESCE(inserted.Id, deleted.Id) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;

GO

