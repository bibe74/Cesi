
CREATE PROCEDURE MYSOLUTION.usp_Merge_Address
AS
BEGIN
    SET NOCOUNT ON;

    MERGE INTO Landing.MYSOLUTION_Address AS TGT
    USING Landing.MYSOLUTION_AddressView (nolock) AS SRC
    ON SRC.Id = TGT.Id

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.FirstName = SRC.FirstName,
        TGT.LastName = SRC.LastName,
        TGT.Email = SRC.Email,
        TGT.Company = SRC.Company,
        TGT.Country = SRC.Country,
        TGT.StateProvince = SRC.StateProvince,
        TGT.City = SRC.City,
        TGT.Address1 = SRC.Address1,
        TGT.Address2 = SRC.Address2,
        TGT.ZipPostalCode = SRC.ZipPostalCode,
        TGT.PhoneNumber = SRC.PhoneNumber,
        TGT.County = SRC.County,
        TGT.CodiceFiscale = SRC.CodiceFiscale,
        TGT.Piva = SRC.Piva

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
        Company,
        Country,
        StateProvince,
        City,
        Address1,
        Address2,
        ZipPostalCode,
        PhoneNumber,
        County,
        CodiceFiscale,
        Piva
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
        'Landing.MYSOLUTION_Address' AS full_olap_table_name,
        'Id = ' + CAST(COALESCE(inserted.Id, deleted.Id) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;

GO

