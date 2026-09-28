
CREATE   PROCEDURE Bridge.usp_Merge_ADUserCapoArea
AS
BEGIN

    MERGE INTO Bridge.ADUserCapoArea AS TGT
    USING Bridge.ADUserCapoAreaView AS SRC
    ON SRC.ADUser = TGT.ADUser AND SRC.CapoArea = TGT.CapoArea
    WHEN NOT MATCHED THEN INSERT (
        ADUser,
        CapoArea
    )
    VALUES (
        SRC.ADUser,
        SRC.CapoArea
    )
    WHEN NOT MATCHED BY SOURCE THEN DELETE
    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        $action AS merge_action,
        'Bridge.ADUserCapoArea' AS full_olap_table_name,
        'ADUser = ' + CAST(COALESCE(inserted.ADUser, deleted.ADUser) AS NVARCHAR(1000)) + ', CapoArea = ' + CAST(COALESCE(inserted.CapoArea, deleted.CapoArea) AS NVARCHAR(1000)) AS primary_key_description
    INTO audit.merge_log_details;

END;

GO

