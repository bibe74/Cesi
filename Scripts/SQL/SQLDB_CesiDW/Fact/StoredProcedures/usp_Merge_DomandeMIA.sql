
CREATE PROCEDURE Fact.usp_Merge_DomandeMIA
AS
BEGIN

    SET NOCOUNT ON;

    BEGIN TRANSACTION 

    DECLARE @provider_name NVARCHAR(60) = N'GPT';
    DECLARE @full_table_name sysname = N'Import.Crediti';

    MERGE INTO Fact.DomandeMIA AS TGT
    USING Staging.DomandeMIA (nolock) AS SRC
    ON SRC.Id = TGT.Id

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.IsDeleted = SRC.IsDeleted,
        TGT.PKDataCreazione = SRC.PKDataCreazione,
        TGT.Testo = SRC.Testo,
        TGT.IsDomanda = SRC.IsDomanda,
        TGT.Area = SRC.Area,
        TGT.Email = SRC.Email

    WHEN NOT MATCHED
      THEN INSERT (
        --PKDomandeMIA,
        Id,
        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
        PKDataCreazione,
        Testo,
        IsDomanda,
        Area,
        Email
    ) VALUES (
        Id,
        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
        PKDataCreazione,
        Testo,
        IsDomanda,
        Area,
        Email
    )

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        $action AS merge_action,
        'Staging.DomandeMIA' AS full_olap_table_name,
        'Id = ' + CAST(COALESCE(inserted.Id, deleted.Id) AS NVARCHAR(1000)) AS primary_key_description
    INTO audit.merge_log_details;

    DELETE FROM Fact.DomandeMIA
    WHERE IsDeleted = CAST(1 AS BIT);

    UPDATE audit.tables
    SET lastupdated_local = lastupdated_staging
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    COMMIT TRANSACTION;

END;

GO

