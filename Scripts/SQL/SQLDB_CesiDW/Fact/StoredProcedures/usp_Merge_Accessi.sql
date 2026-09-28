
CREATE PROCEDURE Fact.usp_Merge_Accessi
AS
BEGIN

    SET NOCOUNT ON;

    BEGIN TRANSACTION 

    DECLARE @provider_name NVARCHAR(60) = N'MyDatamartReporting';
    DECLARE @full_table_name sysname = N'Landing.MYSOLUTION_LogsForReport';

    MERGE INTO Fact.Accessi AS TGT
    USING Staging.Accessi (nolock) AS SRC
    ON SRC.PKData = TGT.PKData AND SRC.PKCliente = TGT.PKCliente

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.IsDeleted = SRC.IsDeleted,
        TGT.NumeroAccessi = SRC.NumeroAccessi,
        TGT.NumeroPagineVisitate = SRC.NumeroPagineVisitate,
        TGT.PKCapoArea = SRC.PKCapoArea

    WHEN NOT MATCHED
      THEN INSERT (
        PKData,
        PKCliente,
        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
        NumeroAccessi,
        NumeroPagineVisitate,
        PKCapoArea
      )
      VALUES (
        SRC.PKData,
        SRC.PKCliente,
        SRC.HistoricalHashKey,
        SRC.ChangeHashKey,
        SRC.HistoricalHashKeyASCII,
        SRC.ChangeHashKeyASCII,
        SRC.InsertDatetime,
        SRC.UpdateDatetime,
        SRC.IsDeleted,
        SRC.NumeroAccessi,
        SRC.NumeroPagineVisitate,
        SRC.PKCapoArea
      )

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        $action AS merge_action,
        'Staging.Accessi' AS full_olap_table_name,
        'PKData = ' + CAST(COALESCE(inserted.PKData, deleted.PKData) AS NVARCHAR(1000)) + ', PKCliente = ' + CAST(COALESCE(inserted.PKCliente, deleted.PKCliente) AS NVARCHAR(1000)) AS primary_key_description
    INTO audit.merge_log_details;

    --DELETE FROM Fact.Accessi
    --WHERE IsDeleted = CAST(1 AS BIT);

    UPDATE audit.tables
    SET lastupdated_local = lastupdated_staging
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    COMMIT TRANSACTION;

END;

GO

