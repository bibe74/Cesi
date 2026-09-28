
CREATE PROCEDURE Dim.usp_Merge_MacroTipologia
AS
BEGIN

    SET NOCOUNT ON;

    BEGIN TRANSACTION 

    DECLARE @provider_name NVARCHAR(60) = N'MyDatamartReporting';
    DECLARE @full_table_name sysname = N'Import.MacroTipologia';

    MERGE INTO Dim.MacroTipologia AS TGT
    USING Staging.MacroTipologia (nolock) AS SRC
    ON SRC.MacroTipologia = TGT.MacroTipologia

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.IsDeleted = SRC.IsDeleted,
        
        TGT.IsValidaPerBudgetNuoveVendite = SRC.IsValidaPerBudgetNuoveVendite,
        TGT.IsValidaPerBudgetRinnovi = SRC.IsValidaPerBudgetRinnovi

    WHEN NOT MATCHED
      THEN INSERT (
        MacroTipologia,
        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
        IsValidaPerBudgetNuoveVendite,
        IsValidaPerBudgetRinnovi
      )
      VALUES (
        SRC.MacroTipologia,
        SRC.HistoricalHashKey,
        SRC.ChangeHashKey,
        SRC.HistoricalHashKeyASCII,
        SRC.ChangeHashKeyASCII,
        SRC.InsertDatetime,
        SRC.UpdateDatetime,
        SRC.IsDeleted,
        SRC.IsValidaPerBudgetNuoveVendite,
        SRC.IsValidaPerBudgetRinnovi
      )

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        $action AS merge_action,
        'Staging.MacroTipologia' AS full_olap_table_name,
        'MacroTipologia = ' + CAST(COALESCE(inserted.MacroTipologia, deleted.MacroTipologia) AS NVARCHAR(1000)) AS primary_key_description
    INTO audit.merge_log_details;

    --DELETE FROM Dim.MacroTipologia
    --WHERE IsDeleted = CAST(1 AS BIT);

    UPDATE audit.tables
    SET lastupdated_local = lastupdated_staging
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    COMMIT TRANSACTION;

END;

GO

