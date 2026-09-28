
CREATE PROCEDURE Fact.usp_Merge_Budget
AS
BEGIN

    SET NOCOUNT ON;

    BEGIN TRANSACTION 

    DECLARE @provider_name NVARCHAR(60) = N'MyDatamartReporting';
    DECLARE @full_table_name sysname = N'Import.Budget';

    MERGE INTO Fact.Budget AS TGT
    USING Staging.Budget (nolock) AS SRC
    ON SRC.PKData = TGT.PKData AND SRC.PKCapoArea = TGT.PKCapoArea AND SRC.PKMacroTipologia = TGT.PKMacroTipologia

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.IsDeleted = SRC.IsDeleted,
        
        TGT.ImportoBudgetNuoveVendite = SRC.ImportoBudgetNuoveVendite,
        TGT.ImportoBudgetRinnovi = SRC.ImportoBudgetRinnovi,
        TGT.ImportoBudget = SRC.ImportoBudget

    WHEN NOT MATCHED
      THEN INSERT (
        PKData,
        PKCapoArea,
        PKMacroTipologia,
        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
        ImportoBudgetNuoveVendite,
        ImportoBudgetRinnovi,
        ImportoBudget
      )
      VALUES (
        SRC.PKData,
        SRC.PKCapoArea,
        SRC.PKMacroTipologia,
        SRC.HistoricalHashKey,
        SRC.ChangeHashKey,
        SRC.HistoricalHashKeyASCII,
        SRC.ChangeHashKeyASCII,
        SRC.InsertDatetime,
        SRC.UpdateDatetime,
        SRC.IsDeleted,
        SRC.ImportoBudgetNuoveVendite,
        SRC.ImportoBudgetRinnovi,
        SRC.ImportoBudget
      )

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        $action AS merge_action,
        'Staging.Budget' AS full_olap_table_name,
        'PKData = ' + CAST(COALESCE(inserted.PKData, deleted.PKData) AS NVARCHAR(1000)) + ', PKCapoArea = ' + CAST(COALESCE(inserted.PKCapoArea, deleted.PKCapoArea) AS NVARCHAR(1000)) + ', PKMacroTipologia = ' + CAST(COALESCE(inserted.PKMacroTipologia, deleted.PKMacroTipologia) AS NVARCHAR(1000)) AS primary_key_description
    INTO audit.merge_log_details;

    DELETE FROM Fact.Budget
    WHERE IsDeleted = CAST(1 AS BIT);

    UPDATE audit.tables
    SET lastupdated_local = lastupdated_staging
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    COMMIT TRANSACTION;

END;

GO

