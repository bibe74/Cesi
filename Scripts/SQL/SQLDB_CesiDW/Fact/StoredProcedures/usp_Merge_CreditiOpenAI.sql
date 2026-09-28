
CREATE PROCEDURE Fact.usp_Merge_CreditiOpenAI
AS
BEGIN

    SET NOCOUNT ON;

    BEGIN TRANSACTION 

    DECLARE @provider_name NVARCHAR(60) = N'GPT';
    DECLARE @full_table_name sysname = N'Import.Crediti';

    MERGE INTO Fact.CreditiOpenAI AS TGT
    USING Staging.CreditiOpenAI (nolock) AS SRC
    ON SRC.PKCliente = TGT.PKCliente

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.IsDeleted = SRC.IsDeleted,
        TGT.PKDataScadenzaOrdine = SRC.PKDataScadenzaOrdine,
        TGT.CreditiAcquistati = SRC.CreditiAcquistati,
        TGT.CreditiConsumati = SRC.CreditiConsumati,
        TGT.CreditiFuoriOrdine = SRC.CreditiFuoriOrdine,
        TGT.CreditiResidui = SRC.CreditiResidui

    WHEN NOT MATCHED
      THEN INSERT (
        --PKCreditiOpenAI,
        PKCliente,
        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
        PKDataScadenzaOrdine,
        CreditiAcquistati,
        CreditiConsumati,
        CreditiFuoriOrdine,
        CreditiResidui
    ) VALUES (
        PKCliente,
        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
        PKDataScadenzaOrdine,
        CreditiAcquistati,
        CreditiConsumati,
        CreditiFuoriOrdine,
        CreditiResidui
    )

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        $action AS merge_action,
        'Staging.CreditiOpenAI' AS full_olap_table_name,
        'PKCliente = ' + CAST(COALESCE(inserted.PKCliente, deleted.PKCliente) AS NVARCHAR(1000)) AS primary_key_description
    INTO audit.merge_log_details;

    DELETE FROM Fact.CreditiOpenAI
    WHERE IsDeleted = CAST(1 AS BIT);

    UPDATE audit.tables
    SET lastupdated_local = lastupdated_staging
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    COMMIT TRANSACTION;

END;

GO

