
CREATE PROCEDURE Fact.usp_Merge_CreditiOpenAIDettaglio
AS
BEGIN

    SET NOCOUNT ON;

    BEGIN TRANSACTION 

    DECLARE @provider_name NVARCHAR(60) = N'GPT';
    DECLARE @full_table_name sysname = N'Import.Crediti';

    MERGE INTO Fact.CreditiOpenAIDettaglio AS TGT
    USING Staging.CreditiOpenAIDettaglio (nolock) AS SRC
    ON SRC.PKCliente = TGT.PKCliente
	    AND SRC.CodiceOrdine = TGT.CodiceOrdine
	    AND SRC.PKDataUltimoUtilizzo = TGT.PKDataUltimoUtilizzo
	    AND SRC.PKDataCreazionePartita = TGT.PKDataCreazionePartita
	    AND SRC.PKDataScadenzaPartita = TGT.PKDataScadenzaPartita
        AND SRC.QtaCreditiCaricatiInPartita = TGT.QtaCreditiCaricatiInPartita

    WHEN MATCHED AND (SRC.ChangeHashKey <> TGT.ChangeHashKey)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.IsDeleted = 0,
        TGT.PKDataInizioDemo = SRC.PKDataInizioDemo,
        TGT.QtaCreditiResidui = SRC.QtaCreditiResidui,
        TGT.QtaCreditiUtilizzati = SRC.QtaCreditiUtilizzati,
        TGT.ConteggioDomandeERisposte = SRC.ConteggioDomandeERisposte

    WHEN NOT MATCHED
      THEN INSERT (
        --PKCreditiOpenAIDettaglio,
        PKCliente,
        CodiceOrdine,
        PKDataUltimoUtilizzo,
        PKDataCreazionePartita,
        PKDataScadenzaPartita,
        QtaCreditiCaricatiInPartita,
        HistoricalHashKey,
        ChangeHashKey,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
        PKDataInizioDemo,
        QtaCreditiResidui,
        QtaCreditiUtilizzati,
        ConteggioDomandeERisposte
    ) VALUES (
        PKCliente,
        CodiceOrdine,
        PKDataUltimoUtilizzo,
        PKDataCreazionePartita,
        PKDataScadenzaPartita,
        QtaCreditiCaricatiInPartita,
        HistoricalHashKey,
        ChangeHashKey,
        InsertDatetime,
        UpdateDatetime,
        0,
        PKDataInizioDemo,
        QtaCreditiResidui,
        QtaCreditiUtilizzati,
        ConteggioDomandeERisposte
    )

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        $action AS merge_action,
        'Fact.CreditiOpenAIDettaglio' AS full_olap_table_name,
        'PKCliente = ' + CAST(COALESCE(inserted.PKCliente, deleted.PKCliente) AS NVARCHAR(1000))
            + ', CodiceOrdine = ' + CAST(COALESCE(inserted.CodiceOrdine, deleted.CodiceOrdine) AS NVARCHAR(1000))
	        + ', PKDataUltimoUtilizzo = ' + CAST(COALESCE(inserted.PKDataUltimoUtilizzo, deleted.PKDataUltimoUtilizzo) AS NVARCHAR(1000))
	        + ', PKDataCreazionePartita = ' + CAST(COALESCE(inserted.PKDataCreazionePartita, deleted.PKDataCreazionePartita) AS NVARCHAR(1000))
	        + ', PKDataScadenzaPartita = ' + CAST(COALESCE(inserted.PKDataScadenzaPartita, deleted.PKDataScadenzaPartita) AS NVARCHAR(1000))
	        + ', QtaCreditiCaricatiInPartita = ' + CAST(COALESCE(inserted.QtaCreditiCaricatiInPartita, deleted.QtaCreditiCaricatiInPartita) AS NVARCHAR(1000))
        
        AS primary_key_description
    INTO audit.merge_log_details;

    --DELETE FROM Fact.CreditiOpenAIDettaglio
    --WHERE IsDeleted = CAST(1 AS BIT);
    
    UPDATE audit.tables
    SET lastupdated_local = lastupdated_staging
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    COMMIT TRANSACTION;

END;

GO

