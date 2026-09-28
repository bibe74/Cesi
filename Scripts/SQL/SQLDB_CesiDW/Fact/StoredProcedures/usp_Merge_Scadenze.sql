CREATE PROCEDURE Fact.usp_Merge_Scadenze
AS
BEGIN

    SET NOCOUNT ON;

    BEGIN TRANSACTION 

    DECLARE @provider_name NVARCHAR(60) = N'MyDatamartReporting';
    DECLARE @full_table_name sysname = N'Landing.COMETA_Documento_Riga';

    MERGE INTO Fact.Scadenze AS TGT
    USING Staging.Scadenze (nolock) AS SRC
    ON SRC.IDScadenza = TGT.IDScadenza

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.IsDeleted = SRC.IsDeleted,

        TGT.PKCliente = SRC.PKCliente,
        TGT.PKDataScadenza = SRC.PKDataScadenza,
        TGT.PKDocumenti = SRC.PKDocumenti,

        TGT.IDScadenza = SRC.IDScadenza,
        TGT.TipoScadenza = SRC.TipoScadenza,
        TGT.IDSoggettoCommerciale = SRC.IDSoggettoCommerciale,
        TGT.StatoScadenza = SRC.StatoScadenza,
        TGT.EsitoPagamento = SRC.EsitoPagamento,
        TGT.IDDocumento = SRC.IDDocumento,
        TGT.ImportoScadenza = SRC.ImportoScadenza,
        TGT.ImportoSaldato = SRC.ImportoSaldato,
        TGT.ImportoResiduo = SRC.ImportoResiduo

    WHEN NOT MATCHED
      THEN INSERT (
        PKCliente,
        PKDataScadenza,
        PKDocumenti,
        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
        IDScadenza,
        TipoScadenza,
        IDSoggettoCommerciale,
        StatoScadenza,
        EsitoPagamento,
        IDDocumento,
        ImportoScadenza,
        ImportoSaldato,
        ImportoResiduo
      )
      VALUES (
        SRC.PKCliente,
        SRC.PKDataScadenza,
        SRC.PKDocumenti,
        SRC.HistoricalHashKey,
        SRC.ChangeHashKey,
        SRC.HistoricalHashKeyASCII,
        SRC.ChangeHashKeyASCII,
        SRC.InsertDatetime,
        SRC.UpdateDatetime,
        SRC.IsDeleted,
        SRC.IDScadenza,
        SRC.TipoScadenza,
        SRC.IDSoggettoCommerciale,
        SRC.StatoScadenza,
        SRC.EsitoPagamento,
        SRC.IDDocumento,
        SRC.ImportoScadenza,
        SRC.ImportoSaldato,
        SRC.ImportoResiduo
      )

    WHEN NOT MATCHED BY SOURCE
      THEN UPDATE
        SET TGT.ChangeHashKey = CONVERT(VARBINARY(20), ''),
            TGT.ChangeHashKeyASCII = '',
            TGT.UpdateDatetime = CURRENT_TIMESTAMP,
            TGT.IsDeleted = CAST(1 AS BIT)

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        $action AS merge_action,
        'Staging.Scadenze' AS full_olap_table_name,
        'IDScadenza = ' + CAST(COALESCE(inserted.IDScadenza, deleted.IDScadenza) AS NVARCHAR(1000)) AS primary_key_description
    INTO audit.merge_log_details;

    --DELETE FROM Fact.Scadenze
    --WHERE IsDeleted = CAST(1 AS BIT);

    UPDATE audit.tables
    SET lastupdated_local = lastupdated_staging
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    COMMIT TRANSACTION;

END;

GO

