
CREATE PROCEDURE Fact.usp_Merge_Corsi
AS
BEGIN

    SET NOCOUNT ON;

    BEGIN TRANSACTION 

    DECLARE @provider_name NVARCHAR(60) = N'MyDatamartReporting';
    DECLARE @full_table_name sysname = N'Import.Corsi';

    MERGE INTO Fact.Corsi AS TGT
    USING Staging.Corsi (nolock) AS SRC
    ON SRC.OrderItemId = TGT.OrderItemId AND SRC.Partecipant_Id = TGT.Partecipant_Id

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.IsDeleted = SRC.IsDeleted,
        TGT.PKUtente = SRC.PKUtente,
        TGT.PKDataInizio = SRC.PKDataInizio,
        TGT.PKDataIscrizione = SRC.PKDataIscrizione,
        TGT.NomePartecipante = SRC.NomePartecipante,
        TGT.CognomePartecipante = SRC.CognomePartecipante,
        TGT.EmailPartecipante = SRC.EmailPartecipante,
        TGT.CodiceFiscalePartecipante = SRC.CodiceFiscalePartecipante,
        TGT.EmailPartecipanteRoot = SRC.EmailPartecipanteRoot,
        TGT.Utente = SRC.Utente,
        TGT.Corso = SRC.Corso,
        TGT.IDCorso = SRC.IDCorso,
        TGT.IDWebinar = SRC.IDWebinar,
        TGT.TipoCorso = SRC.TipoCorso,
        TGT.HasDateMultiple = SRC.HasDateMultiple,
        TGT.DescrizioneOrdine = SRC.DescrizioneOrdine,
        TGT.PrezzoUnitarioOrdine = SRC.PrezzoUnitarioOrdine,
        TGT.NumeroOrdine = SRC.NumeroOrdine,
        TGT.StatoOrdine = SRC.StatoOrdine,
        TGT.ImportoTotaleOrdine = SRC.ImportoTotaleOrdine

    WHEN NOT MATCHED
      THEN INSERT (
        --PKCorsi,
        OrderItemId,
        Partecipant_Id,
        PKUtente,
        PKDataInizio,
        PKDataIscrizione,
        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
        NomePartecipante,
        CognomePartecipante,
        EmailPartecipante,
        CodiceFiscalePartecipante,
        EmailPartecipanteRoot,
        Utente,
        Corso,
        IDCorso,
        IDWebinar,
        TipoCorso,
        HasDateMultiple,
        DescrizioneOrdine,
        PrezzoUnitarioOrdine,
        NumeroOrdine,
        StatoOrdine,
        ImportoTotaleOrdine
    ) VALUES (
        OrderItemId,
        Partecipant_Id,
        PKUtente,
        PKDataInizio,
        PKDataIscrizione,
        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
        NomePartecipante,
        CognomePartecipante,
        EmailPartecipante,
        CodiceFiscalePartecipante,
        EmailPartecipanteRoot,
        Utente,
        Corso,
        IDCorso,
        IDWebinar,
        TipoCorso,
        HasDateMultiple,
        DescrizioneOrdine,
        PrezzoUnitarioOrdine,
        NumeroOrdine,
        StatoOrdine,
        ImportoTotaleOrdine
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
        'Staging.Corsi' AS full_olap_table_name,
        'OrderItemId = ' + CAST(COALESCE(inserted.OrderItemId, deleted.OrderItemId) AS NVARCHAR(1000)) + ', Partecipant_Id = ' + CAST(COALESCE(inserted.Partecipant_Id, deleted.Partecipant_Id) AS NVARCHAR(1000)) AS primary_key_description
    INTO audit.merge_log_details;

    DELETE FROM Fact.Corsi
    WHERE IsDeleted = CAST(1 AS BIT);

    UPDATE audit.tables
    SET lastupdated_local = lastupdated_staging
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    COMMIT TRANSACTION;

END;

GO

