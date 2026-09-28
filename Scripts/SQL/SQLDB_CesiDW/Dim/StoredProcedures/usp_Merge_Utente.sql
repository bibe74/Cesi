
CREATE PROCEDURE Dim.usp_Merge_Utente
AS
BEGIN

    SET NOCOUNT ON;

    BEGIN TRANSACTION 

    DECLARE @provider_name NVARCHAR(60) = N'MyDatamartReporting';
    DECLARE @full_table_name sysname = N'Landing.COMETA_Utente';

    MERGE INTO Dim.Utente AS TGT
    USING Staging.Utente (nolock) AS SRC
    ON SRC.IDUtente = TGT.IDUtente

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.IsDeleted = SRC.IsDeleted,
        
        TGT.Email = SRC.Email,
        TGT.RagioneSociale = SRC.RagioneSociale,
        TGT.Nome = SRC.Nome,
        TGT.Cognome = SRC.Cognome,
        TGT.Citta = SRC.Citta,
        TGT.PKCliente = SRC.PKCliente

    WHEN NOT MATCHED
      THEN INSERT (
        IDUtente,
        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
        Email,
        RagioneSociale,
        Nome,
        Cognome,
        Citta,
        PKCliente
      )
      VALUES (
        SRC.IDUtente,
        SRC.HistoricalHashKey,
        SRC.ChangeHashKey,
        SRC.HistoricalHashKeyASCII,
        SRC.ChangeHashKeyASCII,
        SRC.InsertDatetime,
        SRC.UpdateDatetime,
        SRC.IsDeleted,
        SRC.Email,
        SRC.RagioneSociale,
        SRC.Nome,
        SRC.Cognome,
        SRC.Citta,
        SRC.PKCliente
      )

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        $action AS merge_action,
        'Staging.Utente' AS full_olap_table_name,
        'IDUtente = ' + CAST(COALESCE(inserted.IDUtente, deleted.IDUtente) AS NVARCHAR(1000)) AS primary_key_description
    INTO audit.merge_log_details;

    --DELETE FROM Dim.Utente
    --WHERE IsDeleted = CAST(1 AS BIT);

    UPDATE audit.tables
    SET lastupdated_local = lastupdated_staging
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    COMMIT TRANSACTION;

END;

GO

