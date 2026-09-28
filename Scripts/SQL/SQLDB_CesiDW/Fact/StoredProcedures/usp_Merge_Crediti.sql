
CREATE PROCEDURE Fact.usp_Merge_Crediti
AS
BEGIN

    SET NOCOUNT ON;

    BEGIN TRANSACTION 

    DECLARE @provider_name NVARCHAR(60) = N'Webinars';
    DECLARE @full_table_name sysname = N'Import.Crediti';

    MERGE INTO Fact.Crediti AS TGT
    USING Staging.Crediti (nolock) AS SRC
    ON SRC.ID = TGT.ID

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.IsDeleted = SRC.IsDeleted,
        TGT.IDCorso = SRC.IDCorso,
        TGT.Nome = SRC.Nome,
        TGT.Cognome = SRC.Cognome,
        TGT.CodiceFiscale = SRC.CodiceFiscale,
        TGT.EMail = SRC.EMail,
        TGT.Professione = SRC.Professione,
        TGT.Ordine = SRC.Ordine,
        TGT.EnteAccreditante = SRC.EnteAccreditante,
        TGT.TipoCrediti = SRC.TipoCrediti,
        TGT.StatoCrediti = SRC.StatoCrediti,
        TGT.CodiceMateria = SRC.CodiceMateria,
        TGT.Crediti = SRC.Crediti

    WHEN NOT MATCHED
      THEN INSERT (
        --PKCrediti,
        ID,
        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
        IDCorso,
        PKCorso,
        Nome,
        Cognome,
        CodiceFiscale,
        EMail,
        Professione,
        Ordine,
        PKDataCreazione,
        EnteAccreditante,
        TipoCrediti,
        StatoCrediti,
        CodiceMateria,
        Crediti
    ) VALUES (
        ID,
        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
        IDCorso,
        PKCorso,
        Nome,
        Cognome,
        CodiceFiscale,
        EMail,
        Professione,
        Ordine,
        PKDataCreazione,
        EnteAccreditante,
        TipoCrediti,
        StatoCrediti,
        CodiceMateria,
        Crediti
    )

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        $action AS merge_action,
        'Staging.Crediti' AS full_olap_table_name,
        'ID = ' + CAST(COALESCE(inserted.ID, deleted.ID) AS NVARCHAR(1000)) AS primary_key_description
    INTO audit.merge_log_details;

    DELETE FROM Fact.Crediti
    WHERE IsDeleted = CAST(1 AS BIT);

    UPDATE audit.tables
    SET lastupdated_local = lastupdated_staging
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    COMMIT TRANSACTION;

END;

GO

