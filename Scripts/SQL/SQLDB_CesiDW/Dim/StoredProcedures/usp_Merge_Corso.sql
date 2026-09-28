
CREATE PROCEDURE Dim.usp_Merge_Corso
AS
BEGIN

    SET NOCOUNT ON;

    BEGIN TRANSACTION 

    DECLARE @provider_name NVARCHAR(60) = N'Webinars';
    DECLARE @full_table_name sysname = N'Landing.COMETA_Corso';

    MERGE INTO Dim.Corso AS TGT
    USING Staging.Corso (nolock) AS SRC
    ON SRC.IDCorso = TGT.IDCorso

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.IsDeleted = SRC.IsDeleted,
        
        TGT.IDCorso = SRC.IDCorso,
        TGT.Corso = SRC.Corso,
        TGT.TipoCorso = SRC.TipoCorso,
        TGT.Giornata = SRC.Giornata,
        TGT.PKDataInizioCorso = SRC.PKDataInizioCorso,
        TGT.OraInizioCorso = SRC.OraInizioCorso

    WHEN NOT MATCHED
      THEN INSERT (
        IDCorso,
        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
        Corso,
        TipoCorso,
        Giornata,
        PKDataInizioCorso,
        OraInizioCorso
      )
      VALUES (
        SRC.IDCorso,
        SRC.HistoricalHashKey,
        SRC.ChangeHashKey,
        SRC.HistoricalHashKeyASCII,
        SRC.ChangeHashKeyASCII,
        SRC.InsertDatetime,
        SRC.UpdateDatetime,
        SRC.IsDeleted,
        SRC.Corso,
        SRC.TipoCorso,
        SRC.Giornata,
        SRC.PKDataInizioCorso,
        SRC.OraInizioCorso
      )

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        $action AS merge_action,
        'Staging.Corso' AS full_olap_table_name,
        'IDCorso = ' + CAST(COALESCE(inserted.IDCorso, deleted.IDCorso) AS NVARCHAR(1000)) AS primary_key_description
    INTO audit.merge_log_details;

    --DELETE FROM Dim.Corso
    --WHERE IsDeleted = CAST(1 AS BIT);

    UPDATE audit.tables
    SET lastupdated_local = lastupdated_staging
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    COMMIT TRANSACTION;

END;

GO

