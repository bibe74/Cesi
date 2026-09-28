
CREATE PROCEDURE Dim.usp_Merge_GruppoAgenti
AS
BEGIN

    SET NOCOUNT ON;

    BEGIN TRANSACTION 

    DECLARE @provider_name NVARCHAR(60) = N'MyDatamartReporting';
    DECLARE @full_table_name sysname = N'Landing.COMETA_Gruppo_Agenti';

    MERGE INTO Dim.GruppoAgenti AS TGT
    USING Staging.GruppoAgenti (nolock) AS SRC
    ON SRC.id_gruppo_agenti = TGT.id_gruppo_agenti

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.IsDeleted = SRC.IsDeleted,
        
        TGT.IDGruppoAgenti = SRC.IDGruppoAgenti,
        TGT.GruppoAgenti = SRC.GruppoAgenti,
        TGT.CapoArea = SRC.CapoArea,
        TGT.Agente = SRC.Agente,
        TGT.Subagente = SRC.Subagente

    WHEN NOT MATCHED
      THEN INSERT (
        id_gruppo_agenti,
        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
        IDGruppoAgenti,
        GruppoAgenti,
        CapoArea,
        Agente,
        Subagente
      )
      VALUES (
        SRC.id_gruppo_agenti,
        SRC.HistoricalHashKey,
        SRC.ChangeHashKey,
        SRC.HistoricalHashKeyASCII,
        SRC.ChangeHashKeyASCII,
        SRC.InsertDatetime,
        SRC.UpdateDatetime,
        SRC.IsDeleted,
        SRC.IDGruppoAgenti,
        SRC.GruppoAgenti,
        SRC.CapoArea,
        SRC.Agente,
        SRC.Subagente
      )

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        $action AS merge_action,
        'Staging.GruppoAgenti' AS full_olap_table_name,
        'id_gruppo_agenti = ' + CAST(COALESCE(inserted.id_gruppo_agenti, deleted.id_gruppo_agenti) AS NVARCHAR(1000)) AS primary_key_description
    INTO audit.merge_log_details;

    --DELETE FROM Dim.GruppoAgenti
    --WHERE IsDeleted = CAST(1 AS BIT);

    UPDATE GA
    SET GA.PKCapoArea = CA.PKCapoArea
    FROM Dim.GruppoAgenti GA
    INNER JOIN Dim.CapoArea CA ON CA.CapoArea = GA.CapoArea;

    UPDATE audit.tables
    SET lastupdated_local = lastupdated_staging
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    COMMIT TRANSACTION;

END;

GO

