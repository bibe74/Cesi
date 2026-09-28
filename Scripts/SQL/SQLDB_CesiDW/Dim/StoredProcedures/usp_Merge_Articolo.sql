CREATE PROCEDURE Dim.usp_Merge_Articolo
AS
BEGIN

    SET NOCOUNT ON;

    BEGIN TRANSACTION 

    DECLARE @provider_name NVARCHAR(60) = N'MyDatamartReporting';
    DECLARE @full_table_name sysname = N'Landing.COMETA_Articolo';

    MERGE INTO Dim.Articolo AS TGT
    USING Staging.Articolo (nolock) AS SRC
    ON SRC.id_articolo = TGT.id_articolo

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.IsDeleted = SRC.IsDeleted,
        
        TGT.Codice = SRC.Codice,
        TGT.Descrizione = SRC.Descrizione,
        TGT.CodiceCategoriaCommerciale = SRC.CodiceCategoriaCommerciale,
        TGT.CategoriaCommerciale = SRC.CategoriaCommerciale,
        TGT.CodiceCategoriaMerceologica = SRC.CodiceCategoriaMerceologica,
        TGT.CategoriaMerceologica = SRC.CategoriaMerceologica,
        TGT.DescrizioneBreve = SRC.DescrizioneBreve,
        TGT.CategoriaMaster = SRC.CategoriaMaster,
        TGT.CodiceEsercizioMaster = SRC.CodiceEsercizioMaster,
        TGT.Fatturazione = SRC.Fatturazione,
        TGT.Tipo = SRC.Tipo,
        TGT.Data1 = SRC.Data1,
        TGT.Data2 = SRC.Data2,
        TGT.Data3 = SRC.Data3,
        TGT.Data4 = SRC.Data4,
        TGT.Data5 = SRC.Data5,
        TGT.Data6 = SRC.Data6,
        TGT.LivelloMIA = SRC.LivelloMIA,
        TGT.MacroTipoAbbonamento = SRC.MacroTipoAbbonamento

    WHEN NOT MATCHED
      THEN INSERT (
        id_articolo,
        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
        Codice,
        Descrizione,
        CodiceCategoriaCommerciale,
        CategoriaCommerciale,
        CodiceCategoriaMerceologica,
        CategoriaMerceologica,
        DescrizioneBreve,
        CategoriaMaster,
        CodiceEsercizioMaster,
        Fatturazione,
        Tipo,
        Data1,
        Data2,
        Data3,
        Data4,
        Data5,
        Data6,
        LivelloMIA,
        MacroTipoAbbonamento
      )
      VALUES (
        SRC.id_articolo,
        SRC.HistoricalHashKey,
        SRC.ChangeHashKey,
        SRC.HistoricalHashKeyASCII,
        SRC.ChangeHashKeyASCII,
        SRC.InsertDatetime,
        SRC.UpdateDatetime,
        SRC.IsDeleted,
        SRC.Codice,
        SRC.Descrizione,
        SRC.CodiceCategoriaCommerciale,
        SRC.CategoriaCommerciale,
        SRC.CodiceCategoriaMerceologica,
        SRC.CategoriaMerceologica,
        SRC.DescrizioneBreve,
        SRC.CategoriaMaster,
        SRC.CodiceEsercizioMaster,
        SRC.Fatturazione,
        SRC.Tipo,
        SRC.Data1,
        SRC.Data2,
        SRC.Data3,
        SRC.Data4,
        SRC.Data5,
        SRC.Data6,
        SRC.LivelloMIA,
        SRC.MacroTipoAbbonamento
      )

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        $action AS merge_action,
        'Staging.Articolo' AS full_olap_table_name,
        'id_articolo = ' + CAST(COALESCE(inserted.id_articolo, deleted.id_articolo) AS NVARCHAR(1000)) AS primary_key_description
    INTO audit.merge_log_details;

    --DELETE FROM Dim.Articolo
    --WHERE IsDeleted = CAST(1 AS BIT);

    UPDATE audit.tables
    SET lastupdated_local = lastupdated_staging
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    COMMIT TRANSACTION;

END;

GO

