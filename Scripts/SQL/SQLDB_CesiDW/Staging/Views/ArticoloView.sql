CREATE VIEW Staging.ArticoloView
AS
WITH ArticoloCategoriaMasterDettaglio
AS (
    SELECT
        ACM.id_articolo,
        ACM.Codice,
        ACM.CategoriaMaster,
        ACM.CodiceEsercizioMaster,
        ROW_NUMBER() OVER (PARTITION BY ACM.Codice ORDER BY ACM.Percentuale DESC) AS rn

    FROM Import.ArticoloCategoriaMaster ACM
),
DatiArticolo
AS (
    SELECT
        T.id_articolo,
        T.codice AS Codice,
        COALESCE(T.descrizione, N'') AS Descrizione,
        --T.id_cat_com_articolo,
        COALESCE(CCA.codice, CASE WHEN COALESCE(T.id_cat_com_articolo, 0) = 0 THEN N'' ELSE N'???' END) AS CodiceCategoriaCommerciale,
        COALESCE(CCA.descrizione, CASE WHEN COALESCE(T.id_cat_com_articolo, 0) = 0 THEN N'' ELSE N'???' END) AS CategoriaCommerciale,
        --T.id_cat_merceologica,
        COALESCE(CM.codice, CASE WHEN COALESCE(T.id_cat_merceologica, 0) = 0 THEN N'' ELSE N'???' END) AS CodiceCategoriaMerceologica,
        COALESCE(CM.descrizione, CASE WHEN COALESCE(T.id_cat_merceologica, 0) = 0 THEN N'' ELSE N'???' END) AS CategoriaMerceologica,
        COALESCE(T.des_breve, N'') AS DescrizioneBreve,
        COALESCE(ACMD.CategoriaMaster, N'') AS CategoriaMaster,
        COALESCE(ACMD.CodiceEsercizioMaster, N'') AS CodiceEsercizioMaster,
        COALESCE(MST.tipo, N'') AS Tipo

    FROM Landing.COMETA_Articolo T
    LEFT JOIN Landing.COMETA_CategoriaCommercialeArticolo CCA ON CCA.id_cat_com_articolo = T.id_cat_com_articolo
    LEFT JOIN Landing.COMETA_CategoriaMerceologica CM ON CM.id_cat_merceologica = T.id_cat_merceologica
    LEFT JOIN ArticoloCategoriaMasterDettaglio ACMD ON ACMD.Codice = T.codice
        AND ACMD.rn = 1
    LEFT JOIN Landing.COMETA_MySolutionTrascodifica MST ON MST.codice = T.codice
),
TableData
AS (
    SELECT
        DA.id_articolo,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            DA.id_articolo,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            DA.Codice,
            DA.Descrizione,
            DA.CodiceCategoriaCommerciale,
            DA.CategoriaCommerciale,
            DA.CodiceCategoriaMerceologica,
            DA.CategoriaMerceologica,
            DA.DescrizioneBreve,
            DA.CategoriaMaster,
            DA.CodiceEsercizioMaster,
            DA.Tipo,
            ABID.Data1,
            ABID.Data2,
            ABID.Data3,
            ABID.Data4,
            ABID.Data5,
            ABID.Data6,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        DA.Codice,
        DA.Descrizione,
        DA.CodiceCategoriaCommerciale,
        DA.CategoriaCommerciale,
        DA.CodiceCategoriaMerceologica,
        DA.CategoriaMerceologica,
        DA.DescrizioneBreve,
        DA.CategoriaMaster,
        DA.CodiceEsercizioMaster,
        CASE RIGHT(DA.Codice, 2)
          WHEN N'A1' THEN N'Annuale'
          WHEN N'A2' THEN N'Biennale'
          WHEN N'A3' THEN N'Triennale'
          WHEN N'A4' THEN N'Quadriennale'
          WHEN N'A5' THEN N'Quinquennale'
          WHEN N'A6' THEN N'6 anni'
          ELSE CASE RIGHT(DA.Codice, 3)
              WHEN N'A1F' THEN N'Annuale'
              WHEN N'A2F' THEN N'Biennale'
              WHEN N'A3F' THEN N'Triennale'
              WHEN N'A4F' THEN N'Quadriennale'
              WHEN N'A5F' THEN N'Quinquennale'
              WHEN N'A6F' THEN N'6 anni'
              ELSE N''
            END
        END AS Fatturazione,
        DA.Tipo,
        COALESCE(ABID.Data1, N'') AS Data1,
        COALESCE(ABID.Data2, N'') AS Data2,
        COALESCE(ABID.Data3, N'') AS Data3,
        COALESCE(ABID.Data4, N'') AS Data4,
        COALESCE(ABID.Data5, N'') AS Data5,
        COALESCE(ABID.Data6, N'') AS Data6,
        CASE WHEN DA.Codice LIKE N'MIA%L' THEN LEFT(RIGHT(DA.Codice, 2), 1) ELSE N'' END AS LivelloMIA,
        CASE
          WHEN DA.Tipo = N'FISCO' THEN 'MYS'
          WHEN DA.Tipo = N'FULL' THEN 'MYS'
          WHEN DA.Tipo = N'LAVORO' THEN 'MYS'
          WHEN DA.Tipo = N'MIAFISCO' THEN 'MIA'
          WHEN DA.Tipo = N'MIAFULL' THEN 'MIA'
          WHEN DA.Tipo = N'MIALAVORO' THEN 'MIA'
          WHEN DA.Codice LIKE N'MS-AB%' THEN 'B&C'
          WHEN DA.Codice LIKE N'MS-CDI%' THEN 'B&C'
          ELSE N''
        END AS MacroTipoAbbonamento

    FROM DatiArticolo DA
    LEFT JOIN Landing.COMETAINTEGRATION_ArticleBIData ABID ON ABID.ArticleID = DA.id_articolo
)
SELECT
    -- Chiavi
    TD.id_articolo,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Altri campi
    TD.Codice,
    TD.Descrizione,
    TD.CodiceCategoriaCommerciale,
    TD.CategoriaCommerciale,
    TD.CodiceCategoriaMerceologica,
    TD.CategoriaMerceologica,
    TD.DescrizioneBreve,
    TD.CategoriaMaster,
    TD.CodiceEsercizioMaster,
    TD.Fatturazione,
    TD.Tipo,
    TD.Data1,
    TD.Data2,
    TD.Data3,
    TD.Data4,
    TD.Data5,
    TD.Data6,
    TD.LivelloMIA,
    TD.MacroTipoAbbonamento

FROM TableData TD;

GO

