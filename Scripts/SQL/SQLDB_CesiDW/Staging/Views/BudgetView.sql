
CREATE VIEW Staging.BudgetView
AS
WITH TableData
AS (
    SELECT
        DIM.PKData,
        CA.PKCapoArea,
        MT.PKMacroTipologia,

        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            DIM.PKData,
            CA.PKCapoArea,
            MT.PKMacroTipologia,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            B.ImportoBudgetNuoveVendite,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,

        B.ImportoBudgetNuoveVendite,
        NULL AS ImportoBudgetRinnovi,
        B.ImportoBudgetNuoveVendite AS ImportoBudget

    FROM Import.Budget B
    INNER JOIN Dim.Data DIM ON DIM.PKData = B.PKDataInizioMese
    INNER JOIN Dim.CapoArea CA ON CA.CapoArea = B.CapoArea
    INNER JOIN Dim.MacroTipologia MT ON MT.IsValidaPerBudgetNuoveVendite = CAST(1 AS BIT)

    UNION ALL

    SELECT
        DIM.PKData,
        CA.PKCapoArea,
        MT.PKMacroTipologia,

        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            DIM.PKData,
            CA.PKCapoArea,
            MT.PKMacroTipologia,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            B.ImportoBudgetNuoveVendite,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,

        NULL AS ImportoBudgetNuoveVendite,
        B.ImportoBudgetRinnovi,
        B.ImportoBudgetRinnovi AS ImportoBudget

    FROM Import.Budget B
    INNER JOIN Dim.Data DIM ON DIM.PKData = B.PKDataInizioMese
    INNER JOIN Dim.CapoArea CA ON CA.CapoArea = B.CapoArea
    INNER JOIN Dim.MacroTipologia MT ON MT.IsValidaPerBudgetRinnovi = CAST(1 AS BIT)
)
SELECT
    -- Chiavi
    TD.PKData,
    TD.PKCapoArea,
    TD.PKMacroTipologia,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Misure
    TD.ImportoBudgetNuoveVendite,
    TD.ImportoBudgetRinnovi,
    TD.ImportoBudget

FROM TableData TD;

GO

