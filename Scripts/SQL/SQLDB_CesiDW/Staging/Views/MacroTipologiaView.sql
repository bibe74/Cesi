
CREATE VIEW Staging.MacroTipologiaView
AS
WITH TableData
AS (
    SELECT DISTINCT
        T.MacroTipologia,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            T.MacroTipologia,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            T.IsValidaPerBudgetNuoveVendite,
            T.IsValidaPerBudgetRinnovi,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        T.IsValidaPerBudgetNuoveVendite,
        T.IsValidaPerBudgetRinnovi

    FROM Import.Libero2MacroTipologia T
)
SELECT
    -- Chiavi
    TD.MacroTipologia,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Altri campi
    TD.IsValidaPerBudgetNuoveVendite,
    TD.IsValidaPerBudgetRinnovi

FROM TableData TD;

GO

