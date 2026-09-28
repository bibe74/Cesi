
CREATE   VIEW Import.BudgetView
AS
SELECT
    D.PKData AS PKDataInizioMese,
    CA.CapoArea,
    COALESCE(SB.BudgetNuoveVendite, 0.0) AS ImportoBudgetNuoveVendite,
    COALESCE(SB.BudgetRinnovi, 0.0) AS ImportoBudgetRinnovi

FROM Import.stg_Budget SB
INNER JOIN Import.CapiArea CA ON CA.AgenteBudget = SB.Agente
INNER JOIN Dim.Data D ON D.PKData = SB.DataInizioMese;

GO

