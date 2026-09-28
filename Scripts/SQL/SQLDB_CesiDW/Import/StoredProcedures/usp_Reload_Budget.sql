
CREATE   PROCEDURE Import.usp_Reload_Budget
AS
BEGIN

    SET NOCOUNT ON;

    BEGIN TRANSACTION;

    BEGIN TRY

        TRUNCATE TABLE Import.Budget;

        INSERT INTO Import.Budget (
            PKDataInizioMese,
            CapoArea,
            ImportoBudgetNuoveVendite,
            ImportoBudgetRinnovi
        )
        SELECT
            PKDataInizioMese,
            CapoArea,
            ImportoBudgetNuoveVendite,
            ImportoBudgetRinnovi

        FROM Import.BudgetView
        ORDER BY PKDataInizioMese,
            CapoArea;

        COMMIT TRANSACTION;

    END TRY
    BEGIN CATCH

        RAISERROR('Errore nell''importazione del budget', 16, 1);

        ROLLBACK;

    END CATCH

END;

GO

