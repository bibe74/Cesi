
CREATE   PROCEDURE Import.usp_Reload_ComuneCAPAgente
AS
BEGIN

    SET NOCOUNT ON;

    BEGIN TRANSACTION;

    BEGIN TRY

        TRUNCATE TABLE Import.ComuneCAPAgente;

        INSERT INTO Import.ComuneCAPAgente (
            IDProvincia,
            Comune,
            CAP,
            Agente,
            CapoArea
        )
        SELECT
            IDProvincia,
            Comune,
            CAP,
            Agente,
            CapoArea

        FROM Import.ComuneCAPAgenteView
        ORDER BY Comune,
            CAP;

        COMMIT TRANSACTION;

    END TRY
    BEGIN CATCH

        RAISERROR('Errore nell''importazione di ComuneCAPAgente', 16, 1);

        ROLLBACK;

    END CATCH

END;

GO

