
CREATE   PROCEDURE Import.usp_Reload_ProvinciaAgente
AS
BEGIN

    SET NOCOUNT ON;

    BEGIN TRANSACTION;

    BEGIN TRY

        TRUNCATE TABLE Import.ProvinciaAgente;

        INSERT INTO Import.ProvinciaAgente (
            IDProvincia,
            Agente,
            CapoArea
        )
        SELECT
            IDProvincia,
            Agente,
            CapoArea

        FROM Import.ProvinciaAgenteView
        ORDER BY IDProvincia,
            CapoArea;

        COMMIT TRANSACTION;

    END TRY
    BEGIN CATCH

        RAISERROR('Errore nell''importazione di ProvinciaAgente', 16, 1);

        ROLLBACK;

    END CATCH

END;

GO

