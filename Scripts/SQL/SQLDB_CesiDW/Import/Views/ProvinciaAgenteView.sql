
CREATE   VIEW Import.ProvinciaAgenteView
AS
WITH AgenteImport
AS (
    SELECT DISTINCT
        PA.Agente,
        CASE WHEN PA.Agente = N'CESI' THEN N'DIREZIONALI' ELSE PA.Agente END AS AgenteTrascodifica

    FROM Import.stg_ProvinciaAgente PA
)
SELECT
    PA.CodiceProvincia AS IDProvincia,
    PA.Agente,
    CA.CapoArea

FROM Import.stg_ProvinciaAgente PA
INNER JOIN AgenteImport AI ON AI.Agente = PA.Agente
INNER JOIN Import.CapiArea ICA ON ICA.AgenteBudget = AI.AgenteTrascodifica
INNER JOIN Dim.CapoArea CA ON CA.CapoArea = ICA.CapoArea;

GO

