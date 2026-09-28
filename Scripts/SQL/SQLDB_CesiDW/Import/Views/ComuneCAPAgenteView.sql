
CREATE   VIEW Import.ComuneCAPAgenteView
AS
WITH AgenteImport
AS (
    SELECT DISTINCT
        PCCA.Agente,
        CASE PCCA.Agente
          WHEN N'Vampirelli' THEN N'VAMPIRELLI'
          WHEN N'Conzadori' THEN N'TUROLLA'
        END AS AgenteTrascodifica,
        CASE PCCA.Agente
          WHEN N'Vampirelli' THEN N'ANTONIO VAMPIRELLI'
          WHEN N'Conzadori' THEN N'TUROLLA PAOLA'
        END AS CapoArea
    FROM Import.stg_ProvinciaComuneCAPAgente PCCA
),
AgenteImportCapoArea
AS (
    SELECT DISTINCT
        AI.Agente,
        AI.AgenteTrascodifica,
        GA.CapoArea
    FROM Dim.GruppoAgenti GA
    INNER JOIN AgenteImport AI ON GA.CapoArea = AI.CapoArea
)
SELECT
    N'MI' AS IDProvincia,
    PCCA.Comune,
    PCCA.CAP,
    AICA.AgenteTrascodifica AS Agente,
    AICA.CapoArea

FROM Import.stg_ProvinciaComuneCAPAgente PCCA
INNER JOIN AgenteImportCapoArea AICA ON AICA.Agente = PCCA.Agente;

GO

