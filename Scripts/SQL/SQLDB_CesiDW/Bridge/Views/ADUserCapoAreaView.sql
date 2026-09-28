
/**
 * @table Bridge.ADUserCapoArea
 * @description Tabella di bridge ADUser / CapoArea
*/

CREATE   VIEW Bridge.ADUserCapoAreaView
AS
WITH CapiArea
AS (
    SELECT DISTINCT
        CapoArea
    FROM Dim.GruppoAgenti
)
SELECT
    ICA.ADUser,
    CA.CapoArea

FROM CapiArea CA
INNER JOIN Import.CapiArea ICA ON ICA.CapoArea = CA.CapoArea
    AND ICA.ADUser <> N''

UNION

SELECT
    A.ADUser,
    CA.CapoArea

FROM Import.Amministratori A
CROSS JOIN CapiArea CA

UNION

SELECT
    N'CESI\TestAgenti',
    ICA.CapoArea

FROM Import.CapiArea ICA
WHERE ICA.CapoArea = N'ATENEO S.A.S.';

GO

