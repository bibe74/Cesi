
/**
 * @table Landing.MYSOLUTION_Analytics
 * @description

*/

CREATE   VIEW Landing.MYSOLUTION_AnalyticsView
AS
WITH TableData
AS (
    SELECT
        CONVERT(DATE, created_at) AS created_at,
        COALESCE(email, N'') AS email,
        path,
        COUNT(1) AS NumeroVisite
      
      FROM MYSOLUTION.Analytics A
      GROUP BY CONVERT(DATE, created_at),
        email,
        path
)
SELECT
    TD.created_at,
    TD.email,
    TD.path,

    CONVERT(VARBINARY(32), HASHBYTES('SHA2_256', CONCAT(
        TD.created_at,
        TD.email,
        TD.path,
    	' '
    ))) AS HistoricalHashKey,
    CONVERT(VARBINARY(32), HASHBYTES('SHA2_256', CONCAT(
        TD.NumeroVisite,
    	' '
    ))) AS ChangeHashKey,
    CURRENT_TIMESTAMP AS InsertDatetime,
    CURRENT_TIMESTAMP AS UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    TD.NumeroVisite

FROM TableData TD;

GO

