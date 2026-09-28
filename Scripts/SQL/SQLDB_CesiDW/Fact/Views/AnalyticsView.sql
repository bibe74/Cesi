
CREATE   VIEW Fact.AnalyticsView
AS
SELECT
    -- Chiavi
    TD.PKDataVisita,
    TD.PKCliente,
    TD.Percorso,

    -- Campi per data warehouse
    CONVERT(VARBINARY(32), HASHBYTES('SHA2_256', CONCAT(
    	TD.PKDataVisita,
        TD.PKCliente,
        TD.Percorso,
    	' '
    ))) AS HistoricalHashKey,
    CONVERT(VARBINARY(32), HASHBYTES('SHA2_256', CONCAT(
        TD.NumeroVisite,
    	' '
    ))) AS ChangeHashKey,
    CURRENT_TIMESTAMP AS InsertDatetime,
    CURRENT_TIMESTAMP AS UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,
    
    -- Dimensioni
    	
    -- Misure
    TD.NumeroVisite
    	
FROM (
    
    SELECT
        --A.created_at,
        DV.PKData AS PKDataVisita,
        --A.email,
        C.PKCliente,
        A.path AS Percorso,
        A.NumeroVisite

    FROM Landing.MYSOLUTION_Analytics A
    INNER JOIN Dim.Data DV ON DV.PKData = A.created_at
    INNER JOIN Dim.Cliente C ON C.Email = A.email
        AND C.IsDeleted = CAST(0 AS BIT)
    WHERE A.IsDeleted = CAST(0 AS BIT)
        AND A.email <> N''

) TD;

GO

