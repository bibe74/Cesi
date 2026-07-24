USE CesiDW;
GO

/*
SET NOEXEC OFF;
--*/ SET NOEXEC ON;
GO

IF OBJECT_ID('dbo.seq_Fact_Analytics', 'SO') IS NULL
BEGIN

    CREATE SEQUENCE dbo.seq_Fact_Analytics START WITH 1;

END;
GO

CREATE OR ALTER VIEW Fact.AnalyticsView
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

--EXEC audit.usp_CreateScriptFromTableView @schemaName = 'Fact', @tableName = 'Analytics';
GO

--DROP TABLE IF EXISTS Fact.Analytics;
GO

IF OBJECT_ID('Fact.Analytics', 'U') IS NULL
BEGIN
    SELECT TOP (0) 0 AS PKAnalytics,
        *
    INTO Fact.Analytics
    FROM Fact.AnalyticsView;

    ALTER TABLE Fact.Analytics ALTER COLUMN PKAnalytics INT NOT NULL;
    ALTER TABLE Fact.Analytics ADD CONSTRAINT DFT_PKAnalytics DEFAULT (NEXT VALUE FOR seq_Fact_Analytics) FOR PKAnalytics;
    ALTER TABLE Fact.Analytics ADD CONSTRAINT PK_Fact_Analytics PRIMARY KEY CLUSTERED (PKAnalytics);

    CREATE UNIQUE NONCLUSTERED INDEX IX_Fact_Analytics_BusinessKey ON Fact.Analytics (PKDataVisita, PKCliente, Percorso);
    --CREATE UNIQUE NONCLUSTERED INDEX IX_Fact_Analytics_AlternateKey ON Fact.Analytics ();

    ALTER TABLE Fact.Analytics ADD CONSTRAINT FK_Fact_Analytics_PKDataVisita FOREIGN KEY (PKDataVisita) REFERENCES Dim.Data (PKData);
    ALTER TABLE Fact.Analytics ADD CONSTRAINT FK_Fact_Analytics_PKCliente FOREIGN KEY (PKCliente) REFERENCES Dim.Cliente (PKCliente);

    ALTER SEQUENCE dbo.seq_Fact_Analytics RESTART WITH 1;
END;
GO

CREATE OR ALTER PROCEDURE Fact.usp_Merge_Analytics
AS
BEGIN
    SET NOCOUNT ON;

    MERGE INTO Fact.Analytics AS TGT
    USING Fact.AnalyticsView AS SRC ON (
         SRC.PKDataVisita = TGT.PKDataVisita AND SRC.PKCliente = TGT.PKCliente AND SRC.Percorso = TGT.Percorso 
    )

    WHEN MATCHED AND SRC.ChangeHashKey <> TGT.ChangeHashKey
      THEN UPDATE SET TGT.ChangeHashKey = SRC.ChangeHashKey, TGT.UpdateDatetime = SRC.UpdateDatetime, TGT.IsDeleted = SRC.IsDeleted, 
        TGT.NumeroVisite = SRC.NumeroVisite

    WHEN NOT MATCHED BY TARGET
      THEN INSERT (PKDataVisita, PKCliente, Percorso, HistoricalHashKey, ChangeHashKey, InsertDatetime, UpdateDatetime, IsDeleted, NumeroVisite)
        VALUES (PKDataVisita, PKCliente, Percorso, HistoricalHashKey, ChangeHashKey, InsertDatetime, UpdateDatetime, IsDeleted, NumeroVisite)

    WHEN NOT MATCHED BY SOURCE AND TGT.IsDeleted = CAST(0 AS BIT)
      THEN UPDATE SET TGT.ChangeHashKey = CONVERT(VARBINARY(32), 0),
        TGT.UpdateDatetime = CURRENT_TIMESTAMP,
        TGT.IsDeleted = CAST(1 AS BIT)
    
    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        CASE WHEN Inserted.IsDeleted = CAST(1 AS BIT) THEN N'DELETE' ELSE $action END AS merge_action,
        'Fact.Analytics' AS full_olap_table_name,
        'PKDataVisita = ' + CAST(COALESCE(inserted.PKDataVisita, deleted.PKDataVisita) AS NVARCHAR) + 'PKCliente = ' + CAST(COALESCE(inserted.PKCliente, deleted.PKCliente) AS NVARCHAR) + 'Percorso = ' + CAST(COALESCE(inserted.Percorso, deleted.Percorso) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END
GO

EXEC Fact.usp_Merge_Analytics;
GO
