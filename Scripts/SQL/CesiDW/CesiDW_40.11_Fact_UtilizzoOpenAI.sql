USE CesiDW;
GO

/*
SET NOEXEC OFF;
--*/ SET NOEXEC ON;
GO

IF OBJECT_ID('dbo.seq_Fact_UtilizzoOpenAI', 'SO') IS NULL
BEGIN

    CREATE SEQUENCE dbo.seq_Fact_UtilizzoOpenAI START WITH 1;

END;
GO

CREATE OR ALTER VIEW Fact.UtilizzoOpenAIView
AS
SELECT
    -- Chiavi
    TD.IDUtilizzoOpenAI,

    -- Campi per data warehouse
    CONVERT(VARBINARY(32), HASHBYTES('SHA2_256', CONCAT(
        TD.IDUtilizzoOpenAI,
    	' '
    ))) AS HistoricalHashKey,
    CONVERT(VARBINARY(32), HASHBYTES('SHA2_256', CONCAT(
        TD.PKCliente,
        TD.IDThread,
        TD.IDConversazione,
        TD.PKDataConversazione,
        TD.Area,
        TD.ModalitaRisposta,
        TD.Modello,
        TD.IDVectorStorage,
        TD.InputTokens,
        TD.OutputTokens,
        TD.ReasoningTokens,
        TD.TotalTokens,
        TD.EstimatedTotalCostUSD,
    	' '
    ))) AS ChangeHashKey,
    CURRENT_TIMESTAMP AS InsertDatetime,
    CURRENT_TIMESTAMP AS UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,
    
    -- Dimensioni
    TD.PKCliente,
    TD.IDThread,
    TD.IDConversazione,
    TD.PKDataConversazione,
    TD.Area,
    TD.ModalitaRisposta,
    TD.Modello,
    TD.IDVectorStorage,
    	
    -- Misure
    TD.InputTokens,
    TD.OutputTokens,
    TD.ReasoningTokens,
    TD.TotalTokens,
    TD.EstimatedTotalCostUSD
    	
FROM (
    
    SELECT
        OAITU.Id AS IDUtilizzoOpenAI,
        --OAITU.email,
        C.PKCliente,
        OAITU.threadId AS IDThread,
        OAITU.conversationId AS IDConversazione,
        --OAITU.createdOn,
        DC.PKData AS PKDataConversazione,
        OAITU.assistantArea AS Area,
        OAITU.responseMode AS ModalitaRisposta,
        OAITU.model AS Modello,
        OAITU.vectorStorageId AS IDVectorStorage,
        OAITU.input_tokens AS InputTokens,
        OAITU.output_tokens AS OutputTokens,
        OAITU.reasoning_tokens AS ReasoningTokens,
        OAITU.total_tokens AS TotalTokens,
        OAITU.estimatedTotalCostUsd AS EstimatedTotalCostUSD

    FROM Landing.GPT_OpenAITokenUsage OAITU
    INNER JOIN Dim.Cliente C ON C.Email = OAITU.email
        AND C.IsDeleted = CAST(0 AS BIT)
    INNER JOIN Dim.Data DC ON DC.PKData = CONVERT(DATE, OAITU.createdOn)
    WHERE OAITU.IsDeleted = CAST(0 AS BIT)
        AND OAITU.email <> N''

) TD;
GO

--EXEC audit.usp_CreateScriptFromTableView @schemaName = 'Fact', @tableName = 'UtilizzoOpenAI';
GO

--DROP TABLE IF EXISTS Fact.UtilizzoOpenAI;
GO

IF OBJECT_ID('Fact.UtilizzoOpenAI', 'U') IS NULL
BEGIN
    SELECT TOP (0) 0 AS PKUtilizzoOpenAI,
        *
    INTO Fact.UtilizzoOpenAI
    FROM Fact.UtilizzoOpenAIView;

    ALTER TABLE Fact.UtilizzoOpenAI ALTER COLUMN PKUtilizzoOpenAI INT NOT NULL;
    ALTER TABLE Fact.UtilizzoOpenAI ADD CONSTRAINT DFT_PKUtilizzoOpenAI DEFAULT (NEXT VALUE FOR seq_Fact_UtilizzoOpenAI) FOR PKUtilizzoOpenAI;
    ALTER TABLE Fact.UtilizzoOpenAI ADD CONSTRAINT PK_Fact_UtilizzoOpenAI PRIMARY KEY CLUSTERED (PKUtilizzoOpenAI);

    --CREATE UNIQUE NONCLUSTERED INDEX IX_Fact_UtilizzoOpenAI_BusinessKey ON Fact.UtilizzoOpenAI (PKDataConversazione, PKCliente);
    --CREATE UNIQUE NONCLUSTERED INDEX IX_Fact_UtilizzoOpenAI_AlternateKey ON Fact.UtilizzoOpenAI ();

    ALTER TABLE Fact.UtilizzoOpenAI ADD CONSTRAINT FK_Fact_UtilizzoOpenAI_PKCliente FOREIGN KEY (PKCliente) REFERENCES Dim.Cliente (PKCliente);
    ALTER TABLE Fact.UtilizzoOpenAI ADD CONSTRAINT FK_Fact_UtilizzoOpenAI_PKDataConversazione FOREIGN KEY (PKDataConversazione) REFERENCES Dim.Data (PKData);

    ALTER SEQUENCE dbo.seq_Fact_UtilizzoOpenAI RESTART WITH 1;
END;
GO

CREATE OR ALTER PROCEDURE Fact.usp_Merge_UtilizzoOpenAI
AS
BEGIN
    SET NOCOUNT ON;

    MERGE INTO Fact.UtilizzoOpenAI AS TGT
    USING Fact.UtilizzoOpenAIView AS SRC ON (
         SRC.IDUtilizzoOpenAI = TGT.IDUtilizzoOpenAI 
    )

    WHEN MATCHED AND SRC.ChangeHashKey <> TGT.ChangeHashKey
      THEN UPDATE SET TGT.ChangeHashKey = SRC.ChangeHashKey, TGT.UpdateDatetime = SRC.UpdateDatetime, TGT.IsDeleted = SRC.IsDeleted, 
        TGT.PKCliente = SRC.PKCliente, TGT.IDThread = SRC.IDThread, TGT.IDConversazione = SRC.IDConversazione, TGT.PKDataConversazione = SRC.PKDataConversazione, TGT.Area = SRC.Area, TGT.ModalitaRisposta = SRC.ModalitaRisposta, TGT.Modello = SRC.Modello, TGT.IDVectorStorage = SRC.IDVectorStorage, TGT.InputTokens = SRC.InputTokens, TGT.OutputTokens = SRC.OutputTokens, TGT.ReasoningTokens = SRC.ReasoningTokens, TGT.TotalTokens = SRC.TotalTokens, TGT.EstimatedTotalCostUSD = SRC.EstimatedTotalCostUSD

    WHEN NOT MATCHED BY TARGET
      THEN INSERT (IDUtilizzoOpenAI, HistoricalHashKey, ChangeHashKey, InsertDatetime, UpdateDatetime, IsDeleted, PKCliente, IDThread, IDConversazione, PKDataConversazione, Area, ModalitaRisposta, Modello, IDVectorStorage, InputTokens, OutputTokens, ReasoningTokens, TotalTokens, EstimatedTotalCostUSD)
        VALUES (IDUtilizzoOpenAI, HistoricalHashKey, ChangeHashKey, InsertDatetime, UpdateDatetime, IsDeleted, PKCliente, IDThread, IDConversazione, PKDataConversazione, Area, ModalitaRisposta, Modello, IDVectorStorage, InputTokens, OutputTokens, ReasoningTokens, TotalTokens, EstimatedTotalCostUSD)

    WHEN NOT MATCHED BY SOURCE AND TGT.IsDeleted = CAST(0 AS BIT)
      THEN UPDATE SET TGT.ChangeHashKey = CONVERT(VARBINARY(32), 0),
        TGT.UpdateDatetime = CURRENT_TIMESTAMP,
        TGT.IsDeleted = CAST(1 AS BIT)
    
    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        CASE WHEN Inserted.IsDeleted = CAST(1 AS BIT) THEN N'DELETE' ELSE $action END AS merge_action,
        'Fact.UtilizzoOpenAI' AS full_olap_table_name,
        'IDUtilizzoOpenAI = ' + CAST(COALESCE(inserted.IDUtilizzoOpenAI, deleted.IDUtilizzoOpenAI) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END
GO

EXEC Fact.usp_Merge_UtilizzoOpenAI;
GO
