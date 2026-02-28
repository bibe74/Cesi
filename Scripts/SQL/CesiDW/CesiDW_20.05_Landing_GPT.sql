USE CesiDW;
GO

/*
SET NOEXEC OFF;
--*/ SET NOEXEC ON;
GO

/**
 * @table Landing.GPT_OpenAICausale
 * @description 

 * @depends GPT.OpenAICausale

SELECT TOP 100 * FROM GPT.OpenAICausale;
*/

IF OBJECT_ID('Landing.GPT_OpenAICausaleView', 'V') IS NULL EXEC('CREATE VIEW Landing.GPT_OpenAICausaleView AS SELECT 1 AS fld;');
GO

ALTER VIEW Landing.GPT_OpenAICausaleView
AS
WITH TableData
AS (
    SELECT

        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            Id,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            Codice,
            Descrizione,
            Segno,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        Id,
        Codice,
        Descrizione,
        Segno

    FROM GPT.OpenAICausale
)
SELECT
    -- Chiavi
    0+TD.Id AS Id,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.Codice,
    TD.Descrizione,
    TD.Segno

FROM TableData TD;
GO

--DROP TABLE IF EXISTS Landing.GPT_OpenAICausale;
GO

IF OBJECT_ID(N'Landing.GPT_OpenAICausale', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Landing.GPT_OpenAICausale FROM Landing.GPT_OpenAICausaleView;

    ALTER TABLE Landing.GPT_OpenAICausale ALTER COLUMN Id INT NOT NULL;

    ALTER TABLE Landing.GPT_OpenAICausale ADD CONSTRAINT PK_Landing_GPT_OpenAICausale PRIMARY KEY CLUSTERED (UpdateDatetime, Id);

    CREATE UNIQUE NONCLUSTERED INDEX IX_GPT_OpenAICausale_BusinessKey ON Landing.GPT_OpenAICausale (Id);
    CREATE UNIQUE NONCLUSTERED INDEX IX_GPT_OpenAICausale_AlternateKey ON Landing.GPT_OpenAICausale (Codice);
END;
GO

IF OBJECT_ID('GPT.usp_Merge_OpenAICausale', 'P') IS NULL EXEC('CREATE PROCEDURE GPT.usp_Merge_OpenAICausale AS RETURN 0;');
GO

ALTER PROCEDURE GPT.usp_Merge_OpenAICausale
AS
BEGIN
    SET NOCOUNT ON;

    MERGE INTO Landing.GPT_OpenAICausale AS TGT
    USING Landing.GPT_OpenAICausaleView (nolock) AS SRC
    ON SRC.Id = TGT.Id

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.Codice = SRC.Codice,
        TGT.Descrizione = SRC.Descrizione,
        TGT.Segno = SRC.Segno

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        Id,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        Codice,
        Descrizione,
        Segno
      )

    WHEN NOT MATCHED BY SOURCE
        AND TGT.IsDeleted = CAST(0 AS BIT)
      THEN UPDATE
        SET TGT.IsDeleted = CAST(1 AS BIT),
        TGT.UpdateDatetime = CURRENT_TIMESTAMP,
        TGT.ChangeHashKey = CONVERT(VARBINARY(20), ''),
        TGT.ChangeHashKeyASCII = ''

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        CASE WHEN Inserted.IsDeleted = CAST(1 AS BIT) THEN N'DELETE' ELSE $action END AS merge_action,
        'Landing.GPT_OpenAICausale' AS full_olap_table_name,
        'Id = ' + CAST(COALESCE(inserted.Id, deleted.Id) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;
GO

EXEC GPT.usp_Merge_OpenAICausale;
GO

SELECT * FROM Landing.GPT_OpenAICausale;
GO

/**
 * @table Landing.GPT_OpenAICliente
 * @description 

 * @depends GPT.OpenAICliente

SELECT TOP 100 * FROM GPT.OpenAICliente;
*/

IF OBJECT_ID('Landing.GPT_OpenAIClienteView', 'V') IS NULL EXEC('CREATE VIEW Landing.GPT_OpenAIClienteView AS SELECT 1 AS fld;');
GO

ALTER VIEW Landing.GPT_OpenAIClienteView
AS
WITH TableData
AS (
    SELECT

        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            Id,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            Email,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        Id,
        Email

    FROM GPT.OpenAICliente
)
SELECT
    -- Chiavi
    0+TD.Id AS Id,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.Email

FROM TableData TD;
GO

--DROP TABLE IF EXISTS Landing.GPT_OpenAICliente;
GO

IF OBJECT_ID(N'Landing.GPT_OpenAICliente', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Landing.GPT_OpenAICliente FROM Landing.GPT_OpenAIClienteView;

    ALTER TABLE Landing.GPT_OpenAICliente ALTER COLUMN Id INT NOT NULL;

    ALTER TABLE Landing.GPT_OpenAICliente ADD CONSTRAINT PK_Landing_GPT_OpenAICliente PRIMARY KEY CLUSTERED (UpdateDatetime, Id);

    CREATE UNIQUE NONCLUSTERED INDEX IX_GPT_OpenAICliente_BusinessKey ON Landing.GPT_OpenAICliente (Id);
    CREATE UNIQUE NONCLUSTERED INDEX IX_GPT_OpenAICliente_AlternateKey ON Landing.GPT_OpenAICliente (Email);
END;
GO

IF OBJECT_ID('GPT.usp_Merge_OpenAICliente', 'P') IS NULL EXEC('CREATE PROCEDURE GPT.usp_Merge_OpenAICliente AS RETURN 0;');
GO

ALTER PROCEDURE GPT.usp_Merge_OpenAICliente
AS
BEGIN
    SET NOCOUNT ON;

    MERGE INTO Landing.GPT_OpenAICliente AS TGT
    USING Landing.GPT_OpenAIClienteView (nolock) AS SRC
    ON SRC.Id = TGT.Id

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.Email = SRC.Email

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        Id,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        Email
      )

    WHEN NOT MATCHED BY SOURCE
        AND TGT.IsDeleted = CAST(0 AS BIT)
      THEN UPDATE
        SET TGT.IsDeleted = CAST(1 AS BIT),
        TGT.UpdateDatetime = CURRENT_TIMESTAMP,
        TGT.ChangeHashKey = CONVERT(VARBINARY(20), ''),
        TGT.ChangeHashKeyASCII = ''

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        CASE WHEN Inserted.IsDeleted = CAST(1 AS BIT) THEN N'DELETE' ELSE $action END AS merge_action,
        'Landing.GPT_OpenAICliente' AS full_olap_table_name,
        'Id = ' + CAST(COALESCE(inserted.Id, deleted.Id) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;
GO

EXEC GPT.usp_Merge_OpenAICliente;
GO

SELECT * FROM Landing.GPT_OpenAICliente;
GO

/**
 * @table Landing.GPT_OpenAICredito
 * @description 

 * @depends GPT.OpenAICredito

SELECT TOP 100 * FROM GPT.OpenAICredito;
*/

IF OBJECT_ID('Landing.GPT_OpenAICreditoView', 'V') IS NULL EXEC('CREATE VIEW Landing.GPT_OpenAICreditoView AS SELECT 1 AS fld;');
GO

ALTER VIEW Landing.GPT_OpenAICreditoView
AS
WITH TableData
AS (
    SELECT

        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            Id,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            ClienteId,
            CausaleId,
            PartitaId,
            MessageId,
            Documento,
            DataMovimento,
            Quantita,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        Id,
        ClienteId,
        CausaleId,
        PartitaId,
        MessageId,
        Documento,
        DataMovimento,
        Quantita

    FROM GPT.OpenAICredito
)
SELECT
    -- Chiavi
    0+TD.Id AS Id,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.ClienteId,
    TD.CausaleId,
    TD.PartitaId,
    TD.MessageId,
    TD.Documento,
    TD.DataMovimento,

    -- Misure
    TD.Quantita

FROM TableData TD;
GO

--DROP TABLE IF EXISTS Landing.GPT_OpenAICredito;
GO

IF OBJECT_ID(N'Landing.GPT_OpenAICredito', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Landing.GPT_OpenAICredito FROM Landing.GPT_OpenAICreditoView;

    ALTER TABLE Landing.GPT_OpenAICredito ALTER COLUMN Id INT NOT NULL;

    ALTER TABLE Landing.GPT_OpenAICredito ADD CONSTRAINT PK_Landing_GPT_OpenAICredito PRIMARY KEY CLUSTERED (UpdateDatetime, Id);

    CREATE UNIQUE NONCLUSTERED INDEX IX_GPT_OpenAICredito_BusinessKey ON Landing.GPT_OpenAICredito (Id);
END;
GO

IF OBJECT_ID('GPT.usp_Merge_OpenAICredito', 'P') IS NULL EXEC('CREATE PROCEDURE GPT.usp_Merge_OpenAICredito AS RETURN 0;');
GO

ALTER PROCEDURE GPT.usp_Merge_OpenAICredito
AS
BEGIN
    SET NOCOUNT ON;

    MERGE INTO Landing.GPT_OpenAICredito AS TGT
    USING Landing.GPT_OpenAICreditoView (nolock) AS SRC
    ON SRC.Id = TGT.Id

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.ClienteId = SRC.ClienteId,
        TGT.CausaleId = SRC.CausaleId,
        TGT.PartitaId = SRC.PartitaId,
        TGT.MessageId = SRC.MessageId,
        TGT.Documento = SRC.Documento,
        TGT.DataMovimento = SRC.DataMovimento,
        TGT.Quantita = SRC.Quantita

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        Id,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        ClienteId,
        CausaleId,
        PartitaId,
        MessageId,
        Documento,
        DataMovimento,
        Quantita
      )

    WHEN NOT MATCHED BY SOURCE
        AND TGT.IsDeleted = CAST(0 AS BIT)
      THEN UPDATE
        SET TGT.IsDeleted = CAST(1 AS BIT),
        TGT.UpdateDatetime = CURRENT_TIMESTAMP,
        TGT.ChangeHashKey = CONVERT(VARBINARY(20), ''),
        TGT.ChangeHashKeyASCII = ''

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        CASE WHEN Inserted.IsDeleted = CAST(1 AS BIT) THEN N'DELETE' ELSE $action END AS merge_action,
        'Landing.GPT_OpenAICredito' AS full_olap_table_name,
        'Id = ' + CAST(COALESCE(inserted.Id, deleted.Id) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;
GO

EXEC GPT.usp_Merge_OpenAICredito;
GO

SELECT * FROM Landing.GPT_OpenAICredito;
GO

/**
 * @table Landing.GPT_OpenAIPartita
 * @description 

 * @depends GPT.OpenAIPartita

SELECT TOP 100 * FROM GPT.OpenAIPartita;
*/

IF OBJECT_ID('Landing.GPT_OpenAIPartitaView', 'V') IS NULL EXEC('CREATE VIEW Landing.GPT_OpenAIPartitaView AS SELECT 1 AS fld;');
GO

ALTER VIEW Landing.GPT_OpenAIPartitaView
AS
WITH TableData
AS (
    SELECT
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            Id,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            Codice,
            Descrizione,
            DataCreazione,
            DataScadenza,
            ClienteId,
            Stato,
            Quantita,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        Id,
        Codice,
        Descrizione,
        DataCreazione,
        DataScadenza,
        ClienteId,
        Stato,
        Quantita

    FROM GPT.OpenAIPartita
)
SELECT
    -- Chiavi
    0+TD.Id AS Id,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.Codice,
    TD.Descrizione,
    TD.DataCreazione,
    TD.DataScadenza,
    TD.ClienteId,
    TD.Stato,

    -- Misure
    TD.Quantita

FROM TableData TD;
GO

--DROP TABLE IF EXISTS Landing.GPT_OpenAIPartita;
GO

IF OBJECT_ID(N'Landing.GPT_OpenAIPartita', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Landing.GPT_OpenAIPartita FROM Landing.GPT_OpenAIPartitaView;

    ALTER TABLE Landing.GPT_OpenAIPartita ALTER COLUMN Id INT NOT NULL;

    ALTER TABLE Landing.GPT_OpenAIPartita ADD CONSTRAINT PK_Landing_GPT_OpenAIPartita PRIMARY KEY CLUSTERED (UpdateDatetime, Id);

    CREATE UNIQUE NONCLUSTERED INDEX IX_GPT_OpenAIPartita_BusinessKey ON Landing.GPT_OpenAIPartita (Id);
END;
GO

IF OBJECT_ID('GPT.usp_Merge_OpenAIPartita', 'P') IS NULL EXEC('CREATE PROCEDURE GPT.usp_Merge_OpenAIPartita AS RETURN 0;');
GO

ALTER PROCEDURE GPT.usp_Merge_OpenAIPartita
AS
BEGIN
    SET NOCOUNT ON;

    MERGE INTO Landing.GPT_OpenAIPartita AS TGT
    USING Landing.GPT_OpenAIPartitaView (nolock) AS SRC
    ON SRC.Id = TGT.Id

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.Codice = SRC.Codice,
        TGT.Descrizione = SRC.Descrizione,
        TGT.DataCreazione = SRC.DataCreazione,
        TGT.DataScadenza = SRC.DataScadenza,
        TGT.ClienteId = SRC.ClienteId,
        TGT.Stato = SRC.Stato,
        TGT.Quantita = SRC.Quantita

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        Id,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        Codice,
        Descrizione,
        DataCreazione,
        DataScadenza,
        ClienteId,
        Stato,
        Quantita
      )

    WHEN NOT MATCHED BY SOURCE
        AND TGT.IsDeleted = CAST(0 AS BIT)
      THEN UPDATE
        SET TGT.IsDeleted = CAST(1 AS BIT),
        TGT.UpdateDatetime = CURRENT_TIMESTAMP,
        TGT.ChangeHashKey = CONVERT(VARBINARY(20), ''),
        TGT.ChangeHashKeyASCII = ''

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        CASE WHEN Inserted.IsDeleted = CAST(1 AS BIT) THEN N'DELETE' ELSE $action END AS merge_action,
        'Landing.GPT_OpenAIPartita' AS full_olap_table_name,
        'Id = ' + CAST(COALESCE(inserted.Id, deleted.Id) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;
GO

EXEC GPT.usp_Merge_OpenAIPartita;
GO

SELECT * FROM Landing.GPT_OpenAIPartita;
GO

/**
 * @table Landing.GPT_OpenAIMessage
 * @description 

 * @depends GPT.OpenAIMessage

SELECT TOP 100 * FROM GPT.OpenAIMessage;
*/

IF OBJECT_ID('Landing.GPT_OpenAIMessageView', 'V') IS NULL EXEC('CREATE VIEW Landing.GPT_OpenAIMessageView AS SELECT 1 AS fld;');
GO

ALTER VIEW Landing.GPT_OpenAIMessageView
AS
WITH TableData
AS (
    SELECT

        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            Id,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            OpenAIThreadId,
            Message,
            IsQuestion,
            CreatedOn,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        Id,
        OpenAIThreadId,
        Message,
        IsQuestion,
        CreatedOn

    FROM GPT.OpenAIMessage
    WHERE CreatedOn >= CAST('20250901' AS SMALLDATETIME)
)
SELECT
    -- Chiavi
    0+TD.Id AS Id,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.OpenAIThreadId,
    TD.Message,
    TD.IsQuestion,
    TD.CreatedOn

FROM TableData TD;
GO

--DROP TABLE IF EXISTS Landing.GPT_OpenAIMessage;
GO

IF OBJECT_ID(N'Landing.GPT_OpenAIMessage', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Landing.GPT_OpenAIMessage FROM Landing.GPT_OpenAIMessageView;

    ALTER TABLE Landing.GPT_OpenAIMessage ALTER COLUMN Id INT NOT NULL;

    ALTER TABLE Landing.GPT_OpenAIMessage ADD CONSTRAINT PK_Landing_GPT_OpenAIMessage PRIMARY KEY CLUSTERED (UpdateDatetime, Id);

    CREATE UNIQUE NONCLUSTERED INDEX IX_GPT_OpenAIMessage_BusinessKey ON Landing.GPT_OpenAIMessage (Id);
END;
GO

IF OBJECT_ID('GPT.usp_Merge_OpenAIMessage', 'P') IS NULL EXEC('CREATE PROCEDURE GPT.usp_Merge_OpenAIMessage AS RETURN 0;');
GO

ALTER PROCEDURE GPT.usp_Merge_OpenAIMessage
AS
BEGIN
    SET NOCOUNT ON;

    MERGE INTO Landing.GPT_OpenAIMessage AS TGT
    USING Landing.GPT_OpenAIMessageView (nolock) AS SRC
    ON SRC.Id = TGT.Id

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.OpenAIThreadId = SRC.OpenAIThreadId,
        TGT.Message = SRC.Message,
        TGT.IsQuestion = SRC.IsQuestion,
        TGT.CreatedOn = SRC.CreatedOn

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        Id,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        OpenAIThreadId,
        Message,
        IsQuestion,
        CreatedOn
      )

    WHEN NOT MATCHED BY SOURCE
        AND TGT.IsDeleted = CAST(0 AS BIT)
      THEN UPDATE
        SET TGT.IsDeleted = CAST(1 AS BIT),
        TGT.UpdateDatetime = CURRENT_TIMESTAMP,
        TGT.ChangeHashKey = CONVERT(VARBINARY(20), ''),
        TGT.ChangeHashKeyASCII = ''

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        CASE WHEN Inserted.IsDeleted = CAST(1 AS BIT) THEN N'DELETE' ELSE $action END AS merge_action,
        'Landing.GPT_OpenAIMessage' AS full_olap_table_name,
        'Id = ' + CAST(COALESCE(inserted.Id, deleted.Id) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;
GO

EXEC GPT.usp_Merge_OpenAIMessage;
GO

SELECT * FROM Landing.GPT_OpenAIMessage;
GO

/**
 * @table Landing.GPT_OpenAIThread
 * @description 

 * @depends GPT.OpenAIThread

SELECT TOP 100 * FROM GPT.OpenAIThread;
*/

IF OBJECT_ID('Landing.GPT_OpenAIThreadView', 'V') IS NULL EXEC('CREATE VIEW Landing.GPT_OpenAIThreadView AS SELECT 1 AS fld;');
GO

ALTER VIEW Landing.GPT_OpenAIThreadView
AS
WITH TableData
AS (
    SELECT

        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            Id,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            ClienteId,
            Area,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        Id,
        ClienteId,
        Area

    FROM GPT.OpenAIThread
)
SELECT
    -- Chiavi
    0+TD.Id AS Id,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.ClienteId,
    TD.Area

FROM TableData TD;
GO

--DROP TABLE IF EXISTS Landing.GPT_OpenAIThread;
GO

IF OBJECT_ID(N'Landing.GPT_OpenAIThread', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Landing.GPT_OpenAIThread FROM Landing.GPT_OpenAIThreadView;

    ALTER TABLE Landing.GPT_OpenAIThread ALTER COLUMN Id INT NOT NULL;

    ALTER TABLE Landing.GPT_OpenAIThread ADD CONSTRAINT PK_Landing_GPT_OpenAIThread PRIMARY KEY CLUSTERED (UpdateDatetime, Id);

    CREATE UNIQUE NONCLUSTERED INDEX IX_GPT_OpenAIThread_BusinessKey ON Landing.GPT_OpenAIThread (Id);
END;
GO

IF OBJECT_ID('GPT.usp_Merge_OpenAIThread', 'P') IS NULL EXEC('CREATE PROCEDURE GPT.usp_Merge_OpenAIThread AS RETURN 0;');
GO

ALTER PROCEDURE GPT.usp_Merge_OpenAIThread
AS
BEGIN
    SET NOCOUNT ON;

    MERGE INTO Landing.GPT_OpenAIThread AS TGT
    USING Landing.GPT_OpenAIThreadView (nolock) AS SRC
    ON SRC.Id = TGT.Id

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.ClienteId = SRC.ClienteId,
        TGT.Area = SRC.Area

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        Id,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        ClienteId,
        Area
      )

    WHEN NOT MATCHED BY SOURCE
        AND TGT.IsDeleted = CAST(0 AS BIT)
      THEN UPDATE
        SET TGT.IsDeleted = CAST(1 AS BIT),
        TGT.UpdateDatetime = CURRENT_TIMESTAMP,
        TGT.ChangeHashKey = CONVERT(VARBINARY(20), ''),
        TGT.ChangeHashKeyASCII = ''

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        CASE WHEN Inserted.IsDeleted = CAST(1 AS BIT) THEN N'DELETE' ELSE $action END AS merge_action,
        'Landing.GPT_OpenAIThread' AS full_olap_table_name,
        'Id = ' + CAST(COALESCE(inserted.Id, deleted.Id) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;
GO

EXEC GPT.usp_Merge_OpenAIThread;
GO

SELECT * FROM Landing.GPT_OpenAIThread;
GO
