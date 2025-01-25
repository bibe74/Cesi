USE CesiDW;
GO

/*
SET NOEXEC OFF;
--*/ SET NOEXEC ON;
GO

/*
    SCHEMA_NAME > MYSOLUTION
    TABLE_NAME > CoursesData
    STAGING_TABLE_NAME > Corsi
*/

/**
 * @table Staging.Corsi
 * @description

 * @depends Landing.MYSOLUTION_CoursesData

SELECT TOP 1 * FROM Landing.MYSOLUTION_CoursesData;
*/

--DROP TABLE IF EXISTS Staging.Corsi; DELETE FROM audit.tables WHERE provider_name = N'MyDatamartReporting' AND full_table_name = N'Landing.MYSOLUTION_CoursesData';
GO

IF NOT EXISTS (SELECT 1 FROM audit.tables WHERE provider_name = N'MyDatamartReporting' AND full_table_name = N'Landing.MYSOLUTION_CoursesData')
BEGIN

    INSERT INTO audit.tables (
        provider_name,
        full_table_name,
        staging_table_name,
        datawarehouse_table_name,
        lastupdated_staging,
        lastupdated_local
    )
    VALUES
    (   N'MyDatamartReporting',       -- provider_name - nvarchar(60)
        N'Landing.MYSOLUTION_CoursesData',      -- full_table_name - sysname
        N'Staging.Corsi',      -- staging_table_name - sysname
        N'Dim.Corsi',      -- datawarehouse_table_name - sysname
        NULL, -- lastupdated_staging - datetime
        NULL  -- lastupdated_local - datetime
    );

END;
GO

IF OBJECT_ID(N'Staging.CorsiView', N'V') IS NULL EXEC('CREATE VIEW Staging.CorsiView AS SELECT 1 AS fld;');
GO

ALTER VIEW Staging.CorsiView
AS
WITH TableData
AS (
    SELECT

        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            T.OrderItemId,
            T.Partecipant_Id,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            T.PartecipantFirstName,
            T.PartecipantLastName,
            T.PartecipantEmail,
            T.PartecipantFiscalCode,
            T.RootPartecipantEmail,
            T.CustomerUserName,
            U.IDUtente,
            T.CustomerUserName,
            T.CourseName,
            T.CourseCode,
            T.AttCourseCode,
            T.WebinarCode,
            T.AttWebinarCode,
            T.CourseType,
            T.StartDate_text,
            T.StartDate,
            D.PKData,
            T.HasMoreDates,
            T.OrderDescription,
            T.ItemNetUnitPrice,
            T.OrderTotalPrice,
            T.OrderNumber,
            T.OrderStatus,
            DI.PKData,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        T.OrderItemId,
        T.Partecipant_Id,
        T.PartecipantFirstName AS NomePartecipante,
        T.PartecipantLastName AS CognomePartecipante,
        T.PartecipantEmail AS EmailPartecipante,
        T.PartecipantFiscalCode AS CodiceFiscalePartecipante,
        T.RootPartecipantEmail AS EmailPartecipanteRoot,
        T.CustomerUserName AS Utente,
        COALESCE(U.PKUtente, CASE WHEN COALESCE(T.CustomerUserName, N'') = N'' THEN -1 ELSE -101 END) AS PKUtente,
        T.CourseName AS Corso,
        --T.CourseCode AS IDCorso,
        --T.AttCourseCode,
        COALESCE(T.CourseCode, T.AttCourseCode) AS IDCorso,
        --T.WebinarCode AS IDWebinar,
        --T.AttWebinarCode,
        COALESCE(T.WebinarCode, T.AttWebinarCode) AS IDWebinar,
        T.CourseType AS TipoCorso,
        T.StartDate_text,
        T.StartDate,
        COALESCE(D.PKData, CAST('19000101' AS DATE)) AS PKDataInizio,
        T.HasMoreDates AS HasDateMultiple,
        T.OrderDescription AS DescrizioneOrdine,
        T.ItemNetUnitPrice AS PrezzoUnitarioOrdine,
        T.OrderTotalPrice AS ImportoTotaleOrdine,
        T.OrderNumber AS NumeroOrdine,
        T.OrderStatus AS StatoOrdine,
        T.OrderCreatedDate,
        COALESCE(DI.PKData, CAST('19000101' AS DATE)) AS PKDataIscrizione

    FROM Landing.MYSOLUTION_CoursesData T
    LEFT JOIN Dim.Utente U ON U.Email = T.CustomerUserName
    LEFT JOIN Dim.Data D ON D.PKData = T.StartDate
    LEFT JOIN Dim.Data DI ON DI.PKData = T.OrderCreatedDate
)
SELECT
    -- Chiavi
    TD.OrderItemId,
    TD.Partecipant_Id,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Altri campi
    TD.NomePartecipante,
    TD.CognomePartecipante,
    TD.EmailPartecipante,
    TD.CodiceFiscalePartecipante,
    TD.EmailPartecipanteRoot,
    TD.Utente,
    TD.PKUtente,
    TD.Corso,
    TD.IDCorso,
    TD.IDWebinar,
    TD.TipoCorso,
    --TD.StartDate_text,
    --TD.StartDate,
    TD.PKDataInizio,
    TD.HasDateMultiple,
    TD.DescrizioneOrdine,
    TD.PrezzoUnitarioOrdine,
    TD.ImportoTotaleOrdine,
    TD.NumeroOrdine,
    TD.StatoOrdine,
    TD.PKDataIscrizione

FROM TableData TD;
GO

--IF OBJECT_ID(N'Staging.Corsi', N'U') IS NOT NULL DROP TABLE Staging.Corsi;
GO

IF OBJECT_ID(N'Staging.Corsi', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Staging.Corsi FROM Staging.CorsiView;

    ALTER TABLE Staging.Corsi ADD CONSTRAINT PK_Landing_MYSOLUTION_CoursesData PRIMARY KEY CLUSTERED (UpdateDatetime, OrderItemId, Partecipant_Id);

    --ALTER TABLE Staging.Corsi ALTER COLUMN  NVARCHAR(60) NOT NULL;

    --CREATE UNIQUE NONCLUSTERED INDEX IX_MYSOLUTION_CoursesData_BusinessKey ON Staging.Corsi ();
END;
GO

IF OBJECT_ID(N'Staging.usp_Reload_Corsi', N'P') IS NULL EXEC('CREATE PROCEDURE Staging.usp_Reload_Corsi AS RETURN 0;');
GO

ALTER PROCEDURE Staging.usp_Reload_Corsi
AS
BEGIN

    SET NOCOUNT ON;

    DECLARE @lastupdated_staging DATETIME;
    DECLARE @provider_name NVARCHAR(60) = N'MyDatamartReporting';
    DECLARE @full_table_name sysname = N'Landing.MYSOLUTION_CoursesData';

    SELECT TOP 1 @lastupdated_staging = lastupdated_staging
    FROM audit.tables
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    IF (@lastupdated_staging IS NULL) SET @lastupdated_staging = CAST('19000101' AS DATETIME);

    BEGIN TRANSACTION

    TRUNCATE TABLE Staging.Corsi;

    INSERT INTO Staging.Corsi
    SELECT * FROM Staging.CorsiView
    WHERE UpdateDatetime > @lastupdated_staging;

    SELECT @lastupdated_staging = MAX(UpdateDatetime) FROM Staging.Corsi;

    IF (@lastupdated_staging IS NOT NULL)
    BEGIN

    UPDATE audit.tables
    SET lastupdated_staging = @lastupdated_staging
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    END;

    COMMIT

END;
GO

EXEC Staging.usp_Reload_Corsi;
GO

--DROP TABLE IF EXISTS Fact.Corsi; DROP SEQUENCE IF EXISTS dbo.seq_Fact_Corsi;
GO

IF OBJECT_ID('dbo.seq_Fact_Corsi', 'SO') IS NULL
BEGIN

    CREATE SEQUENCE dbo.seq_Fact_Corsi START WITH 1;

END;
GO

IF OBJECT_ID('Fact.Corsi', 'U') IS NULL
BEGIN

    CREATE TABLE Fact.Corsi (
        PKCorsi INT NOT NULL CONSTRAINT PK_Fact_Corsi PRIMARY KEY CLUSTERED CONSTRAINT DFT_Fact_Corsi_PKCorsi DEFAULT (NEXT VALUE FOR dbo.seq_Fact_Corsi),

	    OrderItemId INT NOT NULL,
	    Partecipant_Id INT NOT NULL,
        PKUtente INT NOT NULL CONSTRAINT FK_Fact_Corsi_PKUtente FOREIGN KEY REFERENCES Dim.Utente (PKUtente),
        PKDataInizio DATE NOT NULL CONSTRAINT FK_Fact_Corsi_PKDataInizio FOREIGN KEY REFERENCES Dim.Data (PKData),
        PKDataIscrizione DATE NOT NULL CONSTRAINT FK_Fact_Corsi_PKDataIscrizione FOREIGN KEY REFERENCES Dim.Data (PKData),

	    HistoricalHashKey VARBINARY(20) NULL,
	    ChangeHashKey VARBINARY(20) NULL,
	    HistoricalHashKeyASCII VARCHAR(34) NULL,
	    ChangeHashKeyASCII VARCHAR(34) NULL,
	    InsertDatetime DATETIME NOT NULL,
	    UpdateDatetime DATETIME NOT NULL,
	    IsDeleted BIT NOT NULL,

	    NomePartecipante NVARCHAR(120) NULL,
	    CognomePartecipante NVARCHAR(120) NULL,
	    EmailPartecipante NVARCHAR(120) NULL,
	    CodiceFiscalePartecipante NVARCHAR(20) NULL,
	    EmailPartecipanteRoot NVARCHAR(120) NULL,
	    Utente NVARCHAR(120) NULL,
	    Corso NVARCHAR(240) NOT NULL,
	    IDCorso NVARCHAR(20) NULL,
	    IDWebinar NVARCHAR(120) NULL,
	    TipoCorso NVARCHAR(120) NULL,
	    HasDateMultiple BIT NULL,
	    DescrizioneOrdine NVARCHAR(240) NULL,
	    PrezzoUnitarioOrdine DECIMAL(10, 2) NOT NULL,
	    NumeroOrdine NVARCHAR(120) NOT NULL,
	    StatoOrdine INT NOT NULL,

	    ImportoTotaleOrdine DECIMAL(10, 2) NULL
    );

    CREATE UNIQUE NONCLUSTERED INDEX IX_Fact_Corsi_OrderItemId_Partecipant_Id ON Fact.Corsi (OrderItemId, Partecipant_Id);

    ALTER SEQUENCE dbo.seq_Fact_Corsi RESTART WITH 1;

END;
GO

IF OBJECT_ID(N'Fact.usp_Merge_Corsi', N'P') IS NULL EXEC('CREATE PROCEDURE Fact.usp_Merge_Corsi AS RETURN 0;');
GO

ALTER PROCEDURE Fact.usp_Merge_Corsi
AS
BEGIN

    SET NOCOUNT ON;

    BEGIN TRANSACTION 

    DECLARE @provider_name NVARCHAR(60) = N'MyDatamartReporting';
    DECLARE @full_table_name sysname = N'Import.Corsi';

    MERGE INTO Fact.Corsi AS TGT
    USING Staging.Corsi (nolock) AS SRC
    ON SRC.OrderItemId = TGT.OrderItemId AND SRC.Partecipant_Id = TGT.Partecipant_Id

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.IsDeleted = SRC.IsDeleted,
        TGT.PKUtente = SRC.PKUtente,
        TGT.PKDataInizio = SRC.PKDataInizio,
        TGT.PKDataIscrizione = SRC.PKDataIscrizione,
        TGT.NomePartecipante = SRC.NomePartecipante,
        TGT.CognomePartecipante = SRC.CognomePartecipante,
        TGT.EmailPartecipante = SRC.EmailPartecipante,
        TGT.CodiceFiscalePartecipante = SRC.CodiceFiscalePartecipante,
        TGT.EmailPartecipanteRoot = SRC.EmailPartecipanteRoot,
        TGT.Utente = SRC.Utente,
        TGT.Corso = SRC.Corso,
        TGT.IDCorso = SRC.IDCorso,
        TGT.IDWebinar = SRC.IDWebinar,
        TGT.TipoCorso = SRC.TipoCorso,
        TGT.HasDateMultiple = SRC.HasDateMultiple,
        TGT.DescrizioneOrdine = SRC.DescrizioneOrdine,
        TGT.PrezzoUnitarioOrdine = SRC.PrezzoUnitarioOrdine,
        TGT.NumeroOrdine = SRC.NumeroOrdine,
        TGT.StatoOrdine = SRC.StatoOrdine,
        TGT.ImportoTotaleOrdine = SRC.ImportoTotaleOrdine

    WHEN NOT MATCHED
      THEN INSERT (
        --PKCorsi,
        OrderItemId,
        Partecipant_Id,
        PKUtente,
        PKDataInizio,
        PKDataIscrizione,
        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
        NomePartecipante,
        CognomePartecipante,
        EmailPartecipante,
        CodiceFiscalePartecipante,
        EmailPartecipanteRoot,
        Utente,
        Corso,
        IDCorso,
        IDWebinar,
        TipoCorso,
        HasDateMultiple,
        DescrizioneOrdine,
        PrezzoUnitarioOrdine,
        NumeroOrdine,
        StatoOrdine,
        ImportoTotaleOrdine
    ) VALUES (
        OrderItemId,
        Partecipant_Id,
        PKUtente,
        PKDataInizio,
        PKDataIscrizione,
        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
        NomePartecipante,
        CognomePartecipante,
        EmailPartecipante,
        CodiceFiscalePartecipante,
        EmailPartecipanteRoot,
        Utente,
        Corso,
        IDCorso,
        IDWebinar,
        TipoCorso,
        HasDateMultiple,
        DescrizioneOrdine,
        PrezzoUnitarioOrdine,
        NumeroOrdine,
        StatoOrdine,
        ImportoTotaleOrdine
    )

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        $action AS merge_action,
        'Staging.Corsi' AS full_olap_table_name,
        'OrderItemId = ' + CAST(COALESCE(inserted.OrderItemId, deleted.OrderItemId) AS NVARCHAR(1000)) + ', Partecipant_Id = ' + CAST(COALESCE(inserted.Partecipant_Id, deleted.Partecipant_Id) AS NVARCHAR(1000)) AS primary_key_description
    INTO audit.merge_log_details;

    DELETE FROM Fact.Corsi
    WHERE IsDeleted = CAST(1 AS BIT);

    UPDATE audit.tables
    SET lastupdated_local = lastupdated_staging
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    COMMIT TRANSACTION;

END;
GO

EXEC Fact.usp_Merge_Corsi;
GO
