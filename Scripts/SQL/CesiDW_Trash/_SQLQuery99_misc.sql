----/*
----    SCHEMA_NAME > MYSOLUTION
----    TABLE_NAME > InfoAccounts
----*/

----/**
---- * @table Landing.MYSOLUTION_InfoAccounts
---- * @description 

---- * @depends MYSOLUTION.InfoAccounts

----SELECT TOP 100 * FROM MYSOLUTION.InfoAccounts;
----*/

----IF OBJECT_ID('Landing.MYSOLUTION_InfoAccountsView', 'V') IS NULL EXEC('CREATE VIEW Landing.MYSOLUTION_InfoAccountsView AS SELECT 1 AS fld;');
----GO

----ALTER VIEW Landing.MYSOLUTION_InfoAccountsView
----AS
----WITH TableData
----AS (
----    SELECT
----        guid_account,
----        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
----            guid_account,
----            ' '
----        ))) AS HistoricalHashKey,
----        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
----            legacy_code,
----            ragione_sociale,
----            codice_fiscale,
----            partita_iva,
----            citta,
----            provincia,
----            email,
----            telefono,
----            tipo,
----            guid_owner,
----            owner_name,
----            inizio,
----            scadenza,
----            data_disdetta,
----            stato_disdetta,
----            rif_disdetta,
----            ' '
----        ))) AS ChangeHashKey,
----        CURRENT_TIMESTAMP AS InsertDatetime,
----        CURRENT_TIMESTAMP AS UpdateDatetime,
----        legacy_code,
----        ragione_sociale,
----        codice_fiscale,
----        partita_iva,
----        citta,
----        provincia,
----        email,
----        telefono,
----        tipo,
----        guid_owner,
----        owner_name,
----        inizio,
----        scadenza,
----        data_disdetta,
----        stato_disdetta,
----        rif_disdetta,

----        ROW_NUMBER() OVER (PARTITION BY guid_account ORDER BY inizio DESC) AS rn

----    FROM MYSOLUTION.InfoAccounts
----    WHERE RTRIM(LTRIM(COALESCE(email, N''))) <> N''
----)
----SELECT
----    -- Chiavi
----    TD.guid_account COLLATE DATABASE_DEFAULT AS guid_account, -- GUIDCliente

----    -- Campi per sincronizzazione
----    TD.HistoricalHashKey,
----    TD.ChangeHashKey,
----    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
----    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
----    TD.InsertDatetime,
----    TD.UpdateDatetime,
----    CAST(0 AS BIT) AS IsDeleted,

----    -- Altri campi
----    TD.legacy_code COLLATE DATABASE_DEFAULT AS legacy_code, -- CodiceCliente
----    TD.ragione_sociale COLLATE DATABASE_DEFAULT AS ragione_sociale, -- RagioneSociale
----    TD.codice_fiscale COLLATE DATABASE_DEFAULT AS codice_fiscale,
----    TD.partita_iva COLLATE DATABASE_DEFAULT AS partita_iva,
----    TD.citta COLLATE DATABASE_DEFAULT AS citta, -- Localita
----    TD.provincia COLLATE DATABASE_DEFAULT AS provincia, -- Provincia
----    TD.email COLLATE DATABASE_DEFAULT AS email, -- Email
----    TD.telefono COLLATE DATABASE_DEFAULT AS telefono, -- Telefono
----    TD.tipo COLLATE DATABASE_DEFAULT AS tipo, -- TipoCliente
----    TD.guid_owner COLLATE DATABASE_DEFAULT AS guid_owner, -- GUIDAgente
----    TD.owner_name COLLATE DATABASE_DEFAULT AS owner_name, -- Agente
----    TD.inizio, -- DataInizioContratto
----    TD.scadenza, -- DataFineContratto
----    TD.data_disdetta, -- DataDisdetta
----    TD.stato_disdetta COLLATE DATABASE_DEFAULT AS stato_disdetta, -- StatoDisdetta
----    TD.rif_disdetta COLLATE DATABASE_DEFAULT AS rif_disdetta -- RiferimentoDisdetta

----FROM TableData TD
----WHERE TD.rn = 1;
----GO

------DROP TABLE IF EXISTS Landing.MYSOLUTION_InfoAccounts;
----GO

----IF OBJECT_ID(N'Landing.MYSOLUTION_InfoAccounts', N'U') IS NULL
----BEGIN
----    SELECT TOP 0 * INTO Landing.MYSOLUTION_InfoAccounts FROM Landing.MYSOLUTION_InfoAccountsView;

----    --ALTER TABLE Landing.MYSOLUTION_InfoAccounts ALTER COLUMN guid_account UNIQUEIDENTIFIER NOT NULL;

----    ALTER TABLE Landing.MYSOLUTION_InfoAccounts ADD CONSTRAINT PK_Landing_MYSOLUTION_InfoAccounts PRIMARY KEY CLUSTERED (UpdateDatetime, guid_account);

----    CREATE UNIQUE NONCLUSTERED INDEX IX_MYSOLUTION_InfoAccounts_BusinessKey ON Landing.MYSOLUTION_InfoAccounts (guid_account);
----END;
----GO

----IF OBJECT_ID('MYSOLUTION.usp_Merge_InfoAccounts', 'P') IS NULL EXEC('CREATE PROCEDURE MYSOLUTION.usp_Merge_InfoAccounts AS RETURN 0;');
----GO

----ALTER PROCEDURE MYSOLUTION.usp_Merge_InfoAccounts
----AS
----BEGIN
----    SET NOCOUNT ON;

----    MERGE INTO Landing.MYSOLUTION_InfoAccounts AS TGT
----    USING Landing.MYSOLUTION_InfoAccountsView (nolock) AS SRC
----    ON SRC.guid_account = TGT.guid_account

----    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
----      THEN UPDATE SET
----        TGT.ChangeHashKey = SRC.ChangeHashKey,
----        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
----        --TGT.InsertDatetime = SRC.InsertDatetime,
----        TGT.UpdateDatetime = SRC.UpdateDatetime,
----        TGT.legacy_code = SRC.legacy_code,
----        TGT.ragione_sociale = SRC.ragione_sociale,
----        TGT.codice_fiscale = SRC.codice_fiscale,
----        TGT.partita_iva = SRC.partita_iva,
----        TGT.citta = SRC.citta,
----        TGT.provincia = SRC.provincia,
----        TGT.email = SRC.email,
----        TGT.telefono = SRC.telefono,
----        TGT.tipo = SRC.tipo,
----        TGT.guid_owner = SRC.guid_owner,
----        TGT.owner_name = SRC.owner_name,
----        TGT.inizio = SRC.inizio,
----        TGT.scadenza = SRC.scadenza,
----        TGT.data_disdetta = SRC.data_disdetta,
----        TGT.stato_disdetta = SRC.stato_disdetta,
----        TGT.rif_disdetta = SRC.rif_disdetta

----    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
----      THEN INSERT VALUES (
----        guid_account,

----        HistoricalHashKey,
----        ChangeHashKey,
----        HistoricalHashKeyASCII,
----        ChangeHashKeyASCII,
----        InsertDatetime,
----        UpdateDatetime,
----        IsDeleted,
    
----        legacy_code,
----        ragione_sociale,
----        codice_fiscale,
----        partita_iva,
----        citta,
----        provincia,
----        email,
----        telefono,
----        tipo,
----        guid_owner,
----        owner_name,
----        inizio,
----        scadenza,
----        data_disdetta,
----        stato_disdetta,
----        rif_disdetta
----      )

----    WHEN NOT MATCHED BY SOURCE
----        AND TGT.IsDeleted = CAST(0 AS BIT)
----      THEN UPDATE
----        SET TGT.IsDeleted = CAST(1 AS BIT),
----        TGT.UpdateDatetime = CURRENT_TIMESTAMP,
----        TGT.ChangeHashKey = CONVERT(VARBINARY(20), ''),
----        TGT.ChangeHashKeyASCII = ''

----    OUTPUT
----        CURRENT_TIMESTAMP AS merge_datetime,
----        CASE WHEN Inserted.IsDeleted = CAST(1 AS BIT) THEN N'DELETE' ELSE $action END AS merge_action,
----        'Landing.MYSOLUTION_InfoAccounts' AS full_olap_table_name,
----        'guid_account = ' + CAST(COALESCE(inserted.guid_account, deleted.guid_account) AS NVARCHAR) AS primary_key_description
----    INTO audit.merge_log_details;

----END;
----GO

----EXEC MYSOLUTION.usp_Merge_InfoAccounts;
----GO

----/*
----    SCHEMA_NAME > MYSOLUTION
----    TABLE_NAME > LogsForReport
----*/

----/**
---- * @table Landing.MYSOLUTION_LogsForReport
---- * @description 

---- * @depends MYSOLUTION.LogsForReport

----SELECT TOP 100 * FROM MYSOLUTION.LogsForReport;
----*/

----IF OBJECT_ID('Landing.MYSOLUTION_LogsForReportView', 'V') IS NULL EXEC('CREATE VIEW Landing.MYSOLUTION_LogsForReportView AS SELECT 1 AS fld;');
----GO

----ALTER VIEW Landing.MYSOLUTION_LogsForReportView
----AS
----WITH AggregatedData
----AS (
----    SELECT
----        CAST(DataOra AS DATE) AS Data,
----        IDUser,
----        COUNT(1) AS NumeroPagineVisitate

----    FROM MYSOLUTION.LogsForReport
----    WHERE IDUser IS NOT NULL
----    GROUP BY CAST(DataOra AS DATE),
----        IDUser
----),
----TableData
----AS (
----    SELECT
----        AD.Data,
----        AD.IDUser,
----        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
----            AD.Data,
----            AD.IDUser,
----            ' '
----        ))) AS HistoricalHashKey,
----        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
----            AD.NumeroPagineVisitate,
----            ' '
----        ))) AS ChangeHashKey,
----        CURRENT_TIMESTAMP AS InsertDatetime,
----        CURRENT_TIMESTAMP AS UpdateDatetime,

----        AD.NumeroPagineVisitate

----    FROM AggregatedData AD
----    WHERE AD.Data > CAST('19000101' AS DATE)
----)
----SELECT
----    -- Chiavi
----    TD.Data, -- PKData
----    TD.IDUser, -- GUIDUser

----    -- Campi per sincronizzazione
----    TD.HistoricalHashKey,
----    TD.ChangeHashKey,
----    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
----    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
----    TD.InsertDatetime,
----    TD.UpdateDatetime,
----    CAST(0 AS BIT) AS IsDeleted,

----    -- Altri campi
----    TD.NumeroPagineVisitate

----FROM TableData TD;
----GO

------DROP TABLE IF EXISTS Landing.MYSOLUTION_LogsForReport;
----GO

----IF OBJECT_ID(N'Landing.MYSOLUTION_LogsForReport', N'U') IS NULL
----BEGIN
----    SELECT TOP 0 * INTO Landing.MYSOLUTION_LogsForReport FROM Landing.MYSOLUTION_LogsForReportView;

----    ALTER TABLE Landing.MYSOLUTION_LogsForReport ALTER COLUMN Data DATE NOT NULL;
----    ALTER TABLE Landing.MYSOLUTION_LogsForReport ALTER COLUMN IDUser UNIQUEIDENTIFIER NOT NULL;

----    ALTER TABLE Landing.MYSOLUTION_LogsForReport ADD CONSTRAINT PK_Landing_MYSOLUTION_LogsForReport PRIMARY KEY CLUSTERED (UpdateDatetime, Data, IDUser);

----    CREATE UNIQUE NONCLUSTERED INDEX IX_MYSOLUTION_LogsForReport_BusinessKey ON Landing.MYSOLUTION_LogsForReport (Data, IDUser);
----END;
----GO

----IF OBJECT_ID('MYSOLUTION.usp_Merge_LogsForReport', 'P') IS NULL EXEC('CREATE PROCEDURE MYSOLUTION.usp_Merge_LogsForReport AS RETURN 0;');
----GO

----ALTER PROCEDURE MYSOLUTION.usp_Merge_LogsForReport
----AS
----BEGIN
----    SET NOCOUNT ON;

----    MERGE INTO Landing.MYSOLUTION_LogsForReport AS TGT
----    USING Landing.MYSOLUTION_LogsForReportView (nolock) AS SRC
----    ON SRC.Data = TGT.Data AND SRC.IDUser = TGT.IDUser

----    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
----      THEN UPDATE SET
----        TGT.ChangeHashKey = SRC.ChangeHashKey,
----        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
----        --TGT.InsertDatetime = SRC.InsertDatetime,
----        TGT.UpdateDatetime = SRC.UpdateDatetime,
----        TGT.NumeroPagineVisitate = SRC.NumeroPagineVisitate

----    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
----      THEN INSERT VALUES (
----        Data,
----        IDUser,

----        HistoricalHashKey,
----        ChangeHashKey,
----        HistoricalHashKeyASCII,
----        ChangeHashKeyASCII,
----        InsertDatetime,
----        UpdateDatetime,
----        IsDeleted,
    
----        NumeroPagineVisitate
----      )

----    WHEN NOT MATCHED BY SOURCE
----        AND TGT.IsDeleted = CAST(0 AS BIT)
----      THEN UPDATE
----        SET TGT.IsDeleted = CAST(1 AS BIT),
----        TGT.UpdateDatetime = CURRENT_TIMESTAMP,
----        TGT.ChangeHashKey = CONVERT(VARBINARY(20), ''),
----        TGT.ChangeHashKeyASCII = ''

----    OUTPUT
----        CURRENT_TIMESTAMP AS merge_datetime,
----        CASE WHEN Inserted.IsDeleted = CAST(1 AS BIT) THEN N'DELETE' ELSE $action END AS merge_action,
----        'Landing.MYSOLUTION_LogsForReport' AS full_olap_table_name,
----        'Data/IDUser = ' + CAST(COALESCE(inserted.Data, deleted.Data) AS NVARCHAR) + N'/'+ CAST(COALESCE(inserted.IDUser, deleted.IDUser) AS NVARCHAR(50)) AS primary_key_description
----    INTO audit.merge_log_details;

----END;
----GO

----EXEC MYSOLUTION.usp_Merge_LogsForReport;
----GO

----/*
----    SCHEMA_NAME > MYSOLUTION
----    TABLE_NAME > LogsEpiServer
----*/

----/**
---- * @table Landing.MYSOLUTION_LogsEpiServer
---- * @description 

---- * @depends MYSOLUTION.LogsEpiServer

----SELECT TOP 100 * FROM MYSOLUTION.LogsEpiServer;
----*/

----IF OBJECT_ID('Landing.MYSOLUTION_LogsEpiServerView', 'V') IS NULL EXEC('CREATE VIEW Landing.MYSOLUTION_LogsEpiServerView AS SELECT 1 AS fld;');
----GO

----ALTER VIEW Landing.MYSOLUTION_LogsEpiServerView
----AS
----WITH TableData
----AS (
----    SELECT
----        IDLog,
----        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
----            IDLog,
----            ' '
----        ))) AS HistoricalHashKey,
----        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
----            DataOra,
----            Username,
----            ' '
----        ))) AS ChangeHashKey,
----        CURRENT_TIMESTAMP AS InsertDatetime,
----        CURRENT_TIMESTAMP AS UpdateDatetime,
----        DataOra,
----        Username

----    FROM MYSOLUTION.LogsEpiServer
----    WHERE PageType = N'Login'
----        AND COALESCE(Username, N'') <> N''
----)
----SELECT
----    -- Chiavi
----    TD.IDLog,

----    -- Campi per sincronizzazione
----    TD.HistoricalHashKey,
----    TD.ChangeHashKey,
----    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
----    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
----    TD.InsertDatetime,
----    TD.UpdateDatetime,
----    CAST(0 AS BIT) AS IsDeleted,

----    -- Attributi
----    DataOra,
----    Username

----FROM TableData TD;
----GO

------DROP TABLE IF EXISTS Landing.MYSOLUTION_LogsEpiServer;
----GO

----IF OBJECT_ID(N'Landing.MYSOLUTION_LogsEpiServer', N'U') IS NULL
----BEGIN
----    SELECT TOP 0 * INTO Landing.MYSOLUTION_LogsEpiServer FROM Landing.MYSOLUTION_LogsEpiServerView;

----    ALTER TABLE Landing.MYSOLUTION_LogsEpiServer ADD CONSTRAINT PK_Landing_MYSOLUTION_LogsEpiServer PRIMARY KEY CLUSTERED (UpdateDatetime, IDLog);

----    --ALTER TABLE Landing.MYSOLUTION_LogsEpiServer ALTER COLUMN  NVARCHAR(60) NOT NULL;

----    CREATE UNIQUE NONCLUSTERED INDEX IX_MYSOLUTION_LogsEpiServer_BusinessKey ON Landing.MYSOLUTION_LogsEpiServer (IDLog);
----END;
----GO

----IF OBJECT_ID('MYSOLUTION.usp_Merge_LogsEpiServer', 'P') IS NULL EXEC('CREATE DRPROCEDURE MYSOLUTION.usp_Merge_LogsEpiServer AS RETURN 0;');
----GO

----ALTER PROCEDURE MYSOLUTION.usp_Merge_LogsEpiServer
----AS
----BEGIN
----    SET NOCOUNT ON;

----    MERGE INTO Landing.MYSOLUTION_LogsEpiServer AS TGT
----    USING Landing.MYSOLUTION_LogsEpiServerView (nolock) AS SRC
----    ON SRC.IDLog = TGT.IDLog

----    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
----      THEN UPDATE SET
----        TGT.ChangeHashKey = SRC.ChangeHashKey,
----        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
----        --TGT.InsertDatetime = SRC.InsertDatetime,
----        TGT.UpdateDatetime = SRC.UpdateDatetime,
----        TGT.DataOra = SRC.DataOra,
----        TGT.Username = SRC.Username

----    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
----      THEN INSERT VALUES (
----        IDLog,

----        HistoricalHashKey,
----        ChangeHashKey,
----        HistoricalHashKeyASCII,
----        ChangeHashKeyASCII,
----        InsertDatetime,
----        UpdateDatetime,
----        IsDeleted,
    
----        DataOra,
----        Username
----      )

----    WHEN NOT MATCHED BY SOURCE
----        AND TGT.IsDeleted = CAST(0 AS BIT)
----      THEN UPDATE
----        SET TGT.IsDeleted = CAST(1 AS BIT),
----        TGT.UpdateDatetime = CURRENT_TIMESTAMP,
----        TGT.ChangeHashKey = CONVERT(VARBINARY(20), ''),
----        TGT.ChangeHashKeyASCII = ''

----    OUTPUT
----        CURRENT_TIMESTAMP AS merge_datetime,
----        CASE WHEN Inserted.IsDeleted = CAST(1 AS BIT) THEN N'DELETE' ELSE $action END AS merge_action,
----        'Landing.MYSOLUTION_LogsEpiServer' AS full_olap_table_name,
----        'IDLog = ' + CAST(COALESCE(inserted.IDLog, deleted.IDLog) AS NVARCHAR) AS primary_key_description
----    INTO audit.merge_log_details;

----END;
----GO

----EXEC MYSOLUTION.usp_Merge_LogsEpiServer;
----GO

/*
    SCHEMA_NAME > MYSOLUTION
    TABLE_NAME > DettaglioSettimanaleAccessi
*/

/**
 * @table Landing.MYSOLUTION_DettaglioSettimanaleAccessi
 * @description 

 * @depends MYSOLUTION.DettaglioSettimanaleAccessi

SELECT TOP 100 * FROM MYSOLUTION.DettaglioSettimanaleAccessi;
*/

CREATE OR ALTER VIEW Landing.MYSOLUTION_DettaglioSettimanaleAccessiView
AS
WITH TableData
AS (
    SELECT DISTINCT
        email,
        Anno,
        Settimana,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            email,
            Anno,
            Settimana,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            [Numero Giorni Accesso nella Settimana],
            [Numero Pagine Visitate nella Settimana],
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,

        [Numero Giorni Accesso nella Settimana],
        [Numero Pagine Visitate nella Settimana]

    FROM MYSOLUTION.DettaglioSettimanaleAccessi
    WHERE RTRIM(LTRIM(COALESCE(email, N''))) <> N''
        AND Anno > 1900
)
SELECT
    -- Chiavi
    TD.email COLLATE DATABASE_DEFAULT AS email, -- EmailCliente
    TD.Anno, -- Anno
    TD.Settimana, -- Settimana

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Altri campi
    TD.[Numero Giorni Accesso nella Settimana], -- NumeroGiorniAccesso
    TD.[Numero Pagine Visitate nella Settimana] -- NumeroPagineVisitate

FROM TableData TD;
GO

--DROP TABLE IF EXISTS Landing.MYSOLUTION_DettaglioSettimanaleAccessi;
GO

IF OBJECT_ID(N'Landing.MYSOLUTION_DettaglioSettimanaleAccessi', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Landing.MYSOLUTION_DettaglioSettimanaleAccessi FROM Landing.MYSOLUTION_DettaglioSettimanaleAccessiView;

    ALTER TABLE Landing.MYSOLUTION_DettaglioSettimanaleAccessi ALTER COLUMN email NVARCHAR(100) NOT NULL;

    ALTER TABLE Landing.MYSOLUTION_DettaglioSettimanaleAccessi ADD CONSTRAINT PK_Landing_MYSOLUTION_DettaglioSettimanaleAccessi PRIMARY KEY CLUSTERED (UpdateDatetime, email, Anno, Settimana);

    CREATE UNIQUE NONCLUSTERED INDEX IX_MYSOLUTION_DettaglioSettimanaleAccessi_BusinessKey ON Landing.MYSOLUTION_DettaglioSettimanaleAccessi (email, Anno, Settimana);
END;
GO

CREATE OR ALTER PROCEDURE MYSOLUTION.usp_Merge_DettaglioSettimanaleAccessi
AS
BEGIN
    SET NOCOUNT ON;

    MERGE INTO Landing.MYSOLUTION_DettaglioSettimanaleAccessi AS TGT
    USING Landing.MYSOLUTION_DettaglioSettimanaleAccessiView (nolock) AS SRC
    ON SRC.email = TGT.email AND SRC.Anno = TGT.Anno AND SRC.Settimana = TGT.Settimana

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.[Numero Giorni Accesso nella Settimana] = SRC.[Numero Giorni Accesso nella Settimana],
        TGT.[Numero Pagine Visitate nella Settimana] = SRC.[Numero Pagine Visitate nella Settimana]

    WHEN NOT MATCHED AND SRC.IsDeleted = CAST(0 AS BIT)
      THEN INSERT VALUES (
        email,
        Anno,
        Settimana,

        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
    
        [Numero Giorni Accesso nella Settimana],
        [Numero Pagine Visitate nella Settimana]
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
        'Landing.MYSOLUTION_DettaglioSettimanaleAccessi' AS full_olap_table_name,
        'email/Anno/Mese = ' + CAST(COALESCE(inserted.email, deleted.email) AS NVARCHAR) + N'/'+ CAST(COALESCE(inserted.Anno, deleted.Anno) AS NVARCHAR) + N'/' + CAST(COALESCE(inserted.Settimana, deleted.Settimana) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END;
GO

EXEC MYSOLUTION.usp_Merge_DettaglioSettimanaleAccessi;
GO

/*
    SCHEMA_NAME > MYSOLUTION
    TABLE_NAME > DettaglioSettimanaleAccessi
    STAGING_TABLE_NAME > Accessi
*/

/**
 * @table Staging.Accessi
 * @description

 * @depends Landing.MYSOLUTION_DettaglioSettimanaleAccessi

SELECT TOP 1 * FROM Landing.MYSOLUTION_DettaglioSettimanaleAccessi;
*/

--DROP TABLE IF EXISTS Staging.Accessi; DELETE FROM audit.tables WHERE provider_name = N'MyDatamartReporting' AND full_table_name = N'MYSOLUTION.DettaglioSettimanaleAccessi';
GO

IF NOT EXISTS (SELECT 1 FROM audit.tables WHERE provider_name = N'MyDatamartReporting' AND full_table_name = N'Landing.MYSOLUTION_DettaglioSettimanaleAccessi')
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
        N'Landing.MYSOLUTION_DettaglioSettimanaleAccessi',      -- full_table_name - sysname
        N'Staging.Accessi',      -- staging_table_name - sysname
        N'Fact.Accessi',      -- datawarehouse_table_name - sysname
        NULL, -- lastupdated_staging - datetime
        NULL  -- lastupdated_local - datetime
    );

END;
GO

CREATE OR ALTER VIEW Staging.AccessiView
AS
WITH Settimane
AS (
    SELECT
        D.Anno,
        D.Settimana,
        MIN(D.PKData) AS PKDataLunedi

    FROM Dim.Data D
    GROUP BY D.Anno,
        D.Settimana
),
Accessi
AS (
    SELECT
        --T.email,
        COALESCE(C.PKCliente, CASE WHEN T.email = N'' THEN -1 ELSE -101 END) AS PKCliente,
        --T.Anno,
        --T.Settimana,
        COALESCE(S.PKDataLunedi, CAST('19000101' AS DATE)) AS PKData,

        SUM(T.[Numero Giorni Accesso nella Settimana]) AS NumeroGiorniAccesso,
        SUM(T.[Numero Pagine Visitate nella Settimana]) AS NumeroPagineVisitate

    FROM Landing.MYSOLUTION_DettaglioSettimanaleAccessi T
    LEFT JOIN Dim.Cliente C ON C.Email = T.email
    LEFT JOIN Settimane S ON S.Anno = T.Anno AND S.Settimana = T.Settimana
    GROUP BY COALESCE(C.PKCliente, CASE WHEN T.email = N'' THEN -1 ELSE -101 END),
        COALESCE(S.PKDataLunedi, CAST('19000101' AS DATE))
),
TableData
AS (
    SELECT
        A.PKCliente,
        A.PKData,

        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            A.PKCliente,
            A.PKData,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            A.NumeroGiorniAccesso,
            A.NumeroPagineVisitate,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,

        A.NumeroGiorniAccesso,
        A.NumeroPagineVisitate

    FROM Accessi A
)
SELECT
    -- Chiavi
    TD.PKData,
    TD.PKCliente,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Misure
    TD.NumeroGiorniAccesso,
    TD.NumeroPagineVisitate

FROM TableData TD;
GO

--IF OBJECT_ID(N'Staging.Accessi', N'U') IS NOT NULL DROP TABLE Staging.Accessi;
GO

IF OBJECT_ID(N'Staging.Accessi', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Staging.Accessi FROM Staging.AccessiView;

    ALTER TABLE Staging.Accessi ALTER COLUMN PKData DATE NOT NULL;
    ALTER TABLE Staging.Accessi ALTER COLUMN PKCliente INT NOT NULL;

    ALTER TABLE Staging.Accessi ADD CONSTRAINT PK_Landing_MYSOLUTION_DettaglioSettimanaleAccessi PRIMARY KEY CLUSTERED (UpdateDatetime, PKData, PKCliente);

    ALTER TABLE Staging.Accessi ALTER COLUMN NumeroGiorniAccesso INT NOT NULL;
    ALTER TABLE Staging.Accessi ALTER COLUMN NumeroPagineVisitate INT NOT NULL;

    CREATE UNIQUE NONCLUSTERED INDEX IX_MYSOLUTION_DettaglioSettimanaleAccessi_BusinessKey ON Staging.Accessi (PKData, PKCliente);
END;
GO

IF OBJECT_ID(N'Staging.usp_Reload_Accessi', N'P') IS NULL EXEC('CREATE PROCEDURE Staging.usp_Reload_Accessi AS RETURN 0;');
GO

ALTER PROCEDURE Staging.usp_Reload_Accessi
AS
BEGIN

    SET NOCOUNT ON;

    DECLARE @lastupdated_staging DATETIME;
    DECLARE @provider_name NVARCHAR(60) = N'MyDatamartReporting';
    DECLARE @full_table_name sysname = N'MYSOLUTION.DettaglioSettimanaleAccessi';

    SELECT TOP 1 @lastupdated_staging = lastupdated_staging
    FROM audit.tables
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    IF (@lastupdated_staging IS NULL) SET @lastupdated_staging = CAST('19000101' AS DATETIME);

    BEGIN TRANSACTION

    TRUNCATE TABLE Staging.Accessi;

    INSERT INTO Staging.Accessi
    SELECT * FROM Staging.AccessiView
    WHERE UpdateDatetime > @lastupdated_staging;

    SELECT @lastupdated_staging = MAX(UpdateDatetime) FROM Staging.Accessi;

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

EXEC Staging.usp_Reload_Accessi;
GO

----/*
----    SCHEMA_NAME > MYSOLUTION
----    TABLE_NAME > InfoAccounts
----    STAGING_TABLE_NAME > InfoAccounts
----*/

----/**
---- * @table Staging.InfoAccounts
---- * @description

---- * @depends Landing.MYSOLUTION_InfoAccounts

----SELECT TOP 1 * FROM Landing.MYSOLUTION_InfoAccounts;
----*/

------DROP TABLE IF EXISTS Staging.InfoAccounts; DELETE FROM audit.tables WHERE provider_name = N'MyDatamartReporting' AND full_table_name = N'MYSOLUTION.InfoAccounts';
----GO

----IF NOT EXISTS (SELECT 1 FROM audit.tables WHERE provider_name = N'MyDatamartReporting' AND full_table_name = N'Landing.MYSOLUTION_InfoAccounts')
----BEGIN

----    INSERT INTO audit.tables (
----        provider_name,
----        full_table_name,
----        staging_table_name,
----        datawarehouse_table_name,
----        lastupdated_staging,
----        lastupdated_local
----    )
----    VALUES
----    (   N'MyDatamartReporting',       -- provider_name - nvarchar(60)
----        N'Landing.MYSOLUTION_InfoAccounts',      -- full_table_name - sysname
----        N'Staging.InfoAccounts',      -- staging_table_name - sysname
----        N'',      -- datawarehouse_table_name - sysname
----        NULL, -- lastupdated_staging - datetime
----        NULL  -- lastupdated_local - datetime
----    );

----END;
----GO

----IF OBJECT_ID(N'Staging.InfoAccountsView', N'V') IS NULL EXEC('CREATE VIEW Staging.InfoAccountsView AS SELECT 1 AS fld;');
----GO

----ALTER VIEW Staging.InfoAccountsView
----AS
----WITH InfoAccountsDetail
----AS (
----    SELECT
----        IA.email AS Email,
----        ROW_NUMBER() OVER (PARTITION BY IA.email ORDER BY IA.inizio DESC) AS rnDataInizioContrattoDESC,

----        COALESCE(IA.legacy_code, N'') AS CodiceCliente,
----        COALESCE(IA.ragione_sociale, N'') AS RagioneSociale,
----        COALESCE(IA.codice_fiscale, N'') AS CodiceFiscale,
----        COALESCE(CASE WHEN LEN(IA.partita_iva) > 20 OR IA.partita_iva = N'NO PARTITA IVA' THEN N'' ELSE IA.partita_iva END, N'') AS PartitaIVA,
----        COALESCE(IA.citta, N'') AS Localita,
----        COALESCE(IA.provincia, N'') AS Provincia,
----        COALESCE(IA.telefono, N'') AS Telefono,
----        COALESCE(IA.tipo, N'') AS TipoCliente,
----        --COALESCE(IA.guid_owner, N'') AS GUIDAgente,
----        COALESCE(IA.owner_name, N'') AS Agente,
----        --IA.inizio,
----        COALESCE(DIC.PKData, CAST('19000101' AS DATE)) AS PKDataInizioContratto,
----        --IA.scadenza,
----        COALESCE(DFC.PKData, CAST('19000101' AS DATE)) AS PKDataFineContratto,
----        --IA.data_disdetta,
----        COALESCE(DD.PKData, CAST('19000101' AS DATE)) AS PKDataDisdetta,
----        COALESCE(IA.stato_disdetta, N'') AS StatoDisdetta,
----        COALESCE(IA.rif_disdetta, N'') AS RiferimentoDisdetta

----    FROM Landing.MYSOLUTION_InfoAccounts IA
----    LEFT JOIN Dim.Data DIC ON DIC.PKData = IA.inizio
----    LEFT JOIN Dim.Data DFC ON DFC.PKData = IA.scadenza
----    LEFT JOIN Dim.Data DD ON DD.PKData = IA.data_disdetta
----),
----TableData
----AS (
----    SELECT
----        IAD.Email,
----        IAD.rnDataInizioContrattoDESC,

----        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
----            IAD.Email,
----            IAD.rnDataInizioContrattoDESC,
----            ' '
----        ))) AS HistoricalHashKey,
----        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
----            IAD.CodiceCliente,
----            IAD.RagioneSociale,
----            IAD.CodiceFiscale,
----            IAD.PartitaIVA,
----            IAD.Localita,
----            IAD.Provincia,
----            IAD.Telefono,
----            IAD.TipoCliente,
----            IAD.Agente,
----            IAD.PKDataInizioContratto,
----            IAD.PKDataFineContratto,
----            IAD.PKDataDisdetta,
----            IAD.StatoDisdetta,
----            IAD.RiferimentoDisdetta,
----            ' '
----        ))) AS ChangeHashKey,
----        CURRENT_TIMESTAMP AS InsertDatetime,
----        CURRENT_TIMESTAMP AS UpdateDatetime,

----        IAD.CodiceCliente,
----        IAD.RagioneSociale,
----        IAD.CodiceFiscale,
----        IAD.PartitaIVA,
----        IAD.Localita,
----        IAD.Provincia,
----        IAD.Telefono,
----        IAD.TipoCliente,
----        IAD.Agente,
----        IAD.PKDataInizioContratto,
----        IAD.PKDataFineContratto,
----        IAD.PKDataDisdetta,
----        IAD.StatoDisdetta,
----        IAD.RiferimentoDisdetta

----    FROM InfoAccountsDetail IAD
----)
----SELECT
----    -- Chiavi
----    TD.Email,
----    TD.rnDataInizioContrattoDESC,

----    -- Campi per sincronizzazione
----    TD.HistoricalHashKey,
----    TD.ChangeHashKey,
----    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
----    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
----    TD.InsertDatetime,
----    TD.UpdateDatetime,
----    CAST(0 AS BIT) AS IsDeleted,

----    -- Altri campi
----    TD.CodiceCliente,
----    TD.RagioneSociale,
----    TD.CodiceFiscale,
----    TD.PartitaIVA,
----    TD.Localita,
----    TD.Provincia,
----    TD.Telefono,
----    TD.TipoCliente,
----    TD.Agente,
----    TD.PKDataInizioContratto,
----    TD.PKDataFineContratto,
----    TD.PKDataDisdetta,
----    TD.StatoDisdetta,
----    TD.RiferimentoDisdetta

----FROM TableData TD;
----GO

------IF OBJECT_ID(N'Staging.InfoAccounts', N'U') IS NOT NULL DROP TABLE Staging.InfoAccounts;
----GO

----IF OBJECT_ID(N'Staging.InfoAccounts', N'U') IS NULL
----BEGIN
----    SELECT TOP 0 * INTO Staging.InfoAccounts FROM Staging.InfoAccountsView;

----    ALTER TABLE Staging.InfoAccounts ALTER COLUMN Email NVARCHAR(120) NOT NULL;
----    ALTER TABLE Staging.InfoAccounts ALTER COLUMN rnDataInizioContrattoDESC INT NOT NULL;

----    ALTER TABLE Staging.InfoAccounts ADD CONSTRAINT PK_Landing_MYSOLUTION_InfoAccounts PRIMARY KEY CLUSTERED (UpdateDatetime, Email, rnDataInizioContrattoDESC);

----    CREATE UNIQUE NONCLUSTERED INDEX IX_MYSOLUTION_InfoAccounts_BusinessKey ON Staging.InfoAccounts (Email, rnDataInizioContrattoDESC);
----END;
----GO

----IF OBJECT_ID(N'Staging.usp_Reload_InfoAccounts', N'P') IS NULL EXEC('CREATE PROCEDURE Staging.usp_Reload_InfoAccounts AS RETURN 0;');
----GO

----ALTER DROP PROCEDURE Staging.usp_Reload_InfoAccounts
----AS
----BEGIN

----    SET NOCOUNT ON;

----    DECLARE @lastupdated_staging DATETIME;
----    DECLARE @provider_name NVARCHAR(60) = N'MyDatamartReporting';
----    DECLARE @full_table_name sysname = N'MYSOLUTION.InfoAccounts';

----    SELECT TOP 1 @lastupdated_staging = lastupdated_staging
----    FROM audit.tables
----    WHERE provider_name = @provider_name
----        AND full_table_name = @full_table_name;

----    IF (@lastupdated_staging IS NULL) SET @lastupdated_staging = CAST('19000101' AS DATETIME);

----    BEGIN TRANSACTION

----    TRUNCATE TABLE Staging.InfoAccounts;

----    INSERT INTO Staging.InfoAccounts
----    SELECT * FROM Staging.InfoAccountsView;
----    --WHERE UpdateDatetime > @lastupdated_staging;

----    SELECT @lastupdated_staging = MAX(UpdateDatetime) FROM Staging.InfoAccounts;

----    IF (@lastupdated_staging IS NOT NULL)
----    BEGIN

----    UPDATE audit.tables
----    SET lastupdated_staging = @lastupdated_staging
----    WHERE provider_name = @provider_name
----        AND full_table_name = @full_table_name;

----    END;

----    COMMIT

----END;
----GO

----EXEC Staging.usp_Reload_InfoAccounts;
----GO

----IF OBJECT_ID(N'Staging.ClienteView', N'V') IS NULL EXEC('CREATE VIEW Staging.ClienteView AS SELECT 1 AS fld;');
----GO

----ALTER VIEW Staging.ClienteView
----AS
----WITH SoggettoCommercialeEmailDetail
----AS (
----    SELECT
----        SCE.IDSoggettoCommerciale,
----        ROW_NUMBER() OVER (PARTITION BY SCE.IDSoggettoCommerciale ORDER BY IAD.PKDataInizioContratto DESC) AS rnDataInizioContrattoDESC,

----        SCE.Email,
----        IAD.TipoCliente,
----        IAD.Agente,
----        IAD.PKDataInizioContratto,
----        IAD.PKDataFineContratto,
----        IAD.PKDataDisdetta,
----        IAD.StatoDisdetta,
----        IAD.RiferimentoDisdetta,
----        IAD.Telefono

----    FROM Staging.SoggettoCommerciale_Email SCE
----    LEFT JOIN Staging.InfoAccounts IAD ON IAD.Email = SCE.Email
----),
----GruppoAgenti
----AS (
----    SELECT
----        GA.GruppoAgenti,
----        GA.PKGruppoAgenti,
----        ROW_NUMBER() OVER (PARTITION BY GA.GruppoAgenti ORDER BY GA.PKGruppoAgenti DESC) AS rn

----    FROM Dim.GruppoAgenti GA
----    WHERE GA.GruppoAgenti NOT IN (N'', N'<???>')
----),
----TableData
----AS (
----    SELECT
----        SC.IDSoggettoCommerciale,

----        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
----            SC.IDSoggettoCommerciale,
----            ' '
----        ))) AS HistoricalHashKey,
----        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
----            SC.CodiceSoggettoCommerciale,
----            SC.IDAnagrafica,
----            SC.TipoSoggettoCommerciale,
----            SC.RagioneSociale,
----            SC.Indirizzo,
----            SC.CAP,
----            SC.Localita,
----            SC.Provincia,
----            SC.Nazione,
----            SC.CodiceFiscale,
----            SC.PartitaIVA,
----            SCED.Email,
----            SCED.TipoCliente,
----            SCED.Agente,
----            SCED.PKDataInizioContratto,
----            SCED.PKDataFineContratto,
----            SCED.PKDataDisdetta,
----            SCED.StatoDisdetta,
----            SCED.RiferimentoDisdetta,
----            GA.PKGruppoAgenti,
----            SCED.Telefono,
----            ' '
----        ))) AS ChangeHashKey,
----        CURRENT_TIMESTAMP AS InsertDatetime,
----        CURRENT_TIMESTAMP AS UpdateDatetime,

----        SC.CodiceSoggettoCommerciale,
----        SC.IDAnagrafica,
----        SC.TipoSoggettoCommerciale,
----        SC.RagioneSociale,
----        SC.Indirizzo,
----        SC.CAP,
----        SC.Localita,
----        SC.Provincia,
----        SC.Nazione,
----        SC.CodiceFiscale,
----        SC.PartitaIVA,

----        COALESCE(SCED.Email, N'') AS Email,
----        COALESCE(SCED.TipoCliente, N'') AS TipoCliente,
----        COALESCE(SCED.Agente, N'') AS Agente,
----        COALESCE(SCED.PKDataInizioContratto, CAST('19000101' AS DATE)) AS PKDataInizioContratto,
----        COALESCE(SCED.PKDataFineContratto, CAST('19000101' AS DATE)) AS PKDataFineContratto,
----        COALESCE(SCED.PKDataDisdetta, CAST('19000101' AS DATE)) AS PKDataDisdetta,
----        COALESCE(SCED.StatoDisdetta, N'') AS StatoDisdetta,
----        COALESCE(SCED.RiferimentoDisdetta, N'') AS RiferimentoDisdetta,
----        COALESCE(GA.PKGruppoAgenti, CASE WHEN COALESCE(SCED.Agente, N'') = N'' THEN -1 ELSE -101 END) AS PKGruppoAgenti,
----        COALESCE(SCED.Telefono, N'') AS Telefono

----    FROM Staging.SoggettoCommerciale SC
----    LEFT JOIN SoggettoCommercialeEmailDetail SCED ON SCED.IDSoggettoCommerciale = SC.IDSoggettoCommerciale
----        AND SCED.rnDataInizioContrattoDESC = 1
----    LEFT JOIN Import.AgenteGruppoAgenti AGA ON AGA.Agente = SCED.Agente
----    LEFT JOIN GruppoAgenti GA ON GA.GruppoAgenti = AGA.GruppoAgenti
----        AND GA.rn = 1
----)
----SELECT
----    -- Chiavi
----    TD.IDSoggettoCommerciale,

----    -- Campi per sincronizzazione
----    TD.HistoricalHashKey,
----    TD.ChangeHashKey,
----    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
----    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
----    TD.InsertDatetime,
----    TD.UpdateDatetime,
----    CAST(0 AS BIT) AS IsDeleted,

----    -- Altri campi
----    TD.Email,
----    TD.IDAnagrafica AS IDAnagraficaCometa,
----    CAST(CASE WHEN TD.IDAnagrafica > 0 THEN 1 ELSE 0 END AS BIT) AS HasAnagraficaCometa,
----    TD.CodiceSoggettoCommerciale AS CodiceCliente,
----    TD.TipoSoggettoCommerciale,
----    TD.RagioneSociale,
----    TD.CodiceFiscale,
----    TD.PartitaIVA,
----    TD.Indirizzo,
----    TD.CAP,
----    TD.Localita,
----    TD.Provincia,
----    TD.Nazione,
----    TD.TipoCliente,
----    TD.Agente,
----    TD.PKDataInizioContratto,
----    TD.PKDataFineContratto,
----    TD.PKDataDisdetta,
----    TD.StatoDisdetta,
----    TD.RiferimentoDisdetta,
----    TD.PKGruppoAgenti,
----    TD.Telefono

----FROM TableData TD;
----GO

----IF OBJECT_ID(N'Staging.ClienteExtraView', N'V') IS NULL EXEC('CREATE VIEW Staging.ClienteExtraView AS SELECT 1 AS fld;');
----GO

----ALTER VIEW Staging.ClienteExtraView
----AS
----WITH InfoAccountsDetail
----AS (
----    SELECT
----        COALESCE(IA.email, N'') AS Email,
----        ROW_NUMBER() OVER (PARTITION BY IA.email ORDER BY IA.inizio DESC) AS rnDataInizioContrattoDESC,

----        COALESCE(IA.legacy_code, N'') AS CodiceCliente,
----        COALESCE(IA.ragione_sociale, N'') AS RagioneSociale,
----        COALESCE(IA.codice_fiscale, N'') AS CodiceFiscale,
----        COALESCE(CASE WHEN LEN(IA.partita_iva) > 20 OR IA.partita_iva = N'NO PARTITA IVA' THEN N'' ELSE IA.partita_iva END, N'') AS PartitaIVA,
----        COALESCE(IA.citta, N'') AS Localita,
----        COALESCE(IA.provincia, N'') AS Provincia,
----        COALESCE(IA.telefono, N'') AS Telefono,
----        COALESCE(IA.tipo, N'') AS TipoCliente,
----        --COALESCE(IA.guid_owner, N'') AS GUIDAgente,
----        COALESCE(IA.owner_name, N'') AS Agente,
----        --IA.inizio,
----        COALESCE(DIC.PKData, CAST('19000101' AS DATE)) AS PKDataInizioContratto,
----        --IA.scadenza,
----        COALESCE(DFC.PKData, CAST('19000101' AS DATE)) AS PKDataFineContratto,
----        --IA.data_disdetta,
----        COALESCE(DD.PKData, CAST('19000101' AS DATE)) AS PKDataDisdetta,
----        COALESCE(IA.stato_disdetta, N'') AS StatoDisdetta,
----        COALESCE(IA.rif_disdetta, N'') AS RiferimentoDisdetta

----    FROM Landing.MYSOLUTION_InfoAccounts IA
----    LEFT JOIN Dim.Data DIC ON DIC.PKData = IA.inizio
----    LEFT JOIN Dim.Data DFC ON DFC.PKData = IA.scadenza
----    LEFT JOIN Dim.Data DD ON DD.PKData = IA.data_disdetta
----    WHERE COALESCE(IA.email, N'') <> N''
----),
----GruppoAgenti
----AS (
----    SELECT
----        GA.GruppoAgenti,
----        GA.PKGruppoAgenti,
----        ROW_NUMBER() OVER (PARTITION BY GA.GruppoAgenti ORDER BY GA.PKGruppoAgenti DESC) AS rn

----    FROM Dim.GruppoAgenti GA
----    WHERE GA.GruppoAgenti NOT IN (N'', N'<???>')
----),
----TableData
----AS (
----    SELECT
----        -1000-ROW_NUMBER() OVER (ORDER BY C.Email) AS IDSoggettoCommerciale,

----        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
----            -1000-ROW_NUMBER() OVER (ORDER BY C.Email),
----            ' '
----        ))) AS HistoricalHashKey,
----        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
----            IAD.Email,
----            IAD.CodiceCliente,
----            IAD.RagioneSociale,
----            IAD.CodiceFiscale,
----            IAD.PartitaIVA,
----            IAD.Localita,
----            IAD.Provincia,
----            IAD.Telefono,
----            IAD.TipoCliente,
----            IAD.Agente,
----            IAD.PKDataInizioContratto,
----            IAD.PKDataFineContratto,
----            IAD.PKDataDisdetta,
----            IAD.StatoDisdetta,
----            IAD.RiferimentoDisdetta,
----            ' '
----        ))) AS ChangeHashKey,
----        CURRENT_TIMESTAMP AS InsertDatetime,
----        CURRENT_TIMESTAMP AS UpdateDatetime,

----        IAD.Email,
----        0 AS IDAnagraficaCometa,
----        IAD.CodiceCliente,
----        N'' AS TipoSoggettoCommerciale,
----        IAD.RagioneSociale,
----        IAD.CodiceFiscale,
----        IAD.PartitaIVA,
----        N'' AS Indirizzo,
----        N'' AS CAP,
----        IAD.Localita,
----        IAD.Provincia,
----        N'' AS Nazione,
----        IAD.TipoCliente,
----        IAD.Agente,
----        IAD.PKDataInizioContratto,
----        IAD.PKDataFineContratto,
----        IAD.PKDataDisdetta,
----        IAD.StatoDisdetta,
----        IAD.RiferimentoDisdetta,
----        COALESCE(GA.PKGruppoAgenti, CASE WHEN COALESCE(IAD.Agente, N'') = N'' THEN -1 ELSE -101 END) AS PKGruppoAgenti,
----        IAD.Telefono

----    FROM InfoAccountsDetail IAD
----    LEFT JOIN Staging.Cliente C ON C.Email = IAD.Email
----    LEFT JOIN Import.AgenteGruppoAgenti AGA ON AGA.Agente = IAD.Agente
----    LEFT JOIN GruppoAgenti GA ON GA.GruppoAgenti = AGA.GruppoAgenti
----        AND GA.rn = 1
----    WHERE IAD.rnDataInizioContrattoDESC = 1
----        AND C.IDSoggettoCommerciale IS NULL
----)
----SELECT
----    -- Chiavi
----    TD.IDSoggettoCommerciale,

----    -- Campi per sincronizzazione
----    TD.HistoricalHashKey,
----    TD.ChangeHashKey,
----    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
----    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
----    TD.InsertDatetime,
----    TD.UpdateDatetime,
----    CAST(0 AS BIT) AS IsDeleted,

----    -- Altri campi
----    TD.Email,
----    TD.IDAnagraficaCometa,
----    CAST(CASE WHEN TD.IDAnagraficaCometa > 0 THEN 1 ELSE 0 END AS BIT) AS HasAnagraficaCometa,
----    TD.CodiceCliente,
----    TD.TipoSoggettoCommerciale,
----    TD.RagioneSociale,
----    TD.CodiceFiscale,
----    TD.PartitaIVA,
----    TD.Indirizzo,
----    TD.CAP,
----    TD.Localita,
----    TD.Provincia,
----    TD.Nazione,
----    TD.TipoCliente,
----    TD.Agente,
----    TD.PKDataInizioContratto,
----    TD.PKDataFineContratto,
----    TD.PKDataDisdetta,
----    TD.StatoDisdetta,
----    TD.RiferimentoDisdetta,
----    TD.PKGruppoAgenti,
----    TD.Telefono

----FROM TableData TD;
----GO

------IF OBJECT_ID(N'Staging.Cliente', N'U') IS NOT NULL DROP TABLE Staging.Cliente;
----GO

----IF OBJECT_ID(N'Staging.Cliente', N'U') IS NULL
----BEGIN
----    SELECT TOP 0 * INTO Staging.Cliente FROM Staging.ClienteView;

----    --ALTER TABLE Staging.Cliente ALTER COLUMN GUIDCliente UNIQUEIDENTIFIER NOT NULL;

----    ALTER TABLE Staging.Cliente ADD CONSTRAINT PK_Staging_Cliente PRIMARY KEY CLUSTERED (UpdateDatetime, IDSoggettoCommerciale);

----    ALTER TABLE Staging.Cliente ALTER COLUMN Email NVARCHAR(80) NOT NULL;
----    ----ALTER TABLE Staging.Cliente ALTER COLUMN PKDataCreazione DATE NOT NULL;
----    ----ALTER TABLE Staging.Cliente ALTER COLUMN PKDataUltimoLogin DATE NOT NULL;
----    ALTER TABLE Staging.Cliente ALTER COLUMN IDAnagraficaCometa INT NOT NULL;
----    ALTER TABLE Staging.Cliente ALTER COLUMN HasAnagraficaCometa BIT NOT NULL;
----    ALTER TABLE Staging.Cliente ALTER COLUMN CodiceCliente NVARCHAR(10) NOT NULL;
----    ALTER TABLE Staging.Cliente ALTER COLUMN RagioneSociale NVARCHAR(120) NOT NULL;
----    ALTER TABLE Staging.Cliente ALTER COLUMN CodiceFiscale NVARCHAR(20) NOT NULL;
----    ALTER TABLE Staging.Cliente ALTER COLUMN PartitaIVA NVARCHAR(20) NOT NULL;
----    ALTER TABLE Staging.Cliente ALTER COLUMN Indirizzo NVARCHAR(120) NOT NULL;
----    ALTER TABLE Staging.Cliente ALTER COLUMN CAP NVARCHAR(10) NOT NULL;
----    ALTER TABLE Staging.Cliente ALTER COLUMN Localita NVARCHAR(60) NOT NULL;
----    ALTER TABLE Staging.Cliente ALTER COLUMN Provincia NVARCHAR(10) NOT NULL;
----    ALTER TABLE Staging.Cliente ALTER COLUMN Nazione NVARCHAR(60) NOT NULL;
----    ----ALTER TABLE Staging.Cliente ALTER COLUMN Telefono NVARCHAR(60) NOT NULL;
----    ALTER TABLE Staging.Cliente ALTER COLUMN TipoCliente NVARCHAR(10) NOT NULL;
----    ALTER TABLE Staging.Cliente ALTER COLUMN Agente NVARCHAR(60) NOT NULL;
----    ALTER TABLE Staging.Cliente ALTER COLUMN PKDataInizioContratto DATE NOT NULL;
----    ALTER TABLE Staging.Cliente ALTER COLUMN PKDataFineContratto DATE NOT NULL;
----    ALTER TABLE Staging.Cliente ALTER COLUMN PKDataDisdetta DATE NOT NULL;
----    ALTER TABLE Staging.Cliente ALTER COLUMN StatoDisdetta NVARCHAR(60) NOT NULL;
----    ALTER TABLE Staging.Cliente ALTER COLUMN RiferimentoDisdetta NVARCHAR(60) NOT NULL;
----    ALTER TABLE Staging.Cliente ALTER COLUMN PKGruppoAgenti INT NOT NULL;
----    ALTER TABLE Staging.Cliente ALTER COLUMN Telefono NVARCHAR(60) NOT NULL;

----    CREATE UNIQUE NONCLUSTERED INDEX IX_Staging_Cliente_BusinessKey ON Staging.Cliente (IDSoggettoCommerciale);
----END;
----GO

----IF OBJECT_ID(N'Staging.usp_Reload_Cliente', N'P') IS NULL EXEC('CREATE PROCEDURE Staging.usp_Reload_Cliente AS RETURN 0;');
----GO

----ALTER PROCEDURE Staging.usp_Reload_Cliente
----AS
----BEGIN

----    SET NOCOUNT ON;

----    DECLARE @lastupdated_staging DATETIME;
----    DECLARE @provider_name NVARCHAR(60) = N'MyDatamartReporting';
----    DECLARE @full_table_name sysname = N'Landing.COMETA_SoggettoCommerciale';

----    SELECT TOP 1 @lastupdated_staging = lastupdated_staging
----    FROM audit.tables
----    WHERE provider_name = @provider_name
----        AND full_table_name = @full_table_name;

----    IF (@lastupdated_staging IS NULL) SET @lastupdated_staging = CAST('19000101' AS DATETIME);

----    BEGIN TRANSACTION

----    TRUNCATE TABLE Staging.Cliente;

----    INSERT INTO Staging.Cliente
----    SELECT * FROM Staging.ClienteView;
----    --WHERE UpdateDatetime > @lastupdated_staging;

----    SELECT @lastupdated_staging = MAX(UpdateDatetime) FROM Staging.Cliente;

----    INSERT INTO Staging.Cliente
----    SELECT * FROM Staging.ClienteExtraView;

----    IF (@lastupdated_staging IS NOT NULL)
----    BEGIN

----    UPDATE audit.tables
----    SET lastupdated_staging = @lastupdated_staging
----    WHERE provider_name = @provider_name
----        AND full_table_name = @full_table_name;

----    END;

----    COMMIT

----END;
----GO

----EXEC Staging.usp_Reload_Cliente;
----GO

--ALTER VIEW Staging.AccessiView
--AS
--WITH AccessiDettaglio
--AS (
--    SELECT
--        --LFR.Data,
--        COALESCE(D.PKData, CAST('19000101' AS DATE)) AS PKData,
--        --LFR.IDUser,
--        COALESCE(C.PKCliente, -101) AS PKCliente,
--        LFR.NumeroPagineVisitate

--    FROM Landing.MYSOLUTION_LogsForReport LFR
--    LEFT JOIN Dim.Data D ON D.PKData = LFR.Data
--    LEFT JOIN Staging.SoggettoCommerciale_Email SCE ON SCE.Email = LFR.Username
--        AND SCE.rnSoggettoCommercialeDESC = 1
--    LEFT JOIN Dim.Cliente C ON C.IDSoggettoCommerciale = SCE.IDSoggettoCommerciale
--    WHERE LFR.IsDeleted = CAST(0 AS BIT)
--),
--TableData
--AS (
--    SELECT
--        AD.PKData,
--        AD.PKCliente,

--        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
--            AD.PKData,
--            AD.PKCliente,
--            ' '
--        ))) AS HistoricalHashKey,
--        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
--            SUM(AD.NumeroPagineVisitate),
--            ' '
--        ))) AS ChangeHashKey,
--        CURRENT_TIMESTAMP AS InsertDatetime,
--        CURRENT_TIMESTAMP AS UpdateDatetime,

--        SUM(AD.NumeroPagineVisitate) AS NumeroPagineVisitate

--    FROM AccessiDettaglio AD
--    GROUP BY AD.PKData,
--        AD.PKCliente
--)
--SELECT
--    -- Chiavi
--    TD.PKData,
--    TD.PKCliente,

--    -- Campi per sincronizzazione
--    TD.HistoricalHashKey,
--    TD.ChangeHashKey,
--    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
--    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
--    TD.InsertDatetime,
--    TD.UpdateDatetime,
--    CAST(0 AS BIT) AS IsDeleted,

--    -- Altri campi
--    TD.NumeroPagineVisitate

--FROM TableData TD;
--GO

----CREATE OR ALTER VIEW Fact.OrdiniMeseInCorso AS SELECT * FROM Fact.vOrdiniMeseInCorso;
----GO

----/**
---- * @view Fact.vReportAgenti
----*/

----CREATE OR ALTER VIEW Fact.vReportAgenti
----AS
----WITH TipiCliente
----AS (
----    SELECT DISTINCT
----        TipoCliente
----    FROM Dim.Cliente
----    WHERE PKCliente > 0
----)
----SELECT DISTINCT
----    C.Agente,
----    REPLACE(REPLACE(N'Accessi per Agente %AGENTE% (Tipo cliente %TIPOCLIENTE%)', N'%AGENTE%', C.Agente), N'%TIPOCLIENTE%', TC.TipoCliente) AS EmailSubject,
----    COALESCE(EA.EmailAgente, N'alberto.turelli@gmail.com') AS EmailTo,
----    N'alberto.turelli@gmail.com' AS EmailCc,
----    C.Agente AS pAgente,
----    TC.TipoCliente AS pTipoCliente

----FROM Fact.Accessi A
----INNER JOIN Dim.Cliente C ON C.PKCliente = A.PKCliente
----LEFT JOIN Import.EmailAgenti EA ON EA.Agente = C.Agente
----CROSS JOIN TipiCliente TC
----WHERE A.PKCliente > 0

----UNION ALL

----SELECT
----    N'Tutti',
----    REPLACE(N'Accessi per Agente Tutti (Tipo cliente %TIPOCLIENTE%)', N'%TIPOCLIENTE%', TC.TipoCliente),
----    N'alberto.turelli@gmail.com',
----    N'alberto.turelli@gmail.com',
----    NULL,
----    TC.TipoCliente

----FROM TipiCliente TC;
----GO

----/**
---- * @table Import.Agenti
---- * @description Decodifica agenti
----*/

----DROP TABLE IF EXISTS Import.Agenti;
----GO

----CREATE TABLE Import.Agenti (
----	Agente	NVARCHAR(60) NOT NULL CONSTRAINT PK_Import_Agenti PRIMARY KEY CLUSTERED,
----	Email	NVARCHAR(100) NOT NULL
----);
----GO

----INSERT INTO Import.Agenti (
----	Agente,
----	Email
----)
----SELECT
----	N'CESI\PartnerUp' AS Agente,
----	N'amministrazione@partnerup.it' AS Email

----UNION ALL SELECT N'CESI\Daniele Vincenti', N'ateneo@cesimultimedia.com'
----UNION ALL SELECT N'CESI\Gesuino Scoglia', N'gesuino.scoglia@cesimultimedia.com'
----UNION ALL SELECT N'CESI\Mauro Pelizon', N'mauro.pelizon@cesimultimedia.com'
----UNION ALL SELECT N'CESI\Massimo Lori', N'massimo.lori@cesimultimedia.com'
----UNION ALL SELECT N'CESI\Alessandro Carella', N'alessandro.carella@cesimultimedia.com'
----UNION ALL SELECT N'CESI\Antonio Vampirelli', N'a.vampo@gmail.com'
----UNION ALL SELECT N'CESI\Ernesto Amadio', N'ernesto.amadio@cesimultimedia.com'
----UNION ALL SELECT N'CESI\Giuseppe Rodi', N'g.rodi@cposanremo.it'
----UNION ALL SELECT N'CESI\Carlo Bolzani', N'carlo.bolzani@cesimultimedia.com'
----UNION ALL SELECT N'CESI\Alessandro Conzadori', N'alessandro.conzadori@cesimultimedia.com'
----UNION ALL SELECT N'CESI\Giammaria Vellutino', N'g.vellutino@gmail.com';
----GO


----SELECT * FROM Import.CapiArea WHERE ADUser <> N'' ORDER BY ADUser;


----ALTER TABLE Import.Agenti ADD ADUser NVARCHAR(60) NULL;
----GO

----UPDATE Import.Agenti SET ADUser = Agente WHERE ADUser IS NULL;
----GO

----ALTER TABLE Import.Agenti ALTER COLUMN ADUser NVARCHAR(60) NOT NULL;
----GO

----/**
---- * @table Import.AgenteCapoArea
---- * @description Tabella di accesso Agente / Capo Area
----*/

----DROP TABLE IF EXISTS Import.AgenteCapoArea;
----GO

----CREATE TABLE Import.AgenteCapoArea (
----	Agente	    NVARCHAR(60) NOT NULL,
----	CapoArea	NVARCHAR(60) NOT NULL
----);
----GO

----ALTER TABLE Import.AgenteCapoArea ADD CONSTRAINT PK_Import_AgenteCapoArea PRIMARY KEY CLUSTERED (Agente, CapoArea);
----GO

----/*
----SELECT DISTINCT CapoArea FROM Dim.GruppoAgenti WHERE CapoArea LIKE N'%Carella%'
----SELECT DISTINCT CapoArea FROM Dim.GruppoAgenti WHERE CapoArea LIKE N'%Conzadori%'
----SELECT DISTINCT CapoArea FROM Dim.GruppoAgenti WHERE CapoArea LIKE N'%Vampirelli%'
----SELECT DISTINCT CapoArea FROM Dim.GruppoAgenti WHERE CapoArea LIKE N'%Bolzani%'
----SELECT DISTINCT CapoArea FROM Dim.GruppoAgenti WHERE CapoArea LIKE N'%Vincenti%'
----SELECT DISTINCT CapoArea FROM Dim.GruppoAgenti WHERE CapoArea LIKE N'%Amadio%'
----SELECT DISTINCT CapoArea FROM Dim.GruppoAgenti WHERE CapoArea LIKE N'%Scoglia%'
----SELECT DISTINCT CapoArea FROM Dim.GruppoAgenti WHERE CapoArea LIKE N'%Vellutino%'
----SELECT DISTINCT CapoArea FROM Dim.GruppoAgenti WHERE CapoArea LIKE N'%Rodi%'
----SELECT DISTINCT CapoArea FROM Dim.GruppoAgenti WHERE CapoArea LIKE N'%Lori%'
----SELECT DISTINCT CapoArea FROM Dim.GruppoAgenti WHERE CapoArea LIKE N'%Pelizon%'
----SELECT DISTINCT CapoArea FROM Dim.GruppoAgenti WHERE CapoArea LIKE N'%Partner%'
----*/

----INSERT INTO Import.AgenteCapoArea (
----	Agente,
----	CapoArea
----)
----SELECT
----    N'CESI\Alessandro Carella' AS Agente,
----    N'CARELLA ALESSANDRO' AS CapoArea

------UNION ALL SELECT N'CESI\Alessandro Conzadori', N'?'
----UNION ALL SELECT N'CESI\Antonio Vampirelli', N'ANTONIO VAMPIRELLI'
----UNION ALL SELECT N'CESI\Carlo Bolzani', N'BOLZANI CARLO ALBERTO'
------UNION ALL SELECT N'CESI\Daniele Vincenti', N'?'
----UNION ALL SELECT N'CESI\Ernesto Amadio', N'AMADIO ERNESTO'
----UNION ALL SELECT N'CESI\Gesuino Scoglia', N'GESUINO SCOGLIA'
----UNION ALL SELECT N'CESI\Giammaria Vellutino', N'VELLUTINO GIANMARIA'
----UNION ALL SELECT N'CESI\Giuseppe Rodi', N'C.P.O. SNC DI GIUSEPPE RODI'
----UNION ALL SELECT N'CESI\Massimo Lori', N'MASSIMO LORI'
----UNION ALL SELECT N'CESI\Mauro Pelizon', N'PELIZON MAURO'
----UNION ALL SELECT N'CESI\PartnerUp', N'PARTNERUP SRL';
----GO

----/**
---- * @table Import.AgenteGruppoAgenti
---- * @description Associazioni Agente (da Cliente) / GruppoAgenti
----*/

----DROP TABLE IF EXISTS Import.AgenteGruppoAgenti;
----GO

----CREATE TABLE Import.AgenteGruppoAgenti (
----    Agente NVARCHAR(60) NOT NULL CONSTRAINT PK_Import_AgenteGruppoAgenti PRIMARY KEY CLUSTERED,
----    GruppoAgenti NVARCHAR(60) NOT NULL
----);

----INSERT INTO Import.AgenteGruppoAgenti
----(
----    Agente,
----    GruppoAgenti
----)
----SELECT DISTINCT
----    Agente,
----    CASE Agente
----      WHEN N'Amadio Ernesto' THEN N'AMADIO ERNESTO'
----      WHEN N'Colucci Dario' THEN N'COLUCCI DARIO'
----      WHEN N'Coppola Sandro' THEN N'COPPOLA SANDRO'
----      WHEN N'Loprevite Antonio' THEN N'LOPREVITE - DIREZIONALE'
----      WHEN N'Mottica Gabriella' THEN N''
----      WHEN N'Multimedia Cesi' THEN N''
----      WHEN N'MySolution Romagna' THEN N''
----      WHEN N'Partnerup Partnerup' THEN N'PARTNERUP'
----      WHEN N'Pelizon Mauro' THEN N'PELIZON MAURO'
----      WHEN N'Pillirone Graziella' THEN N'PILLIRONE GRAZIELLA'
----      WHEN N'Rizzitelli Andrea' THEN N'RIZZITELLI ANDREA'
----      WHEN N'Rodi Giuseppe' THEN N'RODI GIUSEPPE 50 E 50'
----      WHEN N'Scoglia Gesuino' THEN N'GESUINO SCOGLIA'
----      WHEN N'Serpietri Federico' THEN N''
----      WHEN N'Tognarini Giuseppe' THEN N'TOGNARINI GIUSEPPE'
----      WHEN N'Turolla Paola' THEN N'TUROLLA PAOLA'
----      WHEN N'Vampirelli Antonio' THEN N'ANTONIO VAMPIRELLI'
----      WHEN N'Vellutino Gianmaria' THEN N'VELLUTINO GIANMARIA'
----      WHEN N'Vincenti Daniele' THEN N''
----      WHEN N'Zagni Zurita' THEN N'ZAGNI ZURITA PAOLA'
----      ELSE N''
----    END AS GruppoAgenti

----FROM Dim.Cliente
----ORDER BY Agente;
----GO

--/*
---- * @table Import.CapiArea
---- * @description
----*/

------DROP TABLE IF EXISTS Import.CapiArea;
----GO

----IF OBJECT_ID('Import.CapiArea', 'U') IS NULL
----BEGIN

----    SELECT
----        Acc.CapoArea,
----        Acc.CapoArea AS Agente,
----        COALESCE(ACA.Agente, N'') AS ADUser,
----        COALESCE(A.Email, N'') AS Email,
----        CAST(0 AS BIT) AS InvioEmail

----    INTO Import.CapiArea

----    FROM (

----        SELECT
----            GA.CapoArea,
----            COUNT(DISTINCT A.PKCliente) AS NumeroClienti,
----            SUM(A.NumeroAccessi) AS NumeroAccessi,
----            SUM(A.NumeroPagineVisitate) AS NumeroPagineVisitate

----        FROM Fact.Accessi A
----        INNER JOIN Dim.Cliente C ON C.PKCliente = A.PKCliente
----        INNER JOIN Dim.GruppoAgenti GA ON GA.PKGruppoAgenti = C.PKGruppoAgenti
----        WHERE GA.CapoArea <> N''
----            AND A.PKData >= CAST('20210101' AS DATE)
----        GROUP BY GA.CapoArea

----    ) Acc
----    LEFT JOIN Import.AgenteCapoArea ACA ON ACA.CapoArea = Acc.CapoArea
----    LEFT JOIN Import.Agenti A ON A.Agente = ACA.Agente
----    ORDER BY ACA.CapoArea;

----    ALTER TABLE Import.CapiArea ADD CONSTRAINT PK_Import_CapiArea PRIMARY KEY CLUSTERED (CapoArea);
----    ALTER TABLE Import.CapiArea ALTER COLUMN ADUser NVARCHAR(60) NOT NULL;
----    ALTER TABLE Import.CapiArea ALTER COLUMN Email NVARCHAR(100) NOT NULL;

----    UPDATE Import.CapiArea SET ADUser = N'CESI\Daniele Vincenti', Email = N'ateneo@cesimultimedia.com' WHERE CapoArea = N'ATENEO S.A.S.';
----    UPDATE Import.CapiArea SET InvioEmail = CAST(1 AS BIT) WHERE CapoArea IN (N'ATENEO S.A.S.', N'PARTNERUP SRL');

----END;
----GO

----SELECT * FROM Import.CapiArea;

----IF NOT EXISTS(SELECT * FROM sys.synonyms WHERE schema_id = SCHEMA_ID('MYSOLUTION') AND name = 'InfoAccounts') EXEC('CREATE SYNONYM MYSOLUTION.InfoAccounts FOR SERVER01.MyDatamartReporting.dbo.InfoAccounts;');
----IF NOT EXISTS(SELECT * FROM sys.synonyms WHERE schema_id = SCHEMA_ID('MYSOLUTION') AND name = 'LogsForReport') EXEC('CREATE DROP SYNONYM MYSOLUTION.LogsForReport FOR SERVER01.MyDatamartReporting.dbo.LogsForReport;');
----IF NOT EXISTS(SELECT * FROM sys.synonyms WHERE schema_id = SCHEMA_ID('CESI') AND name = 'Membership') EXEC('CREATE SYNONYM CESI.Membership FOR SERVER01.MyDatamartReporting.dbo.Cesi_Membership;');
----IF NOT EXISTS(SELECT * FROM sys.synonyms WHERE schema_id = SCHEMA_ID('CESI') AND name = 'Users') EXEC('CREATE SYNONYM CESI.Users FOR SERVER01.MyDatamartReporting.dbo.Cesi_Users;');

----/**
---- * @table Import.EmailAgenti
----*/

------DROP TABLE IF EXISTS Import.EmailAgenti;
----GO

----IF OBJECT_ID('Import.EmailAgenti', 'U') IS NULL
----BEGIN

----    SELECT DISTINCT
----        owner_name AS Agente,
----        'alberto.turelli@gmail.com' AS EmailAgente

----    INTO Import.EmailAgenti
----    FROM MYSOLUTION.InfoAccounts
----    ORDER BY owner_name;

----    ALTER TABLE Import.EmailAgenti ALTER COLUMN Agente NVARCHAR(60) NOT NULL;

----    ALTER TABLE Import.EmailAgenti ADD CONSTRAINT PK_Import_EmailAgenti PRIMARY KEY CLUSTERED (Agente);

----END;
----GO

----/**
---- * @storedprocedure Fact.usp_ReportAgenti
----*/

----CREATE OR ALTER PROCEDURE Fact.usp_ReportAgenti (
----    @PKDataInizioPeriodo DATE = NULL,
----    @Agente NVARCHAR(60) = NULL,
----    @TipoCliente NVARCHAR(10) = NULL
----)
----AS
----BEGIN

----    SET NOCOUNT ON;

----    DECLARE @PKDataFinePeriodo DATE;

----    IF (@PKDataInizioPeriodo IS NULL)
----    BEGIN

----        SELECT
----            @PKDataFinePeriodo = DATEADD(DAY, 1, MAX(A.PKData))

----        FROM Fact.Accessi A
----        INNER JOIN Dim.Data D ON D.PKData = A.PKData;

----        SELECT @PKDataFinePeriodo = DATEADD(DAY, 1-DATEPART(WEEKDAY, @PKDataFinePeriodo), @PKDataFinePeriodo);

----        SELECT @PKDataInizioPeriodo = DATEADD(DAY, -27, @PKDataFinePeriodo);
----    END;

----    SELECT @PKDataFinePeriodo = DATEADD(DAY, 27, @PKDataInizioPeriodo);

----    WITH Settimane
----    AS (
----        SELECT
----            D.AnnoSettimana,
----            MIN(D.PKData) AS PKDataLunedi,
----            MAX(D.PKData) AS PKDataDomenica

----        FROM Dim.Data D
----        WHERE D.PKData BETWEEN @PKDataInizioPeriodo AND @PKDataFinePeriodo
----        GROUP BY D.AnnoSettimana
----    ),
----    SettimaneNumerate
----    AS (
----        SELECT
----            S.AnnoSettimana,
----            S.PKDataLunedi,
----            S.PKDataDomenica,
----            LEFT(CONVERT(NVARCHAR(2), ROW_NUMBER() OVER (ORDER BY S.AnnoSettimana DESC)) + '^ sett. ' + CONVERT(NVARCHAR(10), S.PKDataLunedi, 103), 14) AS DescrizioneSettimana,
----            ROW_NUMBER() OVER (ORDER BY S.AnnoSettimana DESC) AS rn

----        FROM Settimane S
----    ),
----    AccessiSettimaneNumerate
----    AS (
----        SELECT
----            C.PKCliente,
----            SN.rn,
----            SN.DescrizioneSettimana,
----            SUM(A.NumeroPagineVisitate) AS NumeroPagineVisitate,
----            COUNT(DISTINCT A.PKData) AS NumeroGiorniAccesso

----        FROM Fact.Accessi A
----        INNER JOIN Dim.Cliente C ON C.PKCliente = A.PKCliente
----            AND (
----                @Agente IS NULL
----                OR C.Agente = @Agente
----            )
----            AND (
----                @TipoCliente IS NULL
----                OR C.TipoCliente = @TipoCliente
----            )
----        INNER JOIN SettimaneNumerate SN ON A.PKData BETWEEN SN.PKDataLunedi AND SN.PKDataDomenica
----        GROUP BY C.PKCliente,
----            SN.rn,
----            SN.DescrizioneSettimana
----    ),
----    Clienti
----    AS (
----        SELECT
----            C.PKCliente,
----            C.Agente,
----            C.RagioneSociale,
----            C.Email,
----            --C.Telefono,
----            C.TipoCliente,
----            C.Localita AS Comune,
----            C.IDProvincia AS Provincia,
----            C.Regione,
----            DIC.Data_IT AS DataInizio,
----            DFC.Data_IT AS DataScadenza

----        FROM Dim.Cliente C
----        INNER JOIN Dim.Data DIC ON DIC.PKData = C.PKDataInizioContratto
----        INNER JOIN Dim.Data DFC ON DFC.PKData = C.PKDataFineContratto
----        --INNER JOIN Dim.GruppoAgenti GA ON GA.PKGruppoAgenti = C.PKGruppoAgenti
----        --    AND (
----        --        @CapoArea IS NULL
----        --        OR GA.CapoArea = @CapoArea
----        --    )
----        WHERE C.IsAttivo = CAST(1 AS BIT)
----        AND (
----            @Agente IS NULL
----            OR C.Agente = @Agente
----        )
----        AND (
----            @TipoCliente IS NULL
----            OR C.TipoCliente = @TipoCliente
----        )
----    )
----    SELECT
----        C.Agente,
----        C.RagioneSociale,
----        C.Email,
----        --C.Telefono,
----        C.Comune,
----        C.Provincia,
----        C.Regione,
----        C.DataInizio,
----        C.DataScadenza,
----        SN.DescrizioneSettimana,
----        COALESCE(ASN.NumeroPagineVisitate, 0) AS NumeroPagineVisitate,
----        COALESCE(ASN.NumeroGiorniAccesso, 0) AS NumeroGiorniAccesso

----    FROM Clienti C
----    CROSS JOIN SettimaneNumerate SN
----    LEFT JOIN AccessiSettimaneNumerate ASN ON ASN.PKCliente = C.PKCliente AND ASN.rn = SN.rn

----    WHERE C.PKCliente > 0

----    ORDER BY C.Agente,
----        C.RagioneSociale,
----        SN.rn;

----END;
----GO

----GRANT EXECUTE ON Fact.usp_ReportAgenti TO cesidw_reader;
----GO

----DECLARE @PKDataInizioPeriodo DATE;
----DECLARE @Agente NVARCHAR(60);
----DECLARE @TipoCliente NVARCHAR(10);

----EXEC Fact.usp_ReportAgenti
----    @PKDataInizioPeriodo = @PKDataInizioPeriodo,
----    @Agente = @Agente,
----    @TipoCliente = @TipoCliente;
----GO


WITH CapoAreaDefaultByCAP
AS (
    SELECT
        CCA.CAP,
        MAX(CCA.CapoArea) AS CapoAreaDefault

    FROM Import.ComuneCAPAgente CCA
    GROUP BY CCA.CAP
    HAVING COUNT(DISTINCT CCA.CapoArea) = 1
),
CapoAreaDefaultByLocalita
AS (
    SELECT
        CCA.Comune AS Localita,
        MAX(CCA.CapoArea) AS CapoAreaDefault

    FROM Import.ComuneCAPAgente CCA
    GROUP BY CCA.Comune
    HAVING COUNT(1) = 1
)
SELECT
    C.CodiceCliente,
    C.RagioneSociale,
    C.IDProvincia,
    C.Localita,
    C.CAP,
    PA.CapoArea AS CapoAreaDefaultByProvincia,
    CADBCAP.CapoAreaDefault AS CapoAreaDefaultByCAP,
    CADBL.CapoAreaDefault AS CapoAreaDefaultByLocalita

FROM Staging.Cliente C
LEFT JOIN Import.ProvinciaAgente PA ON PA.IDProvincia = C.IDProvincia
LEFT JOIN CapoAreaDefaultByCAP CADBCAP ON CADBCAP.CAP = C.CAP
LEFT JOIN CapoAreaDefaultByLocalita CADBL ON CADBL.Localita = C.Localita
GO

