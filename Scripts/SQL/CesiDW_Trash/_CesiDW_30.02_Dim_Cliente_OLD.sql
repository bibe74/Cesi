USE CesiDW;
GO

/*
SET NOEXEC OFF;
--*/ SET NOEXEC ON;
GO

/**
 * @table Staging.Cliente
 * @description

 * @depends Landing.MYSOLUTION_InfoAccounts
 * @depends Landing.CESI_Membership

SELECT TOP 1 * FROM Landing.MYSOLUTION_InfoAccounts;
SELECT TOP 1 * FROM Landing.CESI_Membership;
*/

--DROP TABLE IF EXISTS Staging.Cliente; DELETE FROM audit.tables WHERE provider_name = N'MyDatamartReporting' AND full_table_name = N'Landing.MYSOLUTION_InfoAccounts';
GO

IF NOT EXISTS (SELECT 1 FROM audit.tables WHERE provider_name = N'MyDatamartReporting' AND full_table_name = N'Landing.MYSOLUTION_InfoAccounts')
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
        N'Landing.MYSOLUTION_InfoAccounts',      -- full_table_name - sysname
        N'Staging.Cliente',      -- staging_table_name - sysname
        N'Dim.Cliente',      -- datawarehouse_table_name - sysname
        NULL, -- lastupdated_staging - datetime
        NULL  -- lastupdated_local - datetime
    );

END;
GO

IF OBJECT_ID(N'Staging.ClienteView', N'V') IS NULL EXEC('CREATE VIEW Staging.ClienteView AS SELECT 1 AS fld;');
GO

ALTER VIEW Staging.ClienteView
AS
WITH AnagraficaCometaDetail
AS (
    SELECT
        T.num_riferimento COLLATE DATABASE_DEFAULT AS Email,
        A.id_anagrafica,
        --A.rag_soc_1,
        --A.rag_soc_2,
        RTRIM(LTRIM(RTRIM(LTRIM(COALESCE(A.rag_soc_1, N'')) + N' ' + RTRIM(LTRIM(COALESCE(A.rag_soc_2, N'')))))) AS RagioneSociale,
        --A.indirizzo,
        --A.indirizzo2,
        RTRIM(LTRIM(RTRIM(LTRIM(COALESCE(A.indirizzo, N'')) + N' ' + RTRIM(LTRIM(COALESCE(A.indirizzo2, N'')))))) AS Indirizzo,
        COALESCE(A.cap, N'') AS CAP,
        COALESCE(A.localita, N'') AS Localita,
        COALESCE(A.provincia, N'') AS Provincia,
        COALESCE(A.nazione, N'') AS Nazione,
        COALESCE(A.cod_fiscale, N'') AS CodiceFiscale,
        COALESCE(A.par_iva, N'') AS PartitaIVA,
        ROW_NUMBER() OVER (PARTITION BY T.num_riferimento ORDER BY A.id_anagrafica DESC) AS rn

    FROM Landing.COMETA_Anagrafica A
    INNER JOIN Landing.COMETA_Telefono T ON T.id_anagrafica = A.id_anagrafica
        AND T.tipo = 'E' -- E: Email
        AND COALESCE(T.num_riferimento, N'') <> N''
),
MySolutionUsers
AS (
    SELECT
        MSU.EMail,
        MSU.id_anagrafica,
        MSU.RagioneSociale,
        MSU.indirizzo,
        MSU.cap,
        MSU.localita,
        MSU.provincia,
        MSU.nazione,
        MSU.cod_fiscale,
        MSU.par_iva

    FROM Landing.COMETA_MySolutionUsers MSU
),
InfoAccountsDetail
AS (
    SELECT
        COALESCE(IA.legacy_code, N'') AS CodiceCliente,
        COALESCE(IA.ragione_sociale, N'') AS RagioneSociale,
        COALESCE(IA.codice_fiscale, N'') AS CodiceFiscale,
        COALESCE(CASE WHEN LEN(IA.partita_iva) > 20 OR IA.partita_iva = N'NO PARTITA IVA' THEN N'' ELSE IA.partita_iva END, N'') AS PartitaIVA,
        COALESCE(IA.citta, N'') AS Localita,
        COALESCE(IA.provincia, N'') AS Provincia,
        COALESCE(IA.email, N'') AS Email,
        COALESCE(IA.telefono, N'') AS Telefono,
        COALESCE(IA.tipo, N'') AS TipoCliente,
        --COALESCE(IA.guid_owner, N'') AS GUIDAgente,
        COALESCE(IA.owner_name, N'') AS Agente,
        --IA.inizio,
        COALESCE(DIC.PKData, CAST('19000101' AS DATE)) AS PKDataInizioContratto,
        --IA.scadenza,
        COALESCE(DFC.PKData, CAST('19000101' AS DATE)) AS PKDataFineContratto,
        --IA.data_disdetta,
        COALESCE(DD.PKData, CAST('19000101' AS DATE)) AS PKDataDisdetta,
        COALESCE(IA.stato_disdetta, N'') AS StatoDisdetta,
        COALESCE(IA.rif_disdetta, N'') AS RiferimentoDisdetta,
        ROW_NUMBER() OVER (PARTITION BY IA.email ORDER BY IA.inizio DESC) AS rn

    FROM Landing.MYSOLUTION_InfoAccounts IA
    LEFT JOIN Dim.Data DIC ON DIC.PKData = IA.inizio
    LEFT JOIN Dim.Data DFC ON DFC.PKData = IA.scadenza
    LEFT JOIN Dim.Data DD ON DD.PKData = IA.data_disdetta
),
MembershipDetail
AS (
    SELECT
        M.UserId,
        M.Email,
        M.IsApproved,
        M.IsLockedOut,
        --M.CreateDate,
        COALESCE(CD.PKData, CAST('19000101' AS DATE)) AS PKDataCreazione,
        --M.LastLoginDate,
        COALESCE(LLD.PKData, CAST('19000101' AS DATE)) AS PKDataUltimoLogin

    FROM Landing.CESI_Membership M
    LEFT JOIN Dim.Data CD ON CD.PKData = CAST(M.CreateDate AS DATE)
    LEFT JOIN Dim.Data LLD ON LLD.PKData = CAST(M.LastLoginDate AS DATE)
),
DatiCliente
AS (
    SELECT
        MD.UserId AS GUIDCliente,
        MD.Email,
        MD.IsApproved,
        MD.IsLockedOut,
        MD.PKDataCreazione,
        MD.PKDataUltimoLogin,

        COALESCE(MSU.id_anagrafica, ACD.id_anagrafica, -1) AS IDAnagraficaCometa,
        --CASE WHEN ACD.id_anagrafica IS NULL THEN 0 ELSE 1 END AS HasAnagraficaCometa,
        CASE WHEN MSU.id_anagrafica IS NULL THEN 0 ELSE 1 END AS HasAnagraficaCometa,

        IAD.CodiceCliente,
        COALESCE(MSU.RagioneSociale, ACD.RagioneSociale, IAD.RagioneSociale) AS RagioneSociale,
        COALESCE(MSU.cod_fiscale, ACD.CodiceFiscale, IAD.CodiceFiscale) AS CodiceFiscale,
        COALESCE(MSU.par_iva, ACD.PartitaIVA, IAD.PartitaIVA) AS PartitaIVA,
        COALESCE(MSU.localita, ACD.Localita, IAD.Localita) AS Localita,
        COALESCE(MSU.provincia, ACD.Provincia, IAD.Provincia) AS Provincia,
        COALESCE(MSU.nazione, ACD.Nazione, N'') AS Nazione,
        --IAD.Email,
        IAD.Telefono,
        IAD.TipoCliente,
        IAD.Agente,
        IAD.PKDataInizioContratto,
        IAD.PKDataFineContratto,
        IAD.PKDataDisdetta,
        IAD.StatoDisdetta,
        IAD.RiferimentoDisdetta

    FROM MembershipDetail MD
    INNER JOIN InfoAccountsDetail IAD ON IAD.Email = MD.Email
        AND IAD.rn = 1
    LEFT JOIN MySolutionUsers MSU ON MSU.EMail = MD.Email
    LEFT JOIN AnagraficaCometaDetail ACD ON ACD.Email = MD.Email
        AND ACD.rn = 1
),
GruppoAgenti
AS (
    SELECT
        GA.GruppoAgenti,
        GA.PKGruppoAgenti,
        ROW_NUMBER() OVER (PARTITION BY GA.GruppoAgenti ORDER BY GA.PKGruppoAgenti DESC) AS rn

    FROM Dim.GruppoAgenti GA
    WHERE GA.GruppoAgenti NOT IN (N'', N'<???>')
),
TableData
AS (
    SELECT
        DC.GUIDCliente,

        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            DC.GUIDCliente,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            DC.Email,
            DC.IsApproved,
            DC.IsLockedOut,
            DC.PKDataCreazione,
            DC.PKDataUltimoLogin,
            DC.IDAnagraficaCometa,
            DC.HasAnagraficaCometa,
            DC.CodiceCliente,
            DC.RagioneSociale,
            DC.CodiceFiscale,
            DC.PartitaIVA,
            DC.Localita,
            DC.Provincia,
            DC.Nazione,
            DC.Telefono,
            DC.TipoCliente,
            DC.Agente,
            DC.PKDataInizioContratto,
            DC.PKDataFineContratto,
            DC.PKDataDisdetta,
            DC.StatoDisdetta,
            DC.RiferimentoDisdetta,
            GA.PKGruppoAgenti,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,

        DC.Email,
        DC.IsApproved,
        DC.IsLockedOut,
        DC.PKDataCreazione,
        DC.PKDataUltimoLogin,
        DC.IDAnagraficaCometa,
        DC.HasAnagraficaCometa,
        DC.CodiceCliente,
        DC.RagioneSociale,
        DC.CodiceFiscale,
        DC.PartitaIVA,
        DC.Localita,
        DC.Provincia,
        DC.Nazione,
        DC.Telefono,
        DC.TipoCliente,
        DC.Agente,
        DC.PKDataInizioContratto,
        DC.PKDataFineContratto,
        DC.PKDataDisdetta,
        DC.StatoDisdetta,
        DC.RiferimentoDisdetta,
        COALESCE(GA.PKGruppoAgenti, CASE WHEN DC.Agente = N'' THEN -1 ELSE -101 END) AS PKGruppoAgenti

    FROM DatiCliente DC
    LEFT JOIN Import.AgenteGruppoAgenti AGA ON AGA.Agente = DC.Agente
    LEFT JOIN GruppoAgenti GA ON GA.GruppoAgenti = AGA.GruppoAgenti
        AND GA.rn = 1
)
SELECT
    -- Chiavi
    TD.GUIDCliente,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Altri campi
    TD.Email,
    TD.IsApproved,
    TD.IsLockedOut,
    TD.PKDataCreazione,
    TD.PKDataUltimoLogin,
    TD.IDAnagraficaCometa,
    TD.HasAnagraficaCometa,
    TD.CodiceCliente,
    TD.RagioneSociale,
    TD.CodiceFiscale,
    TD.PartitaIVA,
    TD.Localita,
    TD.Provincia,
    TD.Telefono,
    TD.TipoCliente,
    TD.Agente,
    TD.PKDataInizioContratto,
    TD.PKDataFineContratto,
    TD.PKDataDisdetta,
    TD.StatoDisdetta,
    TD.RiferimentoDisdetta,
    TD.PKGruppoAgenti

FROM TableData TD;
GO

--IF OBJECT_ID(N'Staging.Cliente', N'U') IS NOT NULL DROP TABLE Staging.Cliente;
GO

IF OBJECT_ID(N'Staging.Cliente', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Staging.Cliente FROM Staging.ClienteView;

    ALTER TABLE Staging.Cliente ALTER COLUMN GUIDCliente UNIQUEIDENTIFIER NOT NULL;

    ALTER TABLE Staging.Cliente ADD CONSTRAINT PK_Staging_Cliente PRIMARY KEY CLUSTERED (UpdateDatetime, GUIDCliente);

    ALTER TABLE Staging.Cliente ALTER COLUMN Email NVARCHAR(60) NOT NULL;
    ALTER TABLE Staging.Cliente ALTER COLUMN PKDataCreazione DATE NOT NULL;
    ALTER TABLE Staging.Cliente ALTER COLUMN PKDataUltimoLogin DATE NOT NULL;
    ALTER TABLE Staging.Cliente ALTER COLUMN IDAnagraficaCometa INT NOT NULL;
    ALTER TABLE Staging.Cliente ALTER COLUMN HasAnagraficaCometa BIT NOT NULL;
    ALTER TABLE Staging.Cliente ALTER COLUMN CodiceCliente NVARCHAR(10) NOT NULL;
    ALTER TABLE Staging.Cliente ALTER COLUMN RagioneSociale NVARCHAR(120) NOT NULL;
    ALTER TABLE Staging.Cliente ALTER COLUMN CodiceFiscale NVARCHAR(20) NOT NULL;
    ALTER TABLE Staging.Cliente ALTER COLUMN PartitaIVA NVARCHAR(20) NOT NULL;
    ALTER TABLE Staging.Cliente ALTER COLUMN Localita NVARCHAR(60) NOT NULL;
    ALTER TABLE Staging.Cliente ALTER COLUMN Provincia NVARCHAR(10) NOT NULL;
    ALTER TABLE Staging.Cliente ALTER COLUMN Telefono NVARCHAR(60) NOT NULL;
    ALTER TABLE Staging.Cliente ALTER COLUMN TipoCliente NVARCHAR(10) NOT NULL;
    ALTER TABLE Staging.Cliente ALTER COLUMN Agente NVARCHAR(60) NOT NULL;
    ALTER TABLE Staging.Cliente ALTER COLUMN PKDataInizioContratto DATE NOT NULL;
    ALTER TABLE Staging.Cliente ALTER COLUMN PKDataFineContratto DATE NOT NULL;
    ALTER TABLE Staging.Cliente ALTER COLUMN PKDataDisdetta DATE NOT NULL;
    ALTER TABLE Staging.Cliente ALTER COLUMN StatoDisdetta NVARCHAR(60) NOT NULL;
    ALTER TABLE Staging.Cliente ALTER COLUMN RiferimentoDisdetta NVARCHAR(60) NOT NULL;
    ALTER TABLE Staging.Cliente ALTER COLUMN PKGruppoAgenti INT NOT NULL;

    CREATE UNIQUE NONCLUSTERED INDEX IX_Staging_Cliente_BusinessKey ON Staging.Cliente (GUIDCliente);
END;
GO

IF OBJECT_ID(N'Staging.usp_Reload_Cliente', N'P') IS NULL EXEC('CREATE PROCEDURE Staging.usp_Reload_Cliente AS RETURN 0;');
GO

ALTER PROCEDURE Staging.usp_Reload_Cliente
AS
BEGIN

    SET NOCOUNT ON;

    DECLARE @lastupdated_staging DATETIME;
    DECLARE @provider_name NVARCHAR(60) = N'MyDatamartReporting';
    DECLARE @full_table_name sysname = N'Landing.MYSOLUTION_InfoAccounts';

    SELECT TOP 1 @lastupdated_staging = lastupdated_staging
    FROM audit.tables
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    IF (@lastupdated_staging IS NULL) SET @lastupdated_staging = CAST('19000101' AS DATETIME);

    BEGIN TRANSACTION

    TRUNCATE TABLE Staging.Cliente;

    INSERT INTO Staging.Cliente
    SELECT * FROM Staging.ClienteView
    WHERE UpdateDatetime > @lastupdated_staging;

    SELECT @lastupdated_staging = MAX(UpdateDatetime) FROM Staging.Cliente;

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

EXEC Staging.usp_Reload_Cliente;
GO

--DROP TABLE IF EXISTS Fact.Documenti; DROP TABLE IF EXISTS Fact.Ordini; DROP TABLE IF EXISTS Fact.Accessi; DROP TABLE IF EXISTS Dim.Cliente; DROP SEQUENCE IF EXISTS dbo.seq_Dim_Cliente;
GO

IF OBJECT_ID('dbo.seq_Dim_Cliente', 'SO') IS NULL
BEGIN

    CREATE SEQUENCE dbo.seq_Dim_Cliente START WITH 1;

END;
GO

IF OBJECT_ID('Dim.Cliente', 'U') IS NULL
BEGIN

    CREATE TABLE Dim.Cliente (
        PKCliente INT NOT NULL CONSTRAINT PK_Dim_Cliente PRIMARY KEY CLUSTERED DEFAULT (NEXT VALUE FOR dbo.seq_Dim_Cliente),
        GUIDCliente UNIQUEIDENTIFIER NULL,

	    HistoricalHashKey VARBINARY(20) NULL,
	    ChangeHashKey VARBINARY(20) NULL,
	    HistoricalHashKeyASCII VARCHAR(34) NULL,
	    ChangeHashKeyASCII VARCHAR(34) NULL,
	    InsertDatetime DATETIME NOT NULL,
	    UpdateDatetime DATETIME NOT NULL,
	    IsDeleted BIT NOT NULL,

        Email NVARCHAR(60) NOT NULL,
        IsApproved BIT NOT NULL,
        IsLockedOut BIT NOT NULL,
        PKDataCreazione DATE NOT NULL CONSTRAINT FK_Dim_Cliente_PKDataCreazione REFERENCES Dim.Data (PKData),
        PKDataUltimoLogin DATE NOT NULL CONSTRAINT FK_Dim_Cliente_PKDataUltimoLogin REFERENCES Dim.Data (PKData),
        IDAnagraficaCometa INT NOT NULL,
        HasAnagraficaCometa BIT NOT NULL,
        CodiceCliente NVARCHAR(10) NOT NULL,
        RagioneSociale NVARCHAR(120) NOT NULL,
        CodiceFiscale NVARCHAR(20) NOT NULL,
        PartitaIVA NVARCHAR(20) NOT NULL,
        Localita NVARCHAR(60) NOT NULL,
        Provincia NVARCHAR(10) NOT NULL,
        Telefono NVARCHAR(60) NOT NULL,
        TipoCliente NVARCHAR(10) NOT NULL,
        Agente NVARCHAR(60) NOT NULL,
        PKDataInizioContratto DATE NOT NULL CONSTRAINT FK_Dim_Cliente_PKDataInizioContratto REFERENCES Dim.Data (PKData),
        PKDataFineContratto DATE NOT NULL CONSTRAINT FK_Dim_Cliente_PKDataFineContratto REFERENCES Dim.Data (PKData),
        PKDataDisdetta DATE NOT NULL CONSTRAINT FK_Dim_Cliente_PKDataDisdetta REFERENCES Dim.Data (PKData),
        StatoDisdetta NVARCHAR(60) NOT NULL,
        RiferimentoDisdetta NVARCHAR(60) NOT NULL,
        PKGruppoAgenti INT NOT NULL CONSTRAINT FK_Dim_Cliente_PKGruppoAgenti REFERENCES Dim.GruppoAgenti (PKGruppoAgenti)
    );

    CREATE UNIQUE NONCLUSTERED INDEX IX_Dim_Cliente_GUIDCliente ON Dim.Cliente (GUIDCliente) WHERE GUIDCliente IS NOT NULL;
    --TODO: ripristinare CREATE UNIQUE NONCLUSTERED INDEX IX_Dim_Cliente_Email ON Dim.Cliente (Email) WHERE Email <> N'';
    
    ALTER TABLE Dim.Cliente ADD CONSTRAINT DFT_Dim_Cliente_InsertDatetime DEFAULT (CURRENT_TIMESTAMP) FOR InsertDatetime;
    ALTER TABLE Dim.Cliente ADD CONSTRAINT DFT_Dim_Cliente_UpdateDatetime DEFAULT (CURRENT_TIMESTAMP) FOR UpdateDatetime;
    ALTER TABLE Dim.Cliente ADD CONSTRAINT DFT_Dim_Cliente_IsDeleted DEFAULT (0) FOR IsDeleted;

    ALTER TABLE Dim.Cliente ADD CONSTRAINT DFT_Dim_Cliente_IsApproved DEFAULT (0) FOR IsApproved;
    ALTER TABLE Dim.Cliente ADD CONSTRAINT DFT_Dim_Cliente_IsLockedOut DEFAULT (0) FOR IsLockedOut;
    ALTER TABLE Dim.Cliente ADD CONSTRAINT DFT_Dim_Cliente_PKCreazione DEFAULT (CAST('19000101' AS DATE)) FOR PKDataCreazione;
    ALTER TABLE Dim.Cliente ADD CONSTRAINT DFT_Dim_Cliente_PKUltimoLogin DEFAULT (CAST('19000101' AS DATE)) FOR PKDataUltimoLogin;
    ALTER TABLE Dim.Cliente ADD CONSTRAINT DFT_Dim_Cliente_IDAnagraficaCometa DEFAULT (-1) FOR IDAnagraficaCometa;
    ALTER TABLE Dim.Cliente ADD CONSTRAINT DFT_Dim_Cliente_HasAnagraficaCometa DEFAULT (0) FOR HasAnagraficaCometa;
    ALTER TABLE Dim.Cliente ADD CONSTRAINT DFT_Dim_Cliente_PKInizioContratto DEFAULT (CAST('19000101' AS DATE)) FOR PKDataInizioContratto;
    ALTER TABLE Dim.Cliente ADD CONSTRAINT DFT_Dim_Cliente_PKFineContratto DEFAULT (CAST('19000101' AS DATE)) FOR PKDataFineContratto;
    ALTER TABLE Dim.Cliente ADD CONSTRAINT DFT_Dim_Cliente_PKDataDisdetta DEFAULT (CAST('19000101' AS DATE)) FOR PKDataDisdetta;
    ALTER TABLE Dim.Cliente ADD CONSTRAINT DFT_Dim_Cliente_PKGruppoAgenti DEFAULT (-1) FOR PKGruppoAgenti;

    INSERT INTO Dim.Cliente (
        PKCliente,
        GUIDCliente,
        Email,
        CodiceCliente,
        RagioneSociale,
        CodiceFiscale,
        PartitaIVA,
        Localita,
        Provincia,
        Telefono,
        TipoCliente,
        Agente,
        StatoDisdetta,
        RiferimentoDisdetta
    )
    VALUES
    (   -1,         -- PKCliente - int
        NULL,      -- GUIDCliente - uniqueidentifier
        N'',       -- Email - nvarchar(60)
        N'',       -- CodiceCliente - nvarchar(10)
        N'',       -- RagioneSociale - nvarchar(120)
        N'',       -- CodiceFiscale - nvarchar(20)
        N'',       -- PartitaIVA - nvarchar(20)
        N'',       -- Localita - nvarchar(60)
        N'',       -- Provincia - nvarchar(10)
        N'',       -- Telefono - nvarchar(60)
        N'',       -- TipoCliente - nvarchar(10)
        N'',       -- Agente - nvarchar(60)
        N'',       -- StatoDisdetta - nvarchar(60)
        N''        -- RiferimentoDisdetta - nvarchar(60)
    ),
    (   -101,         -- PKCliente - int
        NULL,      -- GUIDCliente - uniqueidentifier
        N'???',       -- CodiceCliente - nvarchar(10)
        N'<???>',       -- RagioneSociale - nvarchar(120)
        N'',       -- CodiceFiscale - nvarchar(20)
        N'',       -- PartitaIVA - nvarchar(20)
        N'',       -- Localita - nvarchar(60)
        N'',       -- Provincia - nvarchar(10)
        N'',       -- Email - nvarchar(60)
        N'',       -- Telefono - nvarchar(60)
        N'',       -- TipoCliente - nvarchar(10)
        N'<???>',       -- Agente - nvarchar(60)
        N'',       -- StatoDisdetta - nvarchar(60)
        N''        -- RiferimentoDisdetta - nvarchar(60)
    );

    ALTER SEQUENCE dbo.seq_Dim_Cliente RESTART WITH 1;

END;
GO

IF OBJECT_ID(N'Dim.usp_Merge_Cliente', N'P') IS NULL EXEC('CREATE PROCEDURE Dim.usp_Merge_Cliente AS RETURN 0;');
GO

ALTER PROCEDURE Dim.usp_Merge_Cliente
AS
BEGIN

    SET NOCOUNT ON;

    BEGIN TRANSACTION 

    DECLARE @provider_name NVARCHAR(60) = N'MyDatamartReporting';
    DECLARE @full_table_name sysname = N'COMETA.Anagrafica';

    MERGE INTO Dim.Cliente AS TGT
    USING Staging.Cliente (nolock) AS SRC
    ON SRC.GUIDCliente = TGT.GUIDCliente

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.IsDeleted = SRC.IsDeleted,
        
        TGT.Email = SRC.Email,
        TGT.IsApproved = SRC.IsApproved,
        TGT.IsLockedOut = SRC.IsLockedOut,
        TGT.PKDataCreazione = SRC.PKDataCreazione,
        TGT.PKDataUltimoLogin = SRC.PKDataUltimoLogin,
        TGT.IDAnagraficaCometa = SRC.IDAnagraficaCometa,
        TGT.HasAnagraficaCometa = TGT.HasAnagraficaCometa,
        TGT.RagioneSociale = SRC.RagioneSociale,
        TGT.CodiceFiscale = SRC.CodiceFiscale,
        TGT.PartitaIVA = SRC.PartitaIVA,
        TGT.Localita = SRC.Localita,
        TGT.Provincia = SRC.Provincia,
        TGT.Telefono = SRC.Telefono,
        TGT.TipoCliente = SRC.TipoCliente,
        TGT.Agente = SRC.Agente,
        TGT.PKDataInizioContratto = SRC.PKDataInizioContratto,
        TGT.PKDataFineContratto = SRC.PKDataFineContratto,
        TGT.PKDataDisdetta = SRC.PKDataDisdetta,
        TGT.StatoDisdetta = SRC.StatoDisdetta,
        TGT.RiferimentoDisdetta = SRC.RiferimentoDisdetta,
        TGT.PKGruppoAgenti = SRC.PKGruppoAgenti

    WHEN NOT MATCHED
      THEN INSERT (
        GUIDCliente,
        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
        Email,
        IsApproved,
        IsLockedOut,
        PKDataCreazione,
        PKDataUltimoLogin,
        IDAnagraficaCometa,
        HasAnagraficaCometa,
        CodiceCliente,
        RagioneSociale,
        CodiceFiscale,
        PartitaIVA,
        Localita,
        Provincia,
        Telefono,
        TipoCliente,
        Agente,
        PKDataInizioContratto,
        PKDataFineContratto,
        PKDataDisdetta,
        StatoDisdetta,
        RiferimentoDisdetta,
        PKGruppoAgenti
      )
      VALUES (
        SRC.GUIDCliente,
        SRC.HistoricalHashKey,
        SRC.ChangeHashKey,
        SRC.HistoricalHashKeyASCII,
        SRC.ChangeHashKeyASCII,
        SRC.InsertDatetime,
        SRC.UpdateDatetime,
        SRC.IsDeleted,
        SRC.Email,
        SRC.IsApproved,
        SRC.IsLockedOut,
        SRC.PKDataCreazione,
        SRC.PKDataUltimoLogin,
        SRC.IDAnagraficaCometa,
        SRC.HasAnagraficaCometa,
        SRC.CodiceCliente,
        SRC.RagioneSociale,
        SRC.CodiceFiscale,
        SRC.PartitaIVA,
        SRC.Localita,
        SRC.Provincia,
        SRC.Telefono,
        SRC.TipoCliente,
        SRC.Agente,
        SRC.PKDataInizioContratto,
        SRC.PKDataFineContratto,
        SRC.PKDataDisdetta,
        SRC.StatoDisdetta,
        SRC.RiferimentoDisdetta,
        SRC.PKGruppoAgenti
      )

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        $action AS merge_action,
        'Staging.Cliente' AS full_olap_table_name,
        'GUIDCliente = ' + CAST(COALESCE(inserted.GUIDCliente, deleted.GUIDCliente) AS NVARCHAR(1000)) AS primary_key_description
    INTO audit.merge_log_details;

    --DELETE FROM Dim.Cliente
    --WHERE IsDeleted = CAST(1 AS BIT);

    ---- TODO :: correggere anagrafica agenti: BEGIN
    --DELETE A
    --FROM Fact.Accessi A
    --INNER JOIN Dim.Cliente C ON C.PKCliente = A.PKCliente
    --WHERE C.Agente IN (N'', N'Altavilla Mariano');

    --DELETE O
    --FROM Fact.Ordini O
    --INNER JOIN Dim.Cliente C ON C.PKCliente = O.PKCliente
    --WHERE C.Agente IN (N'', N'Altavilla Mariano');

    DELETE FROM Dim.Cliente WHERE Agente IN (N'', N'Altavilla Mariano');
    -- TODO :: correggere anagrafica agenti: END

    UPDATE audit.tables
    SET lastupdated_local = lastupdated_staging
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    COMMIT TRANSACTION;

END;
GO

EXEC Dim.usp_Merge_Cliente;
GO
