USE CesiDW;
GO

/*
SET NOEXEC OFF;
--*/ SET NOEXEC ON;
GO

/*
 * @table Import.CapiArea
 * @description
*/

--DROP TABLE IF EXISTS Import.CapiArea;
GO

IF OBJECT_ID('Import.CapiArea', 'U') IS NULL
BEGIN

    CREATE TABLE Import.CapiArea (
	    CapoArea NVARCHAR(60) NOT NULL CONSTRAINT PK_Import_CapiArea PRIMARY KEY CLUSTERED,
	    Agente NVARCHAR(60) NOT NULL,
	    ADUser NVARCHAR(60) NOT NULL,
	    Email NVARCHAR(100) NOT NULL,
	    InvioEmail BIT NOT NULL,
        AgenteBudget NVARCHAR(60) NOT NULL,
        Prefisso NVARCHAR(3) NOT NULL
    );

    INSERT INTO Import.CapiArea (
        CapoArea,
        Agente,
        ADUser,
        Email,
        InvioEmail,
        AgenteBudget,
        Prefisso
    )
    SELECT
        N'ADVERTISING FRAMEWORK' AS CapoArea,
        N'' AS Agente,
        N'' AS ADUser,
        N'' AS Email,
        CAST(0 AS BIT) AS InvioEmail,
        N'' AS AgenteBudget,
        N'ADV' AS Prefisso

    UNION ALL SELECT N'AMADIO ERNESTO', N'Amadio Ernesto', N'CESI\Ernesto Amadio', N'ernesto.amadio@cesimultimedia.com', 1, N'AMADIO', N'AMA'
    UNION ALL SELECT N'ANTONIO VAMPIRELLI', N'Vampirelli Antonio', N'CESI\Antonio Vampirelli', N'antonio.vampirelli@cesimultimedia.com', 1, N'VAMPIRELLI', N'VAM'
    UNION ALL SELECT N'ARRIGO ANGELO', N'', N'', N'', 0, N'', N'ARR'
    UNION ALL SELECT N'ATENEO S.A.S.', N'ATENEO S.A.S.', N'CESI\Daniele Vincenti', N'ateneo@cesimultimedia.com', 1, N'ATENEO', N'VIN'
    UNION ALL SELECT N'BOLZANI CARLO ALBERTO', N'Bolzani Carlo Alberto', N'CESI\Carlo Bolzani', N'carlo.bolzani@cesimultimedia.com', 1, N'BOLZANI', N'BOL'
    UNION ALL SELECT N'C.P.O. SNC DI GIUSEPPE RODI', N'Rodi Giuseppe', N'CESI\Giuseppe Rodi', N'giuseppe.rodi@cesimultimedia.com', 1, N'', N'CPO'
    UNION ALL SELECT N'CARELLA ALESSANDRO', N'Carella Alessandro', N'CESI\Alessandro Carella', N'alessandro.carella@cesimultimedia.com', 1, N'CARELLA', N'CAR'
    UNION ALL SELECT N'CIABRELLI LORIS', N'', N'', N'', 0, N'', N'CIA'
    UNION ALL SELECT N'COPPOLA SANDRO', N'', N'', N'', 0, N'', N'COP'
    UNION ALL SELECT N'DIREZIONALE LIGURIA', N'Rodi Giuseppe', N'CESI\Giuseppe Rodi', N'giuseppe.rodi@cesimultimedia.com', 1, N'RODI', N'ROD'
    UNION ALL SELECT N'DIREZIONALI', N'Polinari Mirco', N'CESI\Mirco Polinari', N'mirco.polinari@cesimultimedia.it', 1, N'DIREZIONALI', N'DIR'
    UNION ALL SELECT N'EQUILIBRI SAS DI MARIANO ALTAVILLA', N'', N'', N'', 0, N'', N'ALT'
    UNION ALL SELECT N'EURODATA 2000 SRL - COLUCCI DARIO', N'', N'', N'', 0, N'', N'COL'
    UNION ALL SELECT N'GATTUSO NICOLA', N'', N'', N'', 0, N'', N'GAT'
    UNION ALL SELECT N'GESUINO SCOGLIA', N'Scoglia Gesuino', N'CESI\Gesuino Scoglia', N'gesuino.scoglia@cesimultimedia.com', 1, N'SCOGLIA', N'SCO'
    UNION ALL SELECT N'IL NEGOZIO GIURIDICO SRL', N'', N'', N'', 0, N'', N'NEG'
    UNION ALL SELECT N'LOPREVITE ANTONIO', N'', N'', N'', 0, N'LOPREVITE', N'LOP'
    UNION ALL SELECT N'MASSIMO LORI', N'Lori Massimo', N'CESI\Massimo Lori', N'massimo.lori@cesimultimedia.com', 1, N'LORI', N'LOR'
    UNION ALL SELECT N'MY CHOISE PIERANTOGNETTI ANDREA', N'', N'', N'', 0, N'', N'PIE'
    UNION ALL SELECT N'PARTNERUP SRL', N'PartnerUp Srl', N'CESI\PartnerUp', N'amministrazione@partnerup.it', 1, N'PARTNERUP', N'PUP'
    UNION ALL SELECT N'PELIZON MAURO', N'Pelizon Mauro', N'CESI\Mauro Pelizon', N'mauro.pelizon@cesimultimedia.com', 1, N'PELIZON', N'PEL'
    UNION ALL SELECT N'SEPA SRL', N'', N'', N'', 0, N'SEPA', N'SEP'
    UNION ALL SELECT N'STECCONI DARIO', N'Stecconi Dario', N'CESI\Dario Stecconi', 'dario.stecconi@cesimultimedia.com', 1, N'STECCONI', N'STE'
    UNION ALL SELECT N'TOGNARINI GIUSEPPE', N'', N'', N'', 0, N'', N'TOG'
    UNION ALL SELECT N'TUROLLA PAOLA', N'', N'', N'', 0, N'TUROLLA', N'TUR'
    UNION ALL SELECT N'VAJ ALBERTO', N'', N'', N'', 0, N'', N'VAJ'
    UNION ALL SELECT N'VELLUTINO GIANMARIA', N'Vellutino Giammaria', N'CESI\Giammaria Vellutino', N'giammaria.vellutino@cesimultimedia.com', 1, N'VELLUTINO', N'VEL';

    CREATE UNIQUE NONCLUSTERED INDEX IX_Import_CapiArea_Prefisso ON Import.CapiArea (Prefisso);

    ALTER TABLE Import.CapiArea ADD ProvvigioneNuovo DECIMAL(18, 2) NULL;
    ALTER TABLE Import.CapiArea ADD ProvvigioneRinnovo DECIMAL(18, 2) NULL;

    UPDATE CA
    SET CA.ProvvigioneNuovo = 100.0 * PD.ProvvigioneNuovo,
        CA.ProvvigioneRinnovo = 100.0 * PD.ProvvigioneRinnovo

    FROM dbo.[ProvvigioniDefault$] PD
    INNER JOIN Import.CapiArea CA ON CA.CapoArea = PD.CapoArea;

    -- Aggiornamento del 5/4/2024
    INSERT INTO Import.CapiArea (
        CapoArea,
        Agente,
        ADUser,
        Email,
        InvioEmail,
        AgenteBudget,
        Prefisso,
        ProvvigioneNuovo,
        ProvvigioneRinnovo
    )
    VALUES
    (   N'PAVANELLO GIANLUCA',  -- CapoArea - nvarchar(60)
        N'Pavanello Gianluca',  -- Agente - nvarchar(60)
        N'CESI\Gianluca Pavanello',  -- ADUser - nvarchar(60)
        N'gianluca.pavanello@cesimultimedia.com',  -- Email - nvarchar(100)
        1, -- InvioEmail - bit
        N'Pavanello',  -- AgenteBudget - nvarchar(60)
        N'PAV',  -- Prefisso - nvarchar(3)
        30.0, -- ProvvigioneNuovo - decimal(18, 2)
        20.0  -- ProvvigioneRinnovo - decimal(18, 2)
    );

    -- Richiesta del 1/7/2024
    UPDATE Import.CapiArea
    SET InvioEmail = CAST(0 AS BIT)
    WHERE CapoArea IN (
        N'STECCONI DARIO',
        N'CARELLA ALESSANDRO'
    );

END;
GO

/**
 * @table Import.Amministratori
 * @description Amministratori (per la reportistica)
*/

--DROP TABLE IF EXISTS Import.Amministratori;
GO

IF OBJECT_ID('Import.Amministratori', 'U') IS NULL
BEGIN

    CREATE TABLE Import.Amministratori (
	    Amministratore	NVARCHAR(60) NOT NULL CONSTRAINT PK_Import_Amministratori PRIMARY KEY CLUSTERED,
        ADUser          NVARCHAR(60) NOT NULL,
	    Email	        NVARCHAR(100) NOT NULL
    );

    INSERT INTO Import.Amministratori (
	    Amministratore,
        ADUser,
	    Email
    )
    SELECT
        N'Giuggioli Andrea' AS Amministratore,
	    N'CESI\Andrea Giuggioli' AS ADUser,
	    N'andrea.giuggioli@cesimultimedia.com' AS Email

    UNION ALL SELECT N'Cipriani Elio (VPN)', N'CESI\CiprianiVPN', N''
    UNION ALL SELECT N'Cipriani Elio', N'CESI\Elio Cipriani', N'elio.cipriani@cesimultimedia.com'
    UNION ALL SELECT N'Barbaglia Valeria', N'CESI\Barbaglia', N'valeria.barbaglia@cesimultimedia.com'
    UNION ALL SELECT N'Loprevite Antonio', N'CESI\Antonio Loprevite', N'antonio.loprevite@cesimultimedia.com'
    UNION ALL SELECT N'Lobrano Giuseppe', N'CESI\Giuseppe Lobrano', N'giuseppe.lobrano@cesimultimedia.com'
    UNION ALL SELECT N'Turolla Paola', N'CESI\Paola Turolla', N'paola.turolla@cesimultimedia.com'
    UNION ALL SELECT N'SQL Administrator', N'CESI\sadmin', N''
    UNION ALL SELECT N'Turelli Alberto', N'CESI\Alberto Turelli', N'alberto.turelli@gmail.com'
    UNION ALL SELECT N'Turelli Alberto (Metra)', N'METRA\bs_turelli', N''
    UNION ALL SELECT N'Eleonora Soravia', N'CESI\Eleonora Soravia', N'eleonora.soravia@cesimultimedia.com'
    UNION ALL SELECT N'Valentina Borroni', N'CESI\Valentina Borroni', N'valentina.borroni@cesimultimedia.it';

END;
GO

----/**
---- * @view Import.vReportAgenti
---- * @description Vista per invio automatico report Agenti
----*/

----CREATE OR ALTER VIEW Import.vReportAgenti
----AS
----SELECT
----    Email AS pTo,
----    N'andrea.giuggioli@cesimultimedia.it' AS pCc,
----    N'alberto.turelli@gmail.com' AS pBcc,
----    N'andrea.giuggioli@cesimultimedia.it' AS pReplyTo,
----    REPLACE(N'Report Accessi %AGENTE%', N'%AGENTE%', Agente) AS pSubject,
----    REPLACE(N'Report Fatturato Formazione - Master e Revisione - %AGENTE%', N'%AGENTE%', Agente) AS pSubjectFatturatoFormazioneMasterRevisione,
----    CapoArea AS pCapoArea,
----    N'' AS pComment

----FROM Import.CapiArea
----WHERE InvioEmail = CAST(1 AS BIT)

----UNION ALL

----SELECT
----    N'cipriani@cesimultimedia.it;paola.turolla@cesimultimedia.it;giuseppe.lobrano@cesimultimedia.com;valeria.barbaglia@cesimultimedia.it;antonio.loprevite@cesimultimedia.it;andrea.giuggioli@cesimultimedia.it;eleonora.soravia@cesimultimedia.it;valentina.borroni@cesimultimedia.it' AS pTo,
----    N'andrea.giuggioli@cesimultimedia.it' AS pCc,
----    N'alberto.turelli@gmail.com' AS pBcc,
----    N'andrea.giuggioli@cesimultimedia.it' AS pReplyTo,
----    N'Report Accessi' AS pSubject,
----    N'Report Fatturato Formazione - Master e Revisione' AS pSubjectFatturatoFormazioneMasterRevisione,
----    NULL AS pCapoArea,
----    N'' AS pComment;
----GO

/**
 * @table Import.ProfiliDocumento
 * @description Flag per profili documento
*/

--DROP TABLE IF EXISTS Import.ProfiliDocumento;
GO

IF OBJECT_ID('Import.ProfiliDocumento', 'U') IS NULL
BEGIN

    SELECT
        id_prof_documento,
        descrizione AS Profilo,
        CAST(CASE WHEN descrizione LIKE N'FATTURA%' THEN 1 ELSE 0 END AS BIT) AS IsProfiloValidoPerStatisticaFatturato,
        CAST(CASE WHEN descrizione = N'FATTURA SEMINARI' THEN 1 ELSE 0 END AS BIT) AS IsProfiloValidoPerStatisticaFatturatoFormazione

    INTO Import.ProfiliDocumento

    FROM Landing.COMETA_Profilo_Documento
    ORDER BY id_prof_documento;

    ALTER TABLE Import.ProfiliDocumento ADD CONSTRAINT PK_Import_ProfiliDocumento PRIMARY KEY CLUSTERED (id_prof_documento);

    ALTER TABLE Import.ProfiliDocumento ALTER COLUMN IsProfiloValidoPerStatisticaFatturato BIT NOT NULL;
    ALTER TABLE Import.ProfiliDocumento ALTER COLUMN IsProfiloValidoPerStatisticaFatturatoFormazione BIT NOT NULL;

END;
GO

/**
 * @table Import.Budget
 * @description Budget mensile per capo area
*/

--DROP TABLE IF EXISTS Import.Budget;
GO

IF OBJECT_ID('Import.Budget', 'U') IS NULL
BEGIN

    CREATE TABLE Import.Budget (
        PKDataInizioMese DATE NOT NULL,
        CapoArea NVARCHAR(60) NOT NULL,
        ImportoBudgetNuoveVendite DECIMAL(18, 2) NOT NULL,
        ImportoBudgetRinnovi DECIMAL(18, 2) NOT NULL,

        CONSTRAINT PK_Import_Budget PRIMARY KEY CLUSTERED (PKDataInizioMese, CapoArea)
    );

END;
GO

/* Verifiche pre importazione

SELECT SB.Agente, SUM(SB.BudgetNuoveVendite) AS BudgetNuoveVendite, SUM(SB.BudgetRinnovi) AS BudgetRinnovi
FROM Import.stg_Budget SB
LEFT JOIN Import.CapiArea CA ON CA.AgenteBudget = SB.Agente
WHERE CA.AgenteBudget IS NULL
GROUP BY SB.Agente
ORDER BY SB.Agente;
GO

SELECT SB.Agente, SUM(SB.BudgetNuoveVendite) AS BudgetNuoveVendite, SUM(SB.BudgetRinnovi) AS BudgetRinnovi
FROM Import.stg_Budget SB
INNER JOIN Import.CapiArea CA ON CA.AgenteBudget = SB.Agente
LEFT JOIN Dim.Data D ON D.PKData = SB.DataInizioMese
WHERE D.PKData IS NULL
GROUP BY SB.Agente
ORDER BY SB.Agente;
GO

*/

CREATE OR ALTER VIEW Import.BudgetView
AS
SELECT
    D.PKData AS PKDataInizioMese,
    CA.CapoArea,
    COALESCE(SB.BudgetNuoveVendite, 0.0) AS ImportoBudgetNuoveVendite,
    COALESCE(SB.BudgetRinnovi, 0.0) AS ImportoBudgetRinnovi

FROM Import.stg_Budget SB
INNER JOIN Import.CapiArea CA ON CA.AgenteBudget = SB.Agente
INNER JOIN Dim.Data D ON D.PKData = SB.DataInizioMese;
GO

CREATE OR ALTER PROCEDURE Import.usp_Reload_Budget
AS
BEGIN

    SET NOCOUNT ON;

    BEGIN TRANSACTION;

    BEGIN TRY

        TRUNCATE TABLE Import.Budget;

        INSERT INTO Import.Budget (
            PKDataInizioMese,
            CapoArea,
            ImportoBudgetNuoveVendite,
            ImportoBudgetRinnovi
        )
        SELECT
            PKDataInizioMese,
            CapoArea,
            ImportoBudgetNuoveVendite,
            ImportoBudgetRinnovi

        FROM Import.BudgetView
        ORDER BY PKDataInizioMese,
            CapoArea;

        COMMIT TRANSACTION;

    END TRY
    BEGIN CATCH

        RAISERROR('Errore nell''importazione del budget', 16, 1);

        ROLLBACK;

    END CATCH

END;
GO

EXEC Import.usp_Reload_Budget;
GO

/**
 * @table Import.Budget2022
 * @description Budget agenti 2022
*/

BEGIN TRANSACTION 

DELETE FROM Import.Budget WHERE PKDataInizioMese BETWEEN '20220101' AND '20221231';

INSERT INTO Import.Budget (
    PKDataInizioMese,
    CapoArea,
    ImportoBudgetNuoveVendite,
    ImportoBudgetRinnovi
)
SELECT
    DATEADD(MONTH, CASE unpvt.Mese
      WHEN 'gennaio' THEN 1
      WHEN 'febbraio' THEN 2
      WHEN 'marzo' THEN 3
      WHEN 'aprile' THEN 4
      WHEN 'maggio' THEN 5
      WHEN 'giugno' THEN 6
      WHEN 'luglio' THEN 7
      WHEN 'agosto' THEN 8
      WHEN 'settembre' THEN 9
      WHEN 'ottobre' THEN 10
      WHEN 'novembre' THEN 11
      WHEN 'dicembre' THEN 12
    END - 1, CAST('20220101' AS DATE)) AS PKDataInizioMese,
    CapoArea,
    unpvt.Budget AS ImportoBudgetNuoveVendite,
    0.0 AS ImportoBudgetRinnovi

FROM (
    SELECT B.*, CA.CapoArea
    FROM dbo.[BudgetNuoveVendite2022$] B
    INNER JOIN Import.CapiArea CA ON CA.AgenteBudget = B.Agenzia
) p
UNPIVOT (Budget FOR Mese IN (gennaio, febbraio, marzo, aprile, maggio, giugno, luglio, agosto, settembre, ottobre, novembre, dicembre)) unpvt;

ROLLBACK TRANSACTION 
GO

BEGIN TRANSACTION 

UPDATE dbo.[Budget2024$]
SET Agente = N'TUROLLA'
WHERE Agente = N'Turolla/Direzionali';

UPDATE dbo.[Budget2024$]
SET Agente = N'RODI'
WHERE Agente LIKE N'Direzionali Liguria';

--SELECT B.*, CA.CapoArea
--FROM dbo.[Budget2024$] B
--LEFT JOIN Import.CapiArea CA ON CA.AgenteBudget = B.Agente

INSERT INTO Import.Budget (
    PKDataInizioMese,
    CapoArea,
    ImportoBudgetNuoveVendite,
    ImportoBudgetRinnovi
)
SELECT
    DATEADD(MONTH, CASE unpvt.Mese
        WHEN 'gennaio' THEN 1
        WHEN 'febbraio' THEN 2
        WHEN 'marzo' THEN 3
        WHEN 'aprile' THEN 4
        WHEN 'maggio' THEN 5
        WHEN 'giugno' THEN 6
        WHEN 'luglio' THEN 7
        WHEN 'agosto' THEN 8
        WHEN 'settembre' THEN 9
        WHEN 'ottobre' THEN 10
        WHEN 'novembre' THEN 11
        WHEN 'dicembre' THEN 12
    END - 1, CAST('20240101' AS DATE)) AS PKDataInizioMese,
    CapoArea,
    unpvt.Budget AS ImportoBudgetNuoveVendite,
    0.0 AS ImportoBudgetRinnovi

FROM (
    SELECT B.*, CA.CapoArea
    FROM dbo.[Budget2024$] B
    INNER JOIN Import.CapiArea CA ON CA.AgenteBudget = B.Agente
) p
UNPIVOT (Budget FOR Mese IN (gennaio, febbraio, marzo, aprile, maggio, giugno, luglio, agosto, settembre, ottobre, novembre, dicembre)) unpvt;

ROLLBACK TRANSACTION 

SELECT * FROM Dim.CapoArea

/** Richiesta di Antonio Loprevite del 5/5/2022

--SELECT * FROM Import.Budget WHERE CapoArea = N'PARTNERUP SRL' AND PKDataInizioMese >= CAST('20220101' AS DATE);
GO

WITH NewBudgetPartnerUp
AS (
    SELECT
        1 AS Mese,
        70000 AS Budget

    UNION ALL SELECT 2, 70000
    UNION ALL SELECT 3, 70000
    UNION ALL SELECT 4, 70000
    UNION ALL SELECT 5, 50000
    UNION ALL SELECT 6, 50000
    UNION ALL SELECT 7, 50000
    UNION ALL SELECT 8, 50000
    UNION ALL SELECT 9, 80000
    UNION ALL SELECT 10, 80000
    UNION ALL SELECT 11, 80000
    UNION ALL SELECT 12, 80000
)
SELECT
    B.PKDataInizioMese,
    B.CapoArea,
    B.ImportoBudgetNuoveVendite,
    NBPU.Budget AS ImportoBudgetNuoveVendite_NEW

FROM NewBudgetPartnerUp NBPU
INNER JOIN Dim.Data D ON D.Mese = NBPU.Mese
    AND D.Anno = 2022
    AND DATEPART(DAY, D.PKData) = 1
INNER JOIN Import.Budget B ON B.PKDataInizioMese = D.PKData
    AND B.CapoArea = N'PARTNERUP SRL'
ORDER BY B.PKDataInizioMese;
GO

BEGIN TRANSACTION ;

WITH NewBudgetPartnerUp
AS (
    SELECT
        1 AS Mese,
        70000 AS Budget

    UNION ALL SELECT 2, 70000
    UNION ALL SELECT 3, 70000
    UNION ALL SELECT 4, 70000
    UNION ALL SELECT 5, 50000
    UNION ALL SELECT 6, 50000
    UNION ALL SELECT 7, 50000
    UNION ALL SELECT 8, 50000
    UNION ALL SELECT 9, 80000
    UNION ALL SELECT 10, 80000
    UNION ALL SELECT 11, 80000
    UNION ALL SELECT 12, 80000
)
UPDATE B
SET B.ImportoBudgetNuoveVendite = NBPU.Budget

FROM NewBudgetPartnerUp NBPU
INNER JOIN Dim.Data D ON D.Mese = NBPU.Mese
    AND D.Anno = 2022
    AND DATEPART(DAY, D.PKData) = 1
INNER JOIN Import.Budget B ON B.PKDataInizioMese = D.PKData
    AND B.CapoArea = N'PARTNERUP SRL';

ROLLBACK TRANSACTION
GO

*/

/**
 * @table Import.ProvinciaAgente
 * @description Province con agente unico
*/

--DROP TABLE IF EXISTS Import.ProvinciaAgente;
GO

IF OBJECT_ID('Import.ProvinciaAgente', 'U') IS NULL
BEGIN

    CREATE TABLE Import.ProvinciaAgente (
        IDProvincia NVARCHAR(10) NOT NULL CONSTRAINT PK_Import_ProvinciaAgente PRIMARY KEY CLUSTERED,
        Agente NVARCHAR(60) NOT NULL,
        CapoArea NVARCHAR(60) NOT NULL
    );

END;
GO

/* Verifiche pre importazione

WITH Province
AS (
    SELECT DISTINCT
        IDProvincia,
        Provincia
    FROM Dim.Cliente
)
SELECT
    PA.*
FROM Import.stg_ProvinciaAgente PA
LEFT JOIN Province P ON P.IDProvincia = PA.CodiceProvincia
WHERE P.IDProvincia IS NULL
ORDER BY PA.CodiceProvincia;
GO

WITH Province
AS (
    SELECT DISTINCT
        IDProvincia,
        Provincia
    FROM Dim.Cliente
)
SELECT
    P.*
FROM Import.stg_ProvinciaAgente PA
RIGHT JOIN Province P ON P.IDProvincia = PA.CodiceProvincia
WHERE PA.CodiceProvincia IS NULL
ORDER BY P.IDProvincia;
GO

SELECT
    PA.*
FROM Import.stg_ProvinciaAgente PA
LEFT JOIN Import.CapiArea ICA ON ICA.AgenteBudget = PA.Agente
LEFT JOIN Dim.CapoArea CA ON CA.CapoArea = ICA.CapoArea
WHERE CA.PKCapoArea IS NULL
ORDER BY PA.Agente, PA.CodiceProvincia;
GO

*/

CREATE OR ALTER VIEW Import.ProvinciaAgenteView
AS
WITH AgenteImport
AS (
    SELECT DISTINCT
        PA.Agente,
        CASE WHEN PA.Agente = N'CESI' THEN N'DIREZIONALI' ELSE PA.Agente END AS AgenteTrascodifica

    FROM Import.stg_ProvinciaAgente PA
)
SELECT
    PA.CodiceProvincia AS IDProvincia,
    PA.Agente,
    CA.CapoArea

FROM Import.stg_ProvinciaAgente PA
INNER JOIN AgenteImport AI ON AI.Agente = PA.Agente
INNER JOIN Import.CapiArea ICA ON ICA.AgenteBudget = AI.AgenteTrascodifica
INNER JOIN Dim.CapoArea CA ON CA.CapoArea = ICA.CapoArea;
GO

CREATE OR ALTER PROCEDURE Import.usp_Reload_ProvinciaAgente
AS
BEGIN

    SET NOCOUNT ON;

    BEGIN TRANSACTION;

    BEGIN TRY

        TRUNCATE TABLE Import.ProvinciaAgente;

        INSERT INTO Import.ProvinciaAgente (
            IDProvincia,
            Agente,
            CapoArea
        )
        SELECT
            IDProvincia,
            Agente,
            CapoArea

        FROM Import.ProvinciaAgenteView
        ORDER BY IDProvincia,
            CapoArea;

        COMMIT TRANSACTION;

    END TRY
    BEGIN CATCH

        RAISERROR('Errore nell''importazione di ProvinciaAgente', 16, 1);

        ROLLBACK;

    END CATCH

END;
GO

EXEC Import.usp_Reload_ProvinciaAgente;
GO

/**
 * @table Import.ComuneCAPAgente
 * @description Assegnazione specifica Comune+CAP / CapoArea
*/

--DROP TABLE IF EXISTS Import.ComuneCAPAgente;
GO

IF OBJECT_ID('Import.ComuneCAPAgente', 'U') IS NULL
BEGIN

    CREATE TABLE Import.ComuneCAPAgente (
        IDProvincia NVARCHAR(10) NOT NULL,
        Comune NVARCHAR(60) NOT NULL,
        CAP NVARCHAR(10) NOT NULL,
        Agente NVARCHAR(60) NOT NULL,
        CapoArea NVARCHAR(60) NOT NULL,

        CONSTRAINT PK_Import_ComuneCAPAgente PRIMARY KEY CLUSTERED (IDProvincia, Comune, CAP)
    );

END;
GO

/* Verifiche pre importazione

*/

CREATE OR ALTER VIEW Import.ComuneCAPAgenteView
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

CREATE OR ALTER PROCEDURE Import.usp_Reload_ComuneCAPAgente
AS
BEGIN

    SET NOCOUNT ON;

    BEGIN TRANSACTION;

    BEGIN TRY

        TRUNCATE TABLE Import.ComuneCAPAgente;

        INSERT INTO Import.ComuneCAPAgente (
            IDProvincia,
            Comune,
            CAP,
            Agente,
            CapoArea
        )
        SELECT
            IDProvincia,
            Comune,
            CAP,
            Agente,
            CapoArea

        FROM Import.ComuneCAPAgenteView
        ORDER BY Comune,
            CAP;

        COMMIT TRANSACTION;

    END TRY
    BEGIN CATCH

        RAISERROR('Errore nell''importazione di ComuneCAPAgente', 16, 1);

        ROLLBACK;

    END CATCH

END;
GO

EXEC Import.usp_Reload_ComuneCAPAgente;
GO

/**
 * @table Import.Libero2MacroTipologia
 * @description Decodifica soggetto commerciale
*/

--DROP TABLE IF EXISTS Import.Libero2MacroTipologia;
GO

IF OBJECT_ID('Import.Libero2MacroTipologia', 'U') IS NULL
BEGIN

    CREATE TABLE Import.Libero2MacroTipologia (
        IDLibero2 NVARCHAR(10) NOT NULL CONSTRAINT PK_Import_Libero2MacroTipologia PRIMARY KEY CLUSTERED,
        Libero2 NVARCHAR(60) NOT NULL,
        MacroTipologia NVARCHAR(60) NOT NULL,
        IsValidaPerBudgetNuoveVendite BIT NOT NULL,
        IsValidaPerBudgetRinnovi BIT NOT NULL
    );

    INSERT INTO Import.Libero2MacroTipologia (
        IDLibero2,
        Libero2,
        MacroTipologia,
        IsValidaPerBudgetNuoveVendite,
        IsValidaPerBudgetRinnovi
    )
    VALUES (N'AUTO', N'RINNOVO AUTOMATICO', N'Rinnovo', 0, 1),
        (N'NEW', N'NUOVO', N'Nuova vendita', 1, 0),
        (N'NEW F.P.', N'NUOVO CON FATTURA POSTICIPATA', N'Nuova vendita', 1, 0),
        (N'RIN-AG', N'RINNOVO AGENTE', N'Rinnovo', 0, 1),
        (N'RIN-CON', N'RINNOVO CONCORDATO', N'Rinnovo', 0, 1),
        (N'RIN-DIR', N'RINNOVO DIREZIONALI', N'Rinnovo', 0, 1);

    INSERT INTO Import.Libero2MacroTipologia
    (
        IDLibero2,
        Libero2,
        MacroTipologia,
        IsValidaPerBudgetNuoveVendite,
        IsValidaPerBudgetRinnovi
    )
    VALUES (N'REC', N'RECUPERO', N'Nuova vendita', 1, 0);

END;
GO

/**
 * @table Import.CapoAreaDefault_GruppoAgenti
 * @description Gruppo agenti predefinito per Capo Area default
*/

--DROP TABLE IF EXISTS Import.CapoAreaDefault_GruppoAgenti;
GO

IF OBJECT_ID('Import.CapoAreaDefault_GruppoAgenti', 'U') IS NULL
BEGIN

    WITH GruppoAgentiDettaglio
    AS (
        SELECT
            GA.CapoArea,
            GA.PKGruppoAgenti,
            GA.IDGruppoAgenti,
            GA.GruppoAgenti,
            ROW_NUMBER() OVER (PARTITION BY GA.CapoArea ORDER BY GA.PKGruppoAgenti) AS rn

        FROM Dim.GruppoAgenti GA
    ),
    CapoAreaDefault
    AS (
        SELECT DISTINCT
            C.CapoAreaDefault

        FROM Dim.Cliente C
    )
    SELECT
        CAD.CapoAreaDefault,
        COALESCE(GAF.PKGruppoAgenti, GA.PKGruppoAgenti, GAD.PKGruppoAgenti, -1) AS PKGruppoAgentiDefault,
        GANew.IDGruppoAgenti,
        GANew.GruppoAgenti

    INTO Import.CapoAreaDefault_GruppoAgenti

    FROM CapoAreaDefault CAD
    LEFT JOIN Dim.GruppoAgenti GAF ON GAF.CapoArea = CAD.CapoAreaDefault
        AND GAF.GruppoAgenti = CAD.CapoAreaDefault + N' FATTURATO'
    LEFT JOIN Dim.GruppoAgenti GA ON GA.CapoArea = CAD.CapoAreaDefault
        AND GA.GruppoAgenti = CAD.CapoAreaDefault
    LEFT JOIN GruppoAgentiDettaglio GAD ON GAD.CapoArea = CAD.CapoAreaDefault
        AND GAD.rn = 1
    LEFT JOIN Dim.GruppoAgenti GANew ON GANew.PKGruppoAgenti = COALESCE(GAF.PKGruppoAgenti, GA.PKGruppoAgenti, GAD.PKGruppoAgenti, -1)
    WHERE CAD.CapoAreaDefault <> N'';

    ALTER TABLE Import.CapoAreaDefault_GruppoAgenti ADD CONSTRAINT PK_Import_CapoAreaDefault_GruppoAgenti PRIMARY KEY CLUSTERED (CapoAreaDefault);

    ALTER TABLE Import.CapoAreaDefault_GruppoAgenti ALTER COLUMN PKGruppoAgentiDefault INT NOT NULL;

END;
GO

/**
 * @table Import.CondizioniPagamento
 * @description Dettagli condizioni pagamento e relative impostazioni
*/

-- Typo nel file di importazione
--UPDATE dbo.[CondizioniPagamento$] SET CodiceCondizioniPagamento = N'R10A0' WHERE CodiceCondizioniPagamento = N'R10A10' AND CondizioniPagamento = N'10 rate no anticipo';

--DROP TABLE IF EXISTS Import.CondizioniPagamento;
GO

IF OBJECT_ID('Import.CondizioniPagamento', 'U') IS NULL
BEGIN

    CREATE TABLE Import.CondizioniPagamento (
        CodiceCondizioniPagamento NVARCHAR(10) NOT NULL CONSTRAINT PK_Import_CondizioniPagamento PRIMARY KEY CLUSTERED,
        CondizioniPagamento NVARCHAR(60) NOT NULL,
        TipoContratto NVARCHAR(20) NOT NULL,
        ProvvigioniAgenti NVARCHAR(60) NOT NULL
    );


    INSERT INTO Import.CondizioniPagamento
    (
        CodiceCondizioniPagamento,
        CondizioniPagamento,
        TipoContratto,
        ProvvigioniAgenti
    )
    SELECT
        CP.CodiceCondizioniPagamento,
        CP.CondizioniPagamento,
        CP.TipoContratto,
        CP.ProvvigioniAgenti

    FROM dbo.[CondizioniPagamento$] CP
    ORDER BY CP.CodiceCondizioniPagamento;

END;
GO

/**
 * @table Import.LiquidazioneProvvigioneTeorica
 * @description Liquidazione provvigione teorica in base a durata contratto e condizioni pagamento
*/

--DROP TABLE IF EXISTS Import.LiquidazioneProvvigioneTeorica;
GO

IF OBJECT_ID('Import.LiquidazioneProvvigioneTeorica', 'U') IS NULL
BEGIN

    SELECT
        DurataContratto,
        CodiceCondizioniPagamento,
        LiquidazioneProvvigioneTeorica

    INTO Import.LiquidazioneProvvigioneTeorica

    FROM dbo.[LiquidazioneProvvigioneTeorica$];

    ALTER TABLE Import.LiquidazioneProvvigioneTeorica ALTER COLUMN DurataContratto NVARCHAR(20) NOT NULL;
    ALTER TABLE Import.LiquidazioneProvvigioneTeorica ALTER COLUMN CodiceCondizioniPagamento NVARCHAR(10) NOT NULL;

    ALTER TABLE Import.LiquidazioneProvvigioneTeorica ADD CONSTRAINT PK_Import_LiquidazioneProvvigioneTeorica PRIMARY KEY CLUSTERED (DurataContratto, CodiceCondizioniPagamento);

    ALTER TABLE Import.LiquidazioneProvvigioneTeorica ALTER COLUMN LiquidazioneProvvigioneTeorica NVARCHAR(40) NOT NULL;

END;
GO

/**
 * @table Import.ArticoloCategoria
 * @description
*/

--DROP TABLE IF EXISTS Import.ArticoloCategoria;
GO

IF OBJECT_ID('Import.ArticoloCategoria', 'U') IS NULL
BEGIN

    SELECT
        AC.id_articolo,
        AC.Codice,
        AC.Descrizione,
        AC.CategoriaMaster,
        AC.CodiceEsercizioMaster,
        1.0 AS Percentuale,
        1 AS Origine -- 1: File Articoli 2023 dell'8/11/2023

    INTO Import.ArticoloCategoria

    FROM CesiDW_misc.dbo.[ArticoloCategoria$] AC;

    INSERT INTO Import.ArticoloCategoria
    SELECT
        ACM.id_articolo,
        ACM.Codice,
        ACM.Descrizione,
        ACM.CategoriaMaster,
        ACM.CodiceEsercizioMaster,
        ACM.Percentuale,
        2 AS Origine -- 2: Criterio utilizzato fino all'8/11/2023

    FROM Import.ArticoloCategoriaMaster ACM
    LEFT JOIN Import.ArticoloCategoria AC ON AC.id_articolo = ACM.id_articolo
    WHERE AC.id_articolo IS NULL;

END;
GO

/**
 * @table Import.InvioReportCrediti
 * @description 
*/

--DROP TABLE IF EXISTS Import.InvioReportCrediti;
GO

IF OBJECT_ID('Import.InvioReportCrediti', 'U') IS NULL
BEGIN

    CREATE TABLE Import.InvioReportCrediti (
        Email NVARCHAR(40) NOT NULL CONSTRAINT PK_Import_InvioReportCrediti PRIMARY KEY CLUSTERED,
        Descrizione NVARCHAR(40) NULL,
        Note NVARCHAR(400) NULL
    );

    -- Email di Andrea del 19/6/2024
    INSERT INTO Import.InvioReportCrediti (
        Descrizione,
        Email,
        Note
    )
    VALUES (N'Acquaroni Paolo', N'paolo.acquaroni@studioacquaroni.it', N'iscritto al Master Plus e alla Revisione'),
        (N'Fornari Cinzia', N'fornaricinzia@hotmail.com', N'Iscritta al Master Plus ma non è Revisore'),
        --(N'La Torre Giuseppe', N'latorregiuseppe22@gmail.com', N'iscritto al Master Plus e alla Revisione'),
        (N'Elabro di Novara', N'daniela@elabro.it', N'seguono di due la Formazione anche se solo 1 inclusa nell’abbonamento  e l’altra a pagamento'),
        (N'Elabro di Novara', N'carola@elabro.it', N'seguono di due la Formazione anche se solo 1 inclusa nell’abbonamento  e l’altra a pagamento'),
        (N'Studio Mereghetti', N'info@studio-mereghetti.it', N'iscritta al Master e al Master Plus  ha 2 formazioni nell’abbonamento');
 
END;
GO

/**
 * @table Import.ProvinciaAgenteCapoArea
 * @description 
*/

--DROP TABLE IF EXISTS Import.ProvinciaAgenteCapoArea;
GO

IF OBJECT_ID('Import.ProvinciaAgenteCapoArea', 'U') IS NULL
BEGIN

    CREATE TABLE Import.ProvinciaAgenteCapoArea (
        IDProvincia NVARCHAR(10) NOT NULL CONSTRAINT PK_Import_ProvinciaAgenteCapoArea PRIMARY KEY CLUSTERED,
        Agente      NVARCHAR(60) NOT NULL,
        CapoArea    NVARCHAR(60) NOT NULL
    );

END;
GO
