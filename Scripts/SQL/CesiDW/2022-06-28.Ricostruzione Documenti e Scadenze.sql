USE CesiDW;
GO

/*
SET NOEXEC OFF;
--*/ SET NOEXEC ON;
GO

/*
SELECT * INTO CesiDW_misc.backups.Fact_Documenti_20220628 FROM Fact.Documenti;
GO
*/

TRUNCATE TABLE Landing.COMETA_CondizioniPagamento;
TRUNCATE TABLE Landing.COMETA_Documento;
TRUNCATE TABLE Landing.COMETA_Documento_Riga;
TRUNCATE TABLE Landing.COMETA_Esercizio;
TRUNCATE TABLE Landing.COMETA_Libero_1;
TRUNCATE TABLE Landing.COMETA_Libero_2;
TRUNCATE TABLE Landing.COMETA_Libero_3;
TRUNCATE TABLE Landing.COMETA_MySolutionContracts;
TRUNCATE TABLE Landing.COMETA_Profilo_Documento;
TRUNCATE TABLE Landing.COMETA_Registro;
TRUNCATE TABLE Landing.COMETA_Tipo_Fatturazione;
GO

EXEC COMETA.usp_Merge_CondizioniPagamento;
EXEC COMETA.usp_Merge_Documento;
EXEC COMETA.usp_Merge_Documento_Riga;
EXEC COMETA.usp_Merge_Esercizio;
EXEC COMETA.usp_Merge_Libero_1;
EXEC COMETA.usp_Merge_Libero_2;
EXEC COMETA.usp_Merge_Libero_3;
EXEC COMETA.usp_Merge_MySolutionContracts;
EXEC COMETA.usp_Merge_Profilo_Documento;
EXEC COMETA.usp_Merge_Registro;
EXEC COMETA.usp_Merge_Tipo_Fatturazione;
GO

SELECT COUNT(1) FROM Landing.COMETA_CondizioniPagamento;
SELECT COUNT(1) FROM Landing.COMETA_Documento;
SELECT COUNT(1) FROM Landing.COMETA_Documento_Riga;
SELECT COUNT(1) FROM Landing.COMETA_Esercizio;
SELECT COUNT(1) FROM Landing.COMETA_Libero_1;
SELECT COUNT(1) FROM Landing.COMETA_Libero_2;
SELECT COUNT(1) FROM Landing.COMETA_Libero_3;
SELECT COUNT(1) FROM Landing.COMETA_MySolutionContracts;
SELECT COUNT(1) FROM Landing.COMETA_Profilo_Documento;
SELECT COUNT(1) FROM Landing.COMETA_Registro;
SELECT COUNT(1) FROM Landing.COMETA_Tipo_Fatturazione;
GO

/*
    SCHEMA_NAME > COMETA
    TABLE_NAME > Documento_Riga
    STAGING_TABLE_NAME > Documenti
*/

/**
 * @table Staging.Documenti
 * @description

 * @depends Landing.COMETA_Documento_Riga

SELECT TOP 1 * FROM Landing.COMETA_Documento_Riga;
*/

DROP TABLE IF EXISTS Staging.Documenti; DELETE FROM audit.tables WHERE provider_name = N'MyDatamartReporting' AND full_table_name = N'Landing.COMETA_Documento_Riga';
GO

IF NOT EXISTS (SELECT 1 FROM audit.tables WHERE provider_name = N'MyDatamartReporting' AND full_table_name = N'Landing.COMETA_Documento_Riga')
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
        N'Landing.COMETA_Documento_Riga',      -- full_table_name - sysname
        N'Staging.Documenti',      -- staging_table_name - sysname
        N'Fact.Documenti',      -- datawarehouse_table_name - sysname
        NULL, -- lastupdated_staging - datetime
        NULL  -- lastupdated_local - datetime
    );

END;
GO

IF OBJECT_ID(N'Staging.DocumentiView', N'V') IS NULL EXEC('CREATE VIEW Staging.DocumentiView AS SELECT 1 AS fld;');
GO

ALTER VIEW Staging.DocumentiView
AS
WITH TableData
AS (
    SELECT
        DR.id_riga_documento AS IDDocumento_Riga,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            DR.id_riga_documento,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            D.id_documento,
            --D.id_prof_documento,
            PD.codice,
            PD.descrizione,
            --D.id_registro,
            R.tipo_registro,
            R.numero,
            R.descrizione,
            --R.id_esercizio,
            E.codice,
            DIE.PKData,
            DFE.PKData,
            DReg.PKData,
            D.num_documento,
            DD.PKData,
            DIC.PKData,
            DC.PKData,
            --D.id_sog_commerciale,
            ----DSC.tipo,
            ----DSC.descr_sog_com,
            C.TipoSoggettoCommerciale,
            --A.id_anagrafica,
            C.PKCliente,
            --D.id_sog_commerciale_fattura,
            ----DSCF.tipo,
            ----DSCF.descr_sog_com,
            CF.TipoSoggettoCommerciale,
            --AF.id_anagrafica,
            CF.PKCliente,
            --D.id_gruppo_agenti,
            GA.PKGruppoAgenti,
            GA.PKCapoArea,
            --D.data_fine_contratto,
            DFC.PKData,
            D.libero_4,
            --D.data_inizio_contratto,
            DIC.PKData,
            --D.id_libero_1,
            L1.codice,
            L1.descrizione,
            --D.id_libero_2,
            L2.codice,
            L2.descrizione,
            --D.id_libero_3,
            L3.codice,
            L3.descrizione,
            --D.id_tipo_fatturazione,
            TF.codice,
            TF.descrizione,
            GAR.PKGruppoAgenti,
            GAR.PKCapoArea,
            DR.num_riga,
            --DR.id_articolo,
            ART.PKArticolo,
            --DR.descrizione,
            DR.totale_riga,
            --D.id_con_pagamento,
            CP.codice,
            CP.descrizione,
            D.rinnovo_automatico,
            D.note_intestazione,
            IPD.IsProfiloValidoPerStatisticaFatturato,
            IPD.IsProfiloValidoPerStatisticaFatturatoFormazione,
            MT.PKMacroTipologia,
            DR.provv_calcolata_carea,
            DR.provv_calcolata_agente,
            DR.provv_calcolata_subagente,
            MSC.num_progressivo,
			MSC.Quote,
            DR.id_riga_doc_provenienza,
            D.note_decisionali,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,

        -- COMETA_Documento
        D.id_documento AS IDDocumento,
        --D.id_prof_documento,
        PD.codice AS IDProfilo,
        PD.descrizione AS Profilo,
        --D.id_registro,
        COALESCE(R.tipo_registro, CASE WHEN D.id_registro IS NULL THEN N'' ELSE N'???' END) AS TipoRegistro,
        COALESCE(R.numero, CASE WHEN D.id_registro IS NULL THEN -1 ELSE -101 END) AS NumeroRegistro,
        COALESCE(R.descrizione, CASE WHEN D.id_registro IS NULL THEN N'' ELSE N'<???>' END) AS Registro,
        --R.id_esercizio,
        COALESCE(E.codice, CASE WHEN R.id_esercizio IS NULL THEN N'' ELSE N'???' END) AS CodiceEsercizio,
        COALESCE(DIE.PKData, CAST('19000101' AS DATE)) AS PKDataInizioEsercizio,
        COALESCE(DFE.PKData, CAST('19000101' AS DATE)) AS PKDataFineEsercizio,
        --D.data_registrazione,
        COALESCE(DReg.PKData, CAST('19000101' AS DATE)) AS PKDataRegistrazione,
        COALESCE(D.num_documento, N'') AS NumeroDocumento,
        --D.data_documento,
        COALESCE(DD.PKData, DIC.PKData, CAST('19000101' AS DATE)) AS PKDataDocumento,
        --D.data_competenza,
        COALESCE(DC.PKData, CAST('19000101' AS DATE)) AS PKDataCompetenza,
        --D.id_sog_commerciale,
        ----DSC.tipo AS IDTipoSoggettoCommerciale,
        ----DSC.descr_sog_com AS TipoSoggettoCommerciale,
        C.TipoSoggettoCommerciale,
        --A.id_anagrafica,
        C.PKCliente,
        --D.id_sog_commerciale_fattura,
        ----DSCF.tipo AS IDTipoSoggettoCommercialeFattura,
        ----DSCF.descr_sog_com AS TipoSoggettoCommercialeFattura,
        CF.TipoSoggettoCommerciale AS TipoSoggettoCommercialeFattura,
        --AF.id_anagrafica,
        CF.PKCliente AS PKClienteFattura,
        --D.id_gruppo_agenti,
        COALESCE(GA.PKGruppoAgenti, CASE WHEN D.id_gruppo_agenti IS NULL THEN -1 ELSE -101 END) AS PKGruppoAgenti,
        COALESCE(GA.PKCapoArea, CASE WHEN D.id_gruppo_agenti IS NULL THEN -1 ELSE -101 END) AS PKCapoArea,
        --D.data_fine_contratto,
        COALESCE(DFC.PKData, CAST('19000101' AS DATE)) AS PKDataFineContratto,
        COALESCE(D.libero_4, N'') AS Libero4,
        --D.data_inizio_contratto,
        COALESCE(DIC.PKData, CAST('19000101' AS DATE)) AS PKDataInizioContratto,
        --D.id_libero_1,
        COALESCE(L1.codice, CASE WHEN D.id_libero_1 IS NULL THEN N'' ELSE N'???' END) AS IDLibero1,
        COALESCE(L1.descrizione, CASE WHEN D.id_libero_1 IS NULL THEN N'' ELSE N'<???>' END) AS Libero1,
        --D.id_libero_2,
        COALESCE(L2.codice, CASE WHEN D.id_libero_2 IS NULL THEN N'' ELSE N'???' END) AS IDLibero2,
        COALESCE(L2.descrizione, CASE WHEN D.id_libero_2 IS NULL THEN N'' ELSE N'<???>' END) AS Libero2,
        --D.id_libero_3,
        COALESCE(L3.codice, CASE WHEN D.id_libero_3 IS NULL THEN N'' ELSE N'???' END) AS IDLibero3,
        COALESCE(L3.descrizione, CASE WHEN D.id_libero_3 IS NULL THEN N'' ELSE N'<???>' END) AS Libero3,
        --D.id_tipo_fatturazione,
        COALESCE(TF.codice, CASE WHEN D.id_tipo_fatturazione IS NULL THEN N'' ELSE N'???' END) AS IDTipoFatturazione,
        COALESCE(TF.descrizione, CASE WHEN D.id_tipo_fatturazione IS NULL THEN N'' ELSE N'<???>' END) AS TipoFatturazione,

        -- COMETA_Documento_Riga
        --DR.id_gruppo_agenti,
        COALESCE(GAR.PKGruppoAgenti, CASE WHEN DR.id_gruppo_agenti IS NULL THEN -1 ELSE -101 END) AS PKGruppoAgenti_Riga,
        COALESCE(GAR.PKCapoArea, CASE WHEN DR.id_gruppo_agenti IS NULL THEN -1 ELSE -101 END) AS PKCapoArea_Riga,
        DR.num_riga AS NumeroRiga,
        --DR.id_articolo,
        COALESCE(ART.PKArticolo, CASE WHEN COALESCE(DR.id_articolo, 0) = 0 THEN -1 ELSE -101 END) AS PKArticolo,
        --DR.descrizione,
        COALESCE(DR.totale_riga, 0.0) AS ImportoTotale,

        D.id_con_pagamento,
        COALESCE(CP.codice, N'') AS CodiceCondizioniPagamento,
        COALESCE(CP.descrizione, N'') AS CondizioniPagamento,
        COALESCE(D.rinnovo_automatico, '') AS RinnovoAutomatico,
        COALESCE(D.note_intestazione, N'') AS NoteIntestazione,
        COALESCE(IPD.IsProfiloValidoPerStatisticaFatturato, 0) AS IsProfiloValidoPerStatisticaFatturato,
        COALESCE(IPD.IsProfiloValidoPerStatisticaFatturatoFormazione, 0) AS IsProfiloValidoPerStatisticaFatturatoFormazione,
        COALESCE(MT.PKMacroTipologia, -1) AS PKMacroTipologia,

        ROW_NUMBER() OVER (PARTITION BY DR.id_riga_documento ORDER BY D.id_documento) AS rn,

        COALESCE(DR.provv_calcolata_carea, 0.0) AS ImportoProvvigioneCapoArea,
        COALESCE(DR.provv_calcolata_agente, 0.0) AS ImportoProvvigioneAgente,
        COALESCE(DR.provv_calcolata_subagente, 0.0) AS ImportoProvvigioneSubagente,
        COALESCE(MSC.num_progressivo, 0) AS Progressivo,
		COALESCE(MSC.Quote, 0) AS Quote,
        DR.id_riga_doc_provenienza AS IDDocumento_Riga_Provenienza,
        CASE WHEN D.IsDeleted = CAST(0 AS BIT) AND DR.IsDeleted = CAST(0 AS BIT) THEN 0 ELSE 1 END AS IsDeleted,
        COALESCE(D.note_decisionali, N'') AS NoteDecisionali

    FROM Landing.COMETA_Documento_Riga DR
    INNER JOIN Landing.COMETA_Documento D ON D.id_documento = DR.id_documento
    INNER JOIN Landing.COMETA_Profilo_Documento PD ON PD.id_prof_documento = D.id_prof_documento
    LEFT JOIN Landing.COMETA_Registro R ON R.id_registro = D.id_registro
    LEFT JOIN Landing.COMETA_Esercizio E ON E.id_esercizio = R.id_esercizio
    LEFT JOIN Dim.Data DIE ON DIE.PKData = E.data_inizio
    LEFT JOIN Dim.Data DFE ON DFE.PKData = E.data_fine
    LEFT JOIN Dim.Data DReg ON DReg.PKData = D.data_registrazione
    LEFT JOIN Dim.Data DD ON DD.PKData = D.data_documento
    LEFT JOIN Dim.Data DC ON DC.PKData = D.data_competenza
    INNER JOIN Dim.Cliente C ON C.IDSoggettoCommerciale = D.id_sog_commerciale
    INNER JOIN Dim.Cliente CF ON CF.IDSoggettoCommerciale = D.id_sog_commerciale
    LEFT JOIN Dim.GruppoAgenti GA ON GA.id_gruppo_agenti = D.id_gruppo_agenti
    LEFT JOIN Dim.Data DFC ON DFC.PKData = D.data_fine_contratto
    LEFT JOIN Dim.Data DIC ON DIC.PKData = D.data_inizio_contratto
    LEFT JOIN Landing.COMETA_Libero_1 L1 ON L1.id_libero_1 = D.id_libero_1
    LEFT JOIN Landing.COMETA_Libero_2 L2 ON L2.id_libero_2 = D.id_libero_2
    LEFT JOIN Landing.COMETA_Libero_3 L3 ON L3.id_libero_3 = D.id_libero_3
    LEFT JOIN Landing.COMETA_Tipo_Fatturazione TF ON TF.id_tipo_fatturazione = D.id_tipo_fatturazione
    LEFT JOIN Dim.GruppoAgenti GAR ON GAR.id_gruppo_agenti = DR.id_gruppo_agenti
    LEFT JOIN Dim.Articolo ART ON ART.id_articolo = DR.id_articolo
    LEFT JOIN Landing.COMETA_CondizioniPagamento CP ON CP.id_con_pagamento = D.id_con_pagamento
    LEFT JOIN Import.ProfiliDocumento IPD ON IPD.id_prof_documento = D.id_prof_documento
    LEFT JOIN Import.Libero2MacroTipologia L2MT ON L2MT.IDLibero2 = L2.codice
    LEFT JOIN Dim.MacroTipologia MT ON MT.MacroTipologia = L2MT.MacroTipologia
    LEFT JOIN Landing.COMETA_MySolutionContracts MSC ON MSC.id_riga_documento = DR.id_riga_documento

)
SELECT
    -- Chiavi
    TD.IDDocumento_Riga,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    --CAST(0 AS BIT) AS IsDeleted,
    TD.IsDeleted,

    -- Attributi
    TD.IDDocumento,
    TD.IDProfilo,
    TD.Profilo,
    TD.TipoRegistro,
    TD.NumeroRegistro,
    TD.Registro,
    TD.CodiceEsercizio,
    TD.PKDataInizioEsercizio,
    TD.PKDataFineEsercizio,
    TD.PKDataRegistrazione,
    TD.NumeroDocumento,
    TD.PKDataDocumento,
    TD.PKDataCompetenza,
    TD.TipoSoggettoCommerciale,
    TD.PKCliente,
    TD.TipoSoggettoCommercialeFattura,
    TD.PKClienteFattura,
    TD.PKGruppoAgenti,
    TD.PKCapoArea,
    TD.PKDataFineContratto,
    TD.Libero4,
    TD.PKDataInizioContratto,
    TD.IDLibero1,
    TD.Libero1,
    TD.IDLibero2,
    TD.Libero2,
    TD.IDLibero3,
    TD.Libero3,
    TD.IDTipoFatturazione,
    TD.TipoFatturazione,
    TD.PKGruppoAgenti_Riga,
    TD.PKCapoArea_Riga,
    TD.NumeroRiga,
    TD.PKArticolo,
    TD.CodiceCondizioniPagamento,
    TD.CondizioniPagamento,
    TD.RinnovoAutomatico,
    TD.NoteIntestazione,
    TD.IsProfiloValidoPerStatisticaFatturato,
    TD.IsProfiloValidoPerStatisticaFatturatoFormazione,
    TD.PKMacroTipologia,

    -- Misure
    TD.ImportoTotale,
    TD.ImportoProvvigioneCapoArea,
    TD.ImportoProvvigioneAgente,
    TD.ImportoProvvigioneSubagente,

    TD.Progressivo,
	TD.Quote,
    TD.IDDocumento_Riga_Provenienza,
    TD.NoteDecisionali

FROM TableData TD
WHERE TD.rn = 1;
GO

IF OBJECT_ID(N'Staging.Documenti', N'U') IS NOT NULL DROP TABLE Staging.Documenti; UPDATE audit.tables SET lastupdated_staging = NULL, lastupdated_local = NULL WHERE provider_name = N'MyDatamartReporting' AND full_table_name = N'Landing.COMETA_Documento_Riga';
GO

IF OBJECT_ID(N'Staging.Documenti', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Staging.Documenti FROM Staging.DocumentiView;

    ALTER TABLE Staging.Documenti ADD CONSTRAINT PK_Staging_Documenti PRIMARY KEY CLUSTERED (UpdateDatetime, IDDocumento_Riga);

    ALTER TABLE Staging.Documenti ALTER COLUMN IDProfilo NVARCHAR(10) NOT NULL;
    ALTER TABLE Staging.Documenti ALTER COLUMN Profilo NVARCHAR(60) NOT NULL;
    ALTER TABLE Staging.Documenti ALTER COLUMN TipoRegistro CHAR(2) NOT NULL;
    ALTER TABLE Staging.Documenti ALTER COLUMN NumeroRegistro INT NOT NULL;
    ALTER TABLE Staging.Documenti ALTER COLUMN Registro NVARCHAR(60) NOT NULL;
    ALTER TABLE Staging.Documenti ALTER COLUMN CodiceEsercizio CHAR(4) NOT NULL;
    ALTER TABLE Staging.Documenti ALTER COLUMN PKDataInizioEsercizio DATE NOT NULL;
    ALTER TABLE Staging.Documenti ALTER COLUMN PKDataFineEsercizio DATE NOT NULL;
    ALTER TABLE Staging.Documenti ALTER COLUMN PKDataRegistrazione DATE NOT NULL;
    ALTER TABLE Staging.Documenti ALTER COLUMN NumeroDocumento NVARCHAR(20) NOT NULL;
    ALTER TABLE Staging.Documenti ALTER COLUMN PKDataDocumento DATE NOT NULL;
    ALTER TABLE Staging.Documenti ALTER COLUMN PKDataCompetenza DATE NOT NULL;
    ALTER TABLE Staging.Documenti ALTER COLUMN PKCliente INT NOT NULL;
    ALTER TABLE Staging.Documenti ALTER COLUMN PKClienteFattura INT NOT NULL;
    ALTER TABLE Staging.Documenti ALTER COLUMN PKGruppoAgenti INT NOT NULL;
    ALTER TABLE Staging.Documenti ALTER COLUMN PKCapoArea INT NOT NULL;
    ALTER TABLE Staging.Documenti ALTER COLUMN PKDataFineContratto DATE NOT NULL;
    ALTER TABLE Staging.Documenti ALTER COLUMN Libero4 NVARCHAR(200) NOT NULL;
    ALTER TABLE Staging.Documenti ALTER COLUMN PKDataInizioContratto DATE NOT NULL;
    ALTER TABLE Staging.Documenti ALTER COLUMN IDLibero1 NVARCHAR(10) NOT NULL;
    ALTER TABLE Staging.Documenti ALTER COLUMN Libero1 NVARCHAR(60) NOT NULL;
    ALTER TABLE Staging.Documenti ALTER COLUMN IDLibero2 NVARCHAR(10) NOT NULL;
    ALTER TABLE Staging.Documenti ALTER COLUMN Libero2 NVARCHAR(60) NOT NULL;
    ALTER TABLE Staging.Documenti ALTER COLUMN IDLibero3 NVARCHAR(10) NOT NULL;
    ALTER TABLE Staging.Documenti ALTER COLUMN Libero3 NVARCHAR(60) NOT NULL;
    ALTER TABLE Staging.Documenti ALTER COLUMN IDTipoFatturazione NVARCHAR(10) NOT NULL;
    ALTER TABLE Staging.Documenti ALTER COLUMN TipoFatturazione NVARCHAR(60) NOT NULL;
    ALTER TABLE Staging.Documenti ALTER COLUMN PKGruppoAgenti_Riga INT NOT NULL;
    ALTER TABLE Staging.Documenti ALTER COLUMN PKCapoArea_Riga INT NOT NULL;
    ALTER TABLE Staging.Documenti ALTER COLUMN NumeroRiga INT NOT NULL;
    ALTER TABLE Staging.Documenti ALTER COLUMN PKArticolo INT NOT NULL;
    ALTER TABLE Staging.Documenti ALTER COLUMN CodiceCondizioniPagamento NVARCHAR(10) NULL;
    ALTER TABLE Staging.Documenti ALTER COLUMN CondizioniPagamento NVARCHAR(60) NULL;
    ALTER TABLE Staging.Documenti ALTER COLUMN RinnovoAutomatico CHAR(1) NOT NULL;
    ALTER TABLE Staging.Documenti ALTER COLUMN NoteIntestazione NVARCHAR(1000) NOT NULL;
    ALTER TABLE Staging.Documenti ALTER COLUMN IsProfiloValidoPerStatisticaFatturato BIT NOT NULL;
    ALTER TABLE Staging.Documenti ALTER COLUMN IsProfiloValidoPerStatisticaFatturatoFormazione BIT NOT NULL;
    ALTER TABLE Staging.Documenti ALTER COLUMN PKMacroTipologia INT NOT NULL;
    ALTER TABLE Staging.Documenti ALTER COLUMN ImportoTotale DECIMAL(10, 2) NOT NULL;
    ALTER TABLE Staging.Documenti ALTER COLUMN ImportoProvvigioneCapoArea DECIMAL(10, 2) NOT NULL;
    ALTER TABLE Staging.Documenti ALTER COLUMN ImportoProvvigioneAgente DECIMAL(10, 2) NOT NULL;
    ALTER TABLE Staging.Documenti ALTER COLUMN ImportoProvvigioneSubagente DECIMAL(10, 2) NOT NULL;
    ALTER TABLE Staging.Documenti ALTER COLUMN Progressivo INT NOT NULL;
    ALTER TABLE Staging.Documenti ALTER COLUMN Quote INT NOT NULL;
    ALTER TABLE Staging.Documenti ALTER COLUMN NoteDecisionali NVARCHAR(1000) NOT NULL;

    CREATE UNIQUE NONCLUSTERED INDEX IX_Staging_Documenti_BusinessKey ON Staging.Documenti (IDDocumento_Riga);
END;
GO

IF OBJECT_ID(N'Staging.usp_Reload_Documenti', N'P') IS NULL EXEC('CREATE PROCEDURE Staging.usp_Reload_Documenti AS RETURN 0;');
GO

ALTER PROCEDURE Staging.usp_Reload_Documenti
AS
BEGIN

    SET NOCOUNT ON;

    DECLARE @lastupdated_staging DATETIME;
    DECLARE @provider_name NVARCHAR(60) = N'MyDatamartReporting';
    DECLARE @full_table_name sysname = N'Landing.COMETA_Documento_Riga';

    SELECT TOP 1 @lastupdated_staging = lastupdated_staging
    FROM audit.tables
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    IF (@lastupdated_staging IS NULL) SET @lastupdated_staging = CAST('19000101' AS DATETIME);

    BEGIN TRANSACTION

    TRUNCATE TABLE Staging.Documenti;

    INSERT INTO Staging.Documenti
    SELECT * FROM Staging.DocumentiView
    WHERE UpdateDatetime > @lastupdated_staging;

    SELECT @lastupdated_staging = MAX(UpdateDatetime) FROM Staging.Documenti;

    IF (@lastupdated_staging IS NOT NULL)
    BEGIN

    UPDATE audit.tables
    SET lastupdated_staging = @lastupdated_staging
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    END;

    COMMIT

    UPDATE D
    SET D.PKGruppoAgenti = CADGA.PKGruppoAgentiDefault

    FROM Staging.Documenti D
    INNER JOIN Dim.Cliente C ON C.PKCliente = D.PKClienteFattura
    INNER JOIN Import.CapoAreaDefault_GruppoAgenti CADGA ON CADGA.CapoAreaDefault = C.CapoAreaDefault
    WHERE D.Profilo LIKE N'FATTURA%'
        AND D.PKGruppoAgenti < 0;

END;
GO

EXEC Staging.usp_Reload_Documenti;
GO

DROP TABLE IF EXISTS Fact.Scadenze; DROP SEQUENCE IF EXISTS dbo.seq_Fact_Scadenze; DROP TABLE IF EXISTS Fact.Documenti; DROP SEQUENCE IF EXISTS dbo.seq_Fact_Documenti;
GO

IF OBJECT_ID('dbo.seq_Fact_Documenti', 'SO') IS NULL
BEGIN

    CREATE SEQUENCE dbo.seq_Fact_Documenti START WITH 1;

END;
GO

IF OBJECT_ID('Fact.Documenti', 'U') IS NULL
BEGIN

    CREATE TABLE Fact.Documenti (
        PKDocumenti INT NOT NULL CONSTRAINT PK_Fact_Documenti PRIMARY KEY CLUSTERED CONSTRAINT DFT_Fact_Documenti_PKDocumenti DEFAULT (NEXT VALUE FOR dbo.seq_Fact_Documenti),

        PKDataInizioEsercizio DATE NOT NULL CONSTRAINT FK_Fact_Documenti_PKDataInizioEsercizio FOREIGN KEY REFERENCES Dim.Data (PKData),
        PKDataFineEsercizio DATE NOT NULL CONSTRAINT FK_Fact_Documenti_PKDataFineEsercizio FOREIGN KEY REFERENCES Dim.Data (PKData),
        PKDataRegistrazione DATE NOT NULL CONSTRAINT FK_Fact_Documenti_PKDataRegistrazione FOREIGN KEY REFERENCES Dim.Data (PKData),
        PKDataDocumento DATE NOT NULL CONSTRAINT FK_Fact_Documenti_PKDataDocumento FOREIGN KEY REFERENCES Dim.Data (PKData),
        PKDataCompetenza DATE NOT NULL CONSTRAINT FK_Fact_Documenti_PKDataCompetenza FOREIGN KEY REFERENCES Dim.Data (PKData),
        PKCliente INT NOT NULL CONSTRAINT FK_Fact_Documenti_PKCliente FOREIGN KEY REFERENCES Dim.Cliente (PKCliente),
        PKClienteFattura INT NOT NULL CONSTRAINT FK_Fact_Documenti_PKClienteFattura FOREIGN KEY REFERENCES Dim.Cliente (PKCliente),
        PKGruppoAgenti INT NOT NULL CONSTRAINT FK_Fact_Documenti_PKGruppoAgenti FOREIGN KEY REFERENCES Dim.GruppoAgenti (PKGruppoAgenti),
        PKCapoArea INT NOT NULL CONSTRAINT FK_Fact_Documenti_PKCapoArea FOREIGN KEY REFERENCES Dim.CapoArea (PKCapoArea),
        PKDataFineContratto DATE NOT NULL CONSTRAINT FK_Fact_Documenti_PKDataFineContratto FOREIGN KEY REFERENCES Dim.Data (PKData),
        PKDataInizioContratto DATE NOT NULL CONSTRAINT FK_Fact_Documenti_PKDataInizioContratto FOREIGN KEY REFERENCES Dim.Data (PKData),
        PKGruppoAgenti_Riga INT NOT NULL CONSTRAINT FK_Fact_Documenti_PKGruppoAgenti_Riga FOREIGN KEY REFERENCES Dim.GruppoAgenti (PKGruppoAgenti),
        PKCapoArea_Riga INT NOT NULL CONSTRAINT FK_Fact_Documenti_PKCapoArea_Riga FOREIGN KEY REFERENCES Dim.CapoArea (PKCapoArea),
        PKArticolo INT NOT NULL CONSTRAINT FK_Fact_Documenti_PKArticolo FOREIGN KEY REFERENCES Dim.Articolo (PKArticolo),
        PKMacroTipologia INT NOT NULL CONSTRAINT FK_Fact_Documenti_PKMacroTipologia FOREIGN KEY REFERENCES Dim.MacroTipologia (PKMacroTipologia),

	    HistoricalHashKey VARBINARY(20) NULL,
	    ChangeHashKey VARBINARY(20) NULL,
	    HistoricalHashKeyASCII VARCHAR(34) NULL,
	    ChangeHashKeyASCII VARCHAR(34) NULL,
	    InsertDatetime DATETIME NOT NULL,
	    UpdateDatetime DATETIME NOT NULL,
	    IsDeleted BIT NOT NULL,

    	IDDocumento_Riga INT NOT NULL,
	    IDDocumento INT NOT NULL,
        IDProfilo NVARCHAR(10) NOT NULL,
        Profilo NVARCHAR(60) NOT NULL,
        TipoRegistro CHAR(2) NOT NULL,
        NumeroRegistro INT NOT NULL,
        Registro NVARCHAR(60) NOT NULL,
        CodiceEsercizio CHAR(4) NOT NULL,
	    NumeroDocumento NVARCHAR(20) NOT NULL,
        TipoSoggettoCommerciale NVARCHAR(20) NOT NULL,
        TipoSoggettoCommercialeFattura NVARCHAR(20) NOT NULL,
        Libero4 NVARCHAR(200) NOT NULL,
        IDLibero1 NVARCHAR(10) NOT NULL,
        Libero1 NVARCHAR(60) NOT NULL,
        IDLibero2 NVARCHAR(10) NOT NULL,
        Libero2 NVARCHAR(60) NOT NULL,
        IDLibero3 NVARCHAR(10) NOT NULL,
        Libero3 NVARCHAR(60) NOT NULL,
        IDTipoFatturazione NVARCHAR(10) NOT NULL,
        TipoFatturazione NVARCHAR(60) NOT NULL,
	    NumeroRiga INT NOT NULL,
        CodiceCondizioniPagamento NVARCHAR(10) NOT NULL,
        CondizioniPagamento NVARCHAR(60) NOT NULL,
        RinnovoAutomatico CHAR(1) NOT NULL,
        NoteIntestazione NVARCHAR(1000) NOT NULL,
        IsProfiloValidoPerStatisticaFatturato BIT NOT NULL,
        IsProfiloValidoPerStatisticaFatturatoFormazione BIT NOT NULL,

	    ImportoTotale DECIMAL(10, 2) NOT NULL,
        ImportoProvvigioneCapoArea DECIMAL(10, 2) NOT NULL,
        ImportoProvvigioneAgente DECIMAL(10, 2) NOT NULL,
        ImportoProvvigioneSubagente DECIMAL(10, 2) NOT NULL,
        Progressivo INT NOT NULL,
		Quote INT NOT NULL,
        IDDocumento_Riga_Provenienza INT NULL,
        NoteDecisionali NVARCHAR(1000) NOT NULL
    );

    CREATE UNIQUE NONCLUSTERED INDEX IX_Fact_Documenti_IDDocumento_Riga ON Fact.Documenti (IDDocumento_Riga);

    ALTER SEQUENCE dbo.seq_Fact_Documenti RESTART WITH 1;

END;
GO

IF OBJECT_ID(N'Fact.usp_Merge_Documenti', N'P') IS NULL EXEC('CREATE PROCEDURE Fact.usp_Merge_Documenti AS RETURN 0;');
GO

ALTER PROCEDURE Fact.usp_Merge_Documenti
AS
BEGIN

    SET NOCOUNT ON;

    BEGIN TRANSACTION 

    DECLARE @provider_name NVARCHAR(60) = N'MyDatamartReporting';
    DECLARE @full_table_name sysname = N'Landing.COMETA_Documento_Riga';

    MERGE INTO Fact.Documenti AS TGT
    USING Staging.Documenti (nolock) AS SRC
    ON SRC.IDDocumento_Riga = TGT.IDDocumento_Riga

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.IsDeleted = SRC.IsDeleted,
        
        TGT.PKDataInizioEsercizio = SRC.PKDataInizioEsercizio,
        TGT.PKDataFineEsercizio = SRC.PKDataFineEsercizio,
        TGT.PKDataRegistrazione = SRC.PKDataRegistrazione,
        TGT.PKDataDocumento = SRC.PKDataDocumento,
        TGT.PKDataCompetenza = SRC.PKDataCompetenza,
        TGT.PKCliente = SRC.PKCliente,
        TGT.PKClienteFattura = SRC.PKClienteFattura,
        TGT.PKGruppoAgenti = SRC.PKGruppoAgenti,
        TGT.PKCapoArea = SRC.PKCapoArea,
        TGT.PKDataFineContratto = SRC.PKDataFineContratto,
        TGT.PKDataInizioContratto = SRC.PKDataInizioContratto,
        TGT.PKGruppoAgenti_Riga = SRC.PKGruppoAgenti_Riga,
        TGT.PKCapoArea_Riga = SRC.PKCapoArea_Riga,
        TGT.PKArticolo = SRC.PKArticolo,
        TGT.PKMacroTipologia = SRC.PKMacroTipologia,

        TGT.IDDocumento = SRC.IDDocumento,
        TGT.IDProfilo = SRC.IDProfilo,
        TGT.Profilo = SRC.Profilo,
        TGT.TipoRegistro = SRC.TipoRegistro,
        TGT.NumeroRegistro = SRC.NumeroRegistro,
        TGT.Registro = SRC.Registro,
        TGT.CodiceEsercizio = SRC.CodiceEsercizio,
        TGT.NumeroDocumento = SRC.NumeroDocumento,
        TGT.TipoSoggettoCommerciale = SRC.TipoSoggettoCommerciale,
        TGT.TipoSoggettoCommercialeFattura = SRC.TipoSoggettoCommercialeFattura,
        TGT.Libero4 = SRC.Libero4,
        TGT.IDLibero1 = SRC.IDLibero1,
        TGT.Libero1 = SRC.Libero1,
        TGT.IDLibero2 = SRC.IDLibero2,
        TGT.Libero2 = SRC.Libero2,
        TGT.IDLibero3 = SRC.IDLibero3,
        TGT.Libero3 = SRC.Libero3,
        TGT.IDTipoFatturazione = SRC.IDTipoFatturazione,
        TGT.TipoFatturazione = SRC.TipoFatturazione,
        TGT.NumeroRiga = SRC.NumeroRiga,
        TGT.CodiceCondizioniPagamento = SRC.CodiceCondizioniPagamento,
        TGT.CondizioniPagamento = SRC.CondizioniPagamento,
        TGT.RinnovoAutomatico = SRC.RinnovoAutomatico,
        TGT.NoteIntestazione = SRC.NoteIntestazione,
        TGT.IsProfiloValidoPerStatisticaFatturato = SRC.IsProfiloValidoPerStatisticaFatturato,
        TGT.IsProfiloValidoPerStatisticaFatturatoFormazione = SRC.IsProfiloValidoPerStatisticaFatturatoFormazione,

        TGT.ImportoTotale = SRC.ImportoTotale,
        TGT.ImportoProvvigioneCapoArea = SRC.ImportoProvvigioneCapoArea,
        TGT.ImportoProvvigioneAgente = SRC.ImportoProvvigioneAgente,
        TGT.ImportoProvvigioneSubagente = SRC.ImportoProvvigioneSubagente,
        TGT.Progressivo = SRC.Progressivo,
		TGT.Quote = SRC.Quote,
        TGT.IDDocumento_Riga_Provenienza = SRC.IDDocumento_Riga_Provenienza,
        TGT.NoteDecisionali = SRC.NoteDecisionali

    WHEN NOT MATCHED
      THEN INSERT (
        IDDocumento_Riga,
        PKDataInizioEsercizio,
        PKDataFineEsercizio,
        PKDataRegistrazione,
        PKDataDocumento,
        PKDataCompetenza,
        PKCliente,
        PKClienteFattura,
        PKGruppoAgenti,
        PKCapoArea,
        PKDataFineContratto,
        PKDataInizioContratto,
        PKGruppoAgenti_Riga,
        PKCapoArea_Riga,
        PKArticolo,
        PKMacroTipologia,
        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
        IDDocumento,
        IDProfilo,
        Profilo,
        TipoRegistro,
        NumeroRegistro,
        Registro,
        CodiceEsercizio,
        NumeroDocumento,
        TipoSoggettoCommerciale,
        TipoSoggettoCommercialeFattura,
        Libero4,
        IDLibero1,
        Libero1,
        IDLibero2,
        Libero2,
        IDLibero3,
        Libero3,
        IDTipoFatturazione,
        TipoFatturazione,
        NumeroRiga,
        CodiceCondizioniPagamento,
        CondizioniPagamento,
        RinnovoAutomatico,
        NoteIntestazione,
        IsProfiloValidoPerStatisticaFatturato,
        IsProfiloValidoPerStatisticaFatturatoFormazione,
        ImportoTotale,
        ImportoProvvigioneCapoArea,
        ImportoProvvigioneAgente,
        ImportoProvvigioneSubagente,
        Progressivo,
		Quote,
        IDDocumento_Riga_Provenienza,
        NoteDecisionali
      )
      VALUES (
        SRC.IDDocumento_Riga,
        SRC.PKDataInizioEsercizio,
        SRC.PKDataFineEsercizio,
        SRC.PKDataRegistrazione,
        SRC.PKDataDocumento,
        SRC.PKDataCompetenza,
        SRC.PKCliente,
        SRC.PKClienteFattura,
        SRC.PKGruppoAgenti,
        SRC.PKCapoArea,
        SRC.PKDataFineContratto,
        SRC.PKDataInizioContratto,
        SRC.PKGruppoAgenti_Riga,
        SRC.PKCapoArea_Riga,
        SRC.PKArticolo,
        SRC.PKMacroTipologia,
        SRC.HistoricalHashKey,
        SRC.ChangeHashKey,
        SRC.HistoricalHashKeyASCII,
        SRC.ChangeHashKeyASCII,
        SRC.InsertDatetime,
        SRC.UpdateDatetime,
        SRC.IsDeleted,
        SRC.IDDocumento,
        SRC.IDProfilo,
        SRC.Profilo,
        SRC.TipoRegistro,
        SRC.NumeroRegistro,
        SRC.Registro,
        SRC.CodiceEsercizio,
        SRC.NumeroDocumento,
        SRC.TipoSoggettoCommerciale,
        SRC.TipoSoggettoCommercialeFattura,
        SRC.Libero4,
        SRC.IDLibero1,
        SRC.Libero1,
        SRC.IDLibero2,
        SRC.Libero2,
        SRC.IDLibero3,
        SRC.Libero3,
        SRC.IDTipoFatturazione,
        SRC.TipoFatturazione,
        SRC.NumeroRiga,
        SRC.CodiceCondizioniPagamento,
        SRC.CondizioniPagamento,
        SRC.RinnovoAutomatico,
        SRC.NoteIntestazione,
        SRC.IsProfiloValidoPerStatisticaFatturato,
        SRC.IsProfiloValidoPerStatisticaFatturatoFormazione,
        SRC.ImportoTotale,
        SRC.ImportoProvvigioneCapoArea,
        SRC.ImportoProvvigioneAgente,
        SRC.ImportoProvvigioneSubagente,
        SRC.Progressivo,
		SRC.Quote,
        SRC.IDDocumento_Riga_Provenienza,
        SRC.NoteDecisionali
      )

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        $action AS merge_action,
        'Staging.Documenti' AS full_olap_table_name,
        'IDDocumento_Riga = ' + CAST(COALESCE(inserted.IDDocumento_Riga, deleted.IDDocumento_Riga) AS NVARCHAR(1000)) AS primary_key_description
    INTO audit.merge_log_details;

    --DELETE FROM Fact.Documenti
    --WHERE IsDeleted = CAST(1 AS BIT);

    UPDATE audit.tables
    SET lastupdated_local = lastupdated_staging
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    COMMIT TRANSACTION;

END;
GO

EXEC Fact.usp_Merge_Documenti;
GO

/*
    SCHEMA_NAME > COMETA
    TABLE_NAME > Scadenza
    STAGING_TABLE_NAME > Scadenze
*/

/**
 * @table Staging.Scadenze
 * @description

 * @depends Landing.COMETA_Scadenza

SELECT TOP 1 * FROM Landing.COMETA_Scadenza;
*/

DROP TABLE IF EXISTS Staging.Scadenze; DELETE FROM audit.tables WHERE provider_name = N'MyDatamartReporting' AND full_table_name = N'Landing.COMETA_Scadenza';
GO

IF NOT EXISTS (SELECT 1 FROM audit.tables WHERE provider_name = N'MyDatamartReporting' AND full_table_name = N'Landing.COMETA_Scadenza')
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
        N'Landing.COMETA_Scadenza',      -- full_table_name - sysname
        N'Staging.Scadenze',      -- staging_table_name - sysname
        N'Fact.Scadenze',      -- datawarehouse_table_name - sysname
        NULL, -- lastupdated_staging - datetime
        NULL  -- lastupdated_local - datetime
    );

END;
GO

IF OBJECT_ID(N'Staging.ScadenzeView', N'V') IS NULL EXEC('CREATE VIEW Staging.ScadenzeView AS SELECT 1 AS fld;');
GO

ALTER VIEW Staging.ScadenzeView
AS
WITH Documenti
AS (
    SELECT
        D.IDDocumento,
        MIN(D.PKDocumenti) AS PKDocumenti

    FROM Fact.Documenti D
    GROUP BY D.IDDocumento
),
ScadenzeSaldi
AS (
    SELECT
        S.id_scadenza AS IDScadenza,
        COALESCE(S.tipo_scadenza, N'') AS TipoScadenza,
        --S.id_sog_commerciale,
        SC.IDSoggettoCommerciale,
        COALESCE(C.PKCliente, -101) AS PKCliente,
        --S.data_scadenza,
        DS.PKData AS PKDataScadenza,
        COALESCE(S.importo, 0.0) AS ImportoScadenza,
        COALESCE(S.stato_scadenza, N'') AS StatoScadenza,
        COALESCE(S.esito_pagamento, N'') AS EsitoPagamento,
        --S.id_documento,
        DD.IDDocumento,
        DD.PKDocumenti,
        SUM(COALESCE(MS.importo, 0.0)) AS ImportoSaldato,
        COALESCE(S.importo, 0.0) - SUM(COALESCE(MS.importo, 0.0)) AS ImportoResiduo

    FROM Landing.COMETA_Scadenza S
    INNER JOIN Staging.SoggettoCommerciale SC ON SC.IDSoggettoCommerciale = S.id_sog_commerciale
    LEFT JOIN Dim.Cliente C ON C.IDSoggettoCommerciale = SC.IDSoggettoCommerciale
    INNER JOIN Dim.Data DS ON DS.PKData = S.data_scadenza
    INNER JOIN Documenti DD ON DD.IDDocumento = S.id_documento
    LEFT JOIN Landing.COMETA_MovimentiScadenza MS ON MS.id_scadenza = S.id_scadenza
        AND MS.IsDeleted = CAST(0 AS BIT)
    WHERE S.IsDeleted = CAST(0 AS BIT)
        AND S.esito_pagamento IN (N'E', N'I')
        AND S.data_scadenza <= CAST(CURRENT_TIMESTAMP AS DATETIME2)
        AND S.stato_scadenza = N'D'
    GROUP BY COALESCE (S.tipo_scadenza, N''),
        COALESCE (C.PKCliente, -101),
        COALESCE (S.importo, 0.0),
        COALESCE (S.stato_scadenza, N''),
        COALESCE (S.esito_pagamento, N''),
        S.id_scadenza,
        SC.IDSoggettoCommerciale,
        DS.PKData,
        DD.IDDocumento,
        DD.PKDocumenti,
        S.importo
),
TableData
AS (
    SELECT
        SS.IDScadenza,

        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            SS.IDScadenza,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            SS.TipoScadenza,
            SS.IDSoggettoCommerciale,
            SS.PKCliente,
            SS.PKDataScadenza,
            SS.ImportoScadenza,
            SS.StatoScadenza,
            SS.EsitoPagamento,
            SS.IDDocumento,
            SS.PKDocumenti,
            SS.ImportoSaldato,
            SS.ImportoResiduo,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,

        SS.TipoScadenza,
        SS.IDSoggettoCommerciale,
        SS.PKCliente,
        SS.PKDataScadenza,
        SS.ImportoScadenza,
        SS.StatoScadenza,
        SS.EsitoPagamento,
        SS.IDDocumento,
        SS.PKDocumenti,
        SS.ImportoSaldato,
        SS.ImportoResiduo

    FROM ScadenzeSaldi SS
)
SELECT
    -- Chiavi
    TD.IDScadenza,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Dimensioni
    TD.TipoScadenza,
    TD.IDSoggettoCommerciale,
    TD.PKCliente,
    TD.PKDataScadenza,
    TD.StatoScadenza,
    TD.EsitoPagamento,
    TD.IDDocumento,
    TD.PKDocumenti,

    -- Misure
    TD.ImportoScadenza,
    TD.ImportoSaldato,
    TD.ImportoResiduo

FROM TableData TD;
GO

IF OBJECT_ID(N'Staging.Scadenze', N'U') IS NOT NULL DROP TABLE Staging.Scadenze;
GO

IF OBJECT_ID(N'Staging.Scadenze', N'U') IS NULL
BEGIN
    SELECT TOP 0 * INTO Staging.Scadenze FROM Staging.ScadenzeView;

    ALTER TABLE Staging.Scadenze ADD CONSTRAINT PK_Landing_COMETA_Scadenza PRIMARY KEY CLUSTERED (UpdateDatetime, IDScadenza);

    ALTER TABLE Staging.Scadenze ALTER COLUMN TipoScadenza CHAR(1) NOT NULL;
    ALTER TABLE Staging.Scadenze ALTER COLUMN PKCliente INT NOT NULL;
    ALTER TABLE Staging.Scadenze ALTER COLUMN StatoScadenza CHAR(1) NOT NULL;
    ALTER TABLE Staging.Scadenze ALTER COLUMN EsitoPagamento CHAR(1) NOT NULL;
    ALTER TABLE Staging.Scadenze ALTER COLUMN IDDocumento INT NOT NULL;
    ALTER TABLE Staging.Scadenze ALTER COLUMN PKDocumenti INT NOT NULL;
    ALTER TABLE Staging.Scadenze ALTER COLUMN ImportoScadenza DECIMAL(10, 2) NOT NULL;
    ALTER TABLE Staging.Scadenze ALTER COLUMN ImportoSaldato DECIMAL(10, 2) NOT NULL;
    ALTER TABLE Staging.Scadenze ALTER COLUMN ImportoResiduo DECIMAL(10, 2) NOT NULL;

    CREATE UNIQUE NONCLUSTERED INDEX IX_COMETA_Scadenza_BusinessKey ON Staging.Scadenze (IDScadenza);
END;
GO

IF OBJECT_ID(N'Staging.usp_Reload_Scadenze', N'P') IS NULL EXEC('CREATE PROCEDURE Staging.usp_Reload_Scadenze AS RETURN 0;');
GO

ALTER PROCEDURE Staging.usp_Reload_Scadenze
AS
BEGIN

    SET NOCOUNT ON;

    DECLARE @lastupdated_staging DATETIME;
    DECLARE @provider_name NVARCHAR(60) = N'MyDatamartReporting';
    DECLARE @full_table_name sysname = N'Landing.COMETA_Scadenza';

    SELECT TOP 1 @lastupdated_staging = lastupdated_staging
    FROM audit.tables
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    IF (@lastupdated_staging IS NULL) SET @lastupdated_staging = CAST('19000101' AS DATETIME);

    BEGIN TRANSACTION

    TRUNCATE TABLE Staging.Scadenze;

    INSERT INTO Staging.Scadenze
    SELECT * FROM Staging.ScadenzeView
    WHERE UpdateDatetime > @lastupdated_staging;

    SELECT @lastupdated_staging = MAX(UpdateDatetime) FROM Staging.Scadenze;

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

EXEC Staging.usp_Reload_Scadenze;
GO

DROP TABLE IF EXISTS Fact.Scadenze; DROP SEQUENCE IF EXISTS dbo.seq_Fact_Scadenze;
GO

IF OBJECT_ID('dbo.seq_Fact_Scadenze', 'SO') IS NULL
BEGIN

    CREATE SEQUENCE dbo.seq_Fact_Scadenze START WITH 1;

END;
GO

IF OBJECT_ID('Fact.Scadenze', 'U') IS NULL
BEGIN

    CREATE TABLE Fact.Scadenze (
        PKScadenze INT NOT NULL CONSTRAINT PK_Fact_Scadenze PRIMARY KEY CLUSTERED CONSTRAINT DFT_Fact_Scadenze_PKScadenze DEFAULT (NEXT VALUE FOR dbo.seq_Fact_Scadenze),

	    PKCliente INT NOT NULL CONSTRAINT FK_Fact_Scadenze_PKCliente FOREIGN KEY REFERENCES Dim.Cliente (PKCliente),
        PKDataScadenza DATE NOT NULL CONSTRAINT FK_Fact_Scadenze_PKDataScadenza FOREIGN KEY REFERENCES Dim.Data (PKData),
        PKDocumenti INT NOT NULL CONSTRAINT FK_Fact_Scadenze_PKDocumenti FOREIGN KEY REFERENCES Fact.Documenti (PKDocumenti),

	    HistoricalHashKey VARBINARY(20) NULL,
	    ChangeHashKey VARBINARY(20) NULL,
	    HistoricalHashKeyASCII VARCHAR(34) NULL,
	    ChangeHashKeyASCII VARCHAR(34) NULL,
	    InsertDatetime DATETIME NOT NULL,
	    UpdateDatetime DATETIME NOT NULL,
	    IsDeleted BIT NOT NULL,

	    IDScadenza INT NOT NULL,
	    TipoScadenza CHAR(1) NOT NULL,
	    IDSoggettoCommerciale INT NOT NULL,
	    StatoScadenza CHAR(1) NOT NULL,
	    EsitoPagamento CHAR(1) NOT NULL,
	    IDDocumento INT NOT NULL,

	    ImportoScadenza DECIMAL(10, 2) NOT NULL,
	    ImportoSaldato DECIMAL(10, 2) NOT NULL,
	    ImportoResiduo DECIMAL(10, 2) NOT NULL
    );

    CREATE UNIQUE NONCLUSTERED INDEX IX_Fact_Scadenze_IDScadenza ON Fact.Scadenze (IDScadenza);

    ALTER SEQUENCE dbo.seq_Fact_Scadenze RESTART WITH 1;

END;
GO

IF OBJECT_ID(N'Fact.usp_Merge_Scadenze', N'P') IS NULL EXEC('CREATE PROCEDURE Fact.usp_Merge_Scadenze AS RETURN 0;');
GO

ALTER PROCEDURE Fact.usp_Merge_Scadenze
AS
BEGIN

    SET NOCOUNT ON;

    BEGIN TRANSACTION 

    DECLARE @provider_name NVARCHAR(60) = N'MyDatamartReporting';
    DECLARE @full_table_name sysname = N'Landing.COMETA_Documento_Riga';

    MERGE INTO Fact.Scadenze AS TGT
    USING Staging.Scadenze (nolock) AS SRC
    ON SRC.IDScadenza = TGT.IDScadenza

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.IsDeleted = SRC.IsDeleted,

        TGT.PKCliente = SRC.PKCliente,
        TGT.PKDataScadenza = SRC.PKDataScadenza,
        TGT.PKDocumenti = SRC.PKDocumenti,

        TGT.IDScadenza = SRC.IDScadenza,
        TGT.TipoScadenza = SRC.TipoScadenza,
        TGT.IDSoggettoCommerciale = SRC.IDSoggettoCommerciale,
        TGT.StatoScadenza = SRC.StatoScadenza,
        TGT.EsitoPagamento = SRC.EsitoPagamento,
        TGT.IDDocumento = SRC.IDDocumento,
        TGT.ImportoScadenza = SRC.ImportoScadenza,
        TGT.ImportoSaldato = SRC.ImportoSaldato,
        TGT.ImportoResiduo = SRC.ImportoResiduo

    WHEN NOT MATCHED
      THEN INSERT (
        PKCliente,
        PKDataScadenza,
        PKDocumenti,
        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
        IDScadenza,
        TipoScadenza,
        IDSoggettoCommerciale,
        StatoScadenza,
        EsitoPagamento,
        IDDocumento,
        ImportoScadenza,
        ImportoSaldato,
        ImportoResiduo
      )
      VALUES (
        SRC.PKCliente,
        SRC.PKDataScadenza,
        SRC.PKDocumenti,
        SRC.HistoricalHashKey,
        SRC.ChangeHashKey,
        SRC.HistoricalHashKeyASCII,
        SRC.ChangeHashKeyASCII,
        SRC.InsertDatetime,
        SRC.UpdateDatetime,
        SRC.IsDeleted,
        SRC.IDScadenza,
        SRC.TipoScadenza,
        SRC.IDSoggettoCommerciale,
        SRC.StatoScadenza,
        SRC.EsitoPagamento,
        SRC.IDDocumento,
        SRC.ImportoScadenza,
        SRC.ImportoSaldato,
        SRC.ImportoResiduo
      )

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        $action AS merge_action,
        'Staging.Scadenze' AS full_olap_table_name,
        'IDScadenza = ' + CAST(COALESCE(inserted.IDScadenza, deleted.IDScadenza) AS NVARCHAR(1000)) AS primary_key_description
    INTO audit.merge_log_details;

    --DELETE FROM Fact.Scadenze
    --WHERE IsDeleted = CAST(1 AS BIT);

    UPDATE audit.tables
    SET lastupdated_local = lastupdated_staging
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    COMMIT TRANSACTION;

END;
GO

EXEC Fact.usp_Merge_Scadenze;
GO

/* Verifiche */

SELECT
    B.IsDeleted AS IsDeleted_before,
    A.IsDeleted AS IsDeleted_after,
    COUNT(1)
FROM Fact.Documenti A
FULL JOIN CesiDW_misc.backups.Fact_Documenti_20220628 B ON A.IDDocumento_Riga = B.IDDocumento_Riga
GROUP BY A.IsDeleted,
         B.IsDeleted
ORDER BY A.IsDeleted,
         B.IsDeleted;
GO
