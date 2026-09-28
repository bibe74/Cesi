CREATE   PROCEDURE Dim.usp_Merge_Cliente
AS
BEGIN

    SET NOCOUNT ON;

    BEGIN TRANSACTION 

    DECLARE @provider_name NVARCHAR(60) = N'MyDatamartReporting';
    DECLARE @full_table_name sysname = N'Landing.COMETA_SoggettoCommerciale';

    -- Aggiornamento clienti NOP passati in COMETA
    TRUNCATE TABLE Staging.ClientiNOPInCometa;

    INSERT INTO Staging.ClientiNOPInCometa SELECT * FROM Staging.ClientiNOPInCometaView;

    UPDATE T
    SET T.PKCliente = CNIC.PKClienteCometa
    FROM Dim.Utente T
    INNER JOIN Staging.ClientiNOPInCometa CNIC ON CNIC.PKClienteNOP = T.PKCliente;

    UPDATE T
    SET T.PKCliente = CNIC.PKClienteCometa
    FROM Dim.ClienteAccessi T
    INNER JOIN Staging.ClientiNOPInCometa CNIC ON CNIC.PKClienteNOP = T.PKCliente;

    UPDATE T
    SET T.PKCliente = CNIC.PKClienteCometa
    FROM Fact.Accessi T
    INNER JOIN Staging.ClientiNOPInCometa CNIC ON CNIC.PKClienteNOP = T.PKCliente;

    UPDATE T
    SET T.PKCliente = CNIC.PKClienteCometa
    FROM Fact.AccessiUltimi3Mesi T
    INNER JOIN Staging.ClientiNOPInCometa CNIC ON CNIC.PKClienteNOP = T.PKCliente;

    UPDATE T
    SET T.PKCliente = CNIC.PKClienteCometa
    FROM Fact.Documenti T
    INNER JOIN Staging.ClientiNOPInCometa CNIC ON CNIC.PKClienteNOP = T.PKCliente;

    UPDATE T
    SET T.PKClienteFattura = CNIC.PKClienteCometa
    FROM Fact.Documenti T
    INNER JOIN Staging.ClientiNOPInCometa CNIC ON CNIC.PKClienteNOP = T.PKClienteFattura;

    UPDATE T
    SET T.PKCliente = CNIC.PKClienteCometa
    FROM Fact.Scadenze T
    INNER JOIN Staging.ClientiNOPInCometa CNIC ON CNIC.PKClienteNOP = T.PKCliente;

    UPDATE T
    SET T.PKCliente = CNIC.PKClienteCometa
    FROM Fact.CreditiOpenAI T
    INNER JOIN Staging.ClientiNOPInCometa CNIC ON CNIC.PKClienteNOP = T.PKCliente;

    WITH ClientiCometaDaAggiornare
    AS (
        SELECT DISTINCT
            T.PKCliente,
            CNIC.PKClienteCometa

        FROM Fact.CreditiOpenAIDettaglio T
        INNER JOIN Staging.ClientiNOPInCometa CNIC ON CNIC.PKClienteNOP = T.PKCliente
    )
    DELETE COAID
    FROM Fact.CreditiOpenAIDettaglio COAID
    INNER JOIN ClientiCometaDaAggiornare CCDA ON CCDA.PKCliente = COAID.PKCliente;

    EXEC Staging.usp_Reload_CreditiOpenAIDettaglio;

    UPDATE T
    SET T.PKCliente = CNIC.PKClienteCometa
    FROM Fact.CreditiOpenAIDettaglio T
    INNER JOIN Staging.ClientiNOPInCometa CNIC ON CNIC.PKClienteNOP = T.PKCliente;

    DELETE C
    FROM Dim.Cliente C
    INNER JOIN Staging.ClientiNOPInCometa CNIC ON CNIC.PKClienteNOP = C.PKCliente
    LEFT JOIN Fact.CreditiOpenAIDettaglio COAID ON COAID.PKCliente = C.PKCliente
    WHERE COAID.PKCreditiOpenAIDettaglio IS NULL;

    -- Verifica clienti non-COMETA passati in COMETA
    UPDATE C
    SET C.IDSoggettoCommerciale = CC.IDSoggettoCommerciale,
        C.ProvenienzaAnagrafica = CC.ProvenienzaAnagrafica

    FROM Dim.ClienteCometaView CC
    INNER JOIN Dim.Cliente C ON C.Email = CC.Email
        AND C.ProvenienzaAnagrafica <> N'COMETA'
        AND C.PKCliente > 0;

    WITH TargetTable
    AS (
        SELECT
            *
        FROM Dim.Cliente
        WHERE PKCliente > 0
    )
    MERGE INTO TargetTable AS TGT
    USING Dim.ClienteCometaView (nolock) AS SRC
    ON SRC.IDSoggettoCommerciale = TGT.IDSoggettoCommerciale

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.IsDeleted = SRC.IsDeleted,
        
        TGT.Email = SRC.Email,
        TGT.IDAnagraficaCometa = SRC.IDAnagraficaCometa,
        TGT.HasAnagraficaCometa = TGT.HasAnagraficaCometa,
        TGT.HasAnagraficaNopCommerce = TGT.HasAnagraficaNopCommerce,
        TGT.HasAnagraficaMySolution = TGT.HasAnagraficaMySolution,
        TGT.ProvenienzaAnagrafica = TGT.ProvenienzaAnagrafica,
        TGT.CodiceCliente = SRC.CodiceCliente,
        TGT.TipoSoggettoCommerciale = SRC.TipoSoggettoCommerciale,
        TGT.RagioneSociale = SRC.RagioneSociale,
        TGT.CodiceFiscale = SRC.CodiceFiscale,
        TGT.PartitaIVA = SRC.PartitaIVA,
        TGT.Indirizzo = SRC.Indirizzo,
        TGT.CAP = SRC.CAP,
        TGT.Localita = SRC.Localita,
        TGT.Provincia = SRC.Provincia,
        TGT.Regione = SRC.Regione,
        TGT.Macroregione = SRC.Macroregione,
        TGT.Nazione = SRC.Nazione,
        TGT.Telefono = SRC.Telefono,
        TGT.Cellulare = SRC.Cellulare,
        TGT.Fax = SRC.Fax,
        TGT.TipoCliente = SRC.TipoCliente,
        TGT.Agente = SRC.Agente,
        TGT.PKDataInizioContratto = SRC.PKDataInizioContratto,
        TGT.PKDataFineContratto = SRC.PKDataFineContratto,
        TGT.PKDataDisdetta = SRC.PKDataDisdetta,
        TGT.MotivoDisdetta = SRC.MotivoDisdetta,
        TGT.PKGruppoAgenti = SRC.PKGruppoAgenti,
        TGT.Cognome = SRC.Cognome,
        TGT.Nome = SRC.Nome,
        TGT.IsAttivo = SRC.IsAttivo,
        TGT.IsAbbonato = SRC.IsAbbonato,
        --TGT.IDSoggettoCommerciale_migrazione = SRC.IDSoggettoCommerciale_migrazione,
        --TGT.IDSoggettoCommerciale_migrazione_old = SRC.IDSoggettoCommerciale_migrazione_old,
        TGT.IDProvincia = SRC.IDProvincia,
        TGT.CapoAreaDefault = SRC.CapoAreaDefault,
        TGT.AgenteDefault = SRC.AgenteDefault,
        TGT.HasRoleMySolutionDemo = SRC.HasRoleMySolutionDemo,
        TGT.HasRoleMySolutionInterno = SRC.HasRoleMySolutionInterno,
        TGT.HasAbbonamentoMySolution = SRC.HasAbbonamentoMySolution,
        TGT.HasAbbonamentoMIA = SRC.HasAbbonamentoMIA,
        TGT.AgenteZoho = SRC.AgenteZoho,
        TGT.IDProfessione = SRC.IDProfessione,
        TGT.Professione = SRC.Professione

    WHEN NOT MATCHED
      THEN INSERT (
        IDSoggettoCommerciale,
        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
        Email,
        IDAnagraficaCometa,
        HasAnagraficaCometa,
        HasAnagraficaNopCommerce,
        HasAnagraficaMySolution,
        ProvenienzaAnagrafica,
        CodiceCliente,
        TipoSoggettoCommerciale,
        RagioneSociale,
        CodiceFiscale,
        PartitaIVA,
        Indirizzo,
        CAP,
        Localita,
        Provincia,
        Regione,
        Macroregione,
        Nazione,
        Telefono,
        Cellulare,
        Fax,
        TipoCliente,
        Agente,
        PKDataInizioContratto,
        PKDataFineContratto,
        PKDataDisdetta,
        MotivoDisdetta,
        PKGruppoAgenti,
        Cognome,
        Nome,
        IsAbbonato,
        --IDSoggettoCommerciale_migrazione,
        --IDSoggettoCommerciale_migrazione_old,
        IDProvincia,
        CapoAreaDefault,
        AgenteDefault,
        HasRoleMySolutionDemo,
        HasRoleMySolutionInterno,
        HasAbbonamentoMySolution,
        HasAbbonamentoMIA,
        AgenteZoho,
        IDProfessione,
        Professione
      )
      VALUES (
        SRC.IDSoggettoCommerciale,
        SRC.HistoricalHashKey,
        SRC.ChangeHashKey,
        SRC.HistoricalHashKeyASCII,
        SRC.ChangeHashKeyASCII,
        SRC.InsertDatetime,
        SRC.UpdateDatetime,
        SRC.IsDeleted,
        SRC.Email,
        SRC.IDAnagraficaCometa,
        SRC.HasAnagraficaCometa,
        SRC.HasAnagraficaNopCommerce,
        SRC.HasAnagraficaMySolution,
        SRC.ProvenienzaAnagrafica,
        SRC.CodiceCliente,
        SRC.TipoSoggettoCommerciale,
        SRC.RagioneSociale,
        SRC.CodiceFiscale,
        SRC.PartitaIVA,
        SRC.Indirizzo,
        SRC.CAP,
        SRC.Localita,
        SRC.Provincia,
        SRC.Regione,
        SRC.Macroregione,
        SRC.Nazione,
        SRC.Telefono,
        SRC.Cellulare,
        SRC.Fax,
        SRC.TipoCliente,
        SRC.Agente,
        SRC.PKDataInizioContratto,
        SRC.PKDataFineContratto,
        SRC.PKDataDisdetta,
        SRC.MotivoDisdetta,
        SRC.PKGruppoAgenti,
        SRC.Cognome,
        SRC.Nome,
        SRC.IsAbbonato,
        --SRC.IDSoggettoCommerciale_migrazione,
        --SRC.IDSoggettoCommerciale_migrazione_old,
        SRC.IDProvincia,
        SRC.CapoAreaDefault,
        SRC.AgenteDefault,
        SRC.HasRoleMySolutionDemo,
        SRC.HasRoleMySolutionInterno,
        SRC.HasAbbonamentoMySolution,
        SRC.HasAbbonamentoMIA,
        SRC.AgenteZoho,
        SRC.IDProfessione,
        SRC.Professione
      )

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        $action AS merge_action,
        'Dim.Cliente' AS full_olap_table_name,
        'IDSoggettoCommerciale = ' + CAST(COALESCE(inserted.IDSoggettoCommerciale, deleted.IDSoggettoCommerciale) AS NVARCHAR(1000)) AS primary_key_description
    INTO audit.merge_log_details;

    WITH TargetTable
    AS (
        SELECT
            *
        FROM Dim.Cliente
        WHERE IDSoggettoCommerciale < -1000
    )
    MERGE INTO TargetTable AS TGT
    USING Dim.ClienteNOPView (nolock) AS SRC
    ON SRC.IDSoggettoCommerciale = TGT.IDSoggettoCommerciale

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.IsDeleted = SRC.IsDeleted,
        
        TGT.Email = SRC.Email,
        TGT.IDAnagraficaCometa = SRC.IDAnagraficaCometa,
        TGT.HasAnagraficaCometa = TGT.HasAnagraficaCometa,
        TGT.HasAnagraficaNopCommerce = TGT.HasAnagraficaNopCommerce,
        TGT.HasAnagraficaMySolution = TGT.HasAnagraficaMySolution,
        TGT.ProvenienzaAnagrafica = TGT.ProvenienzaAnagrafica,
        TGT.CodiceCliente = SRC.CodiceCliente,
        TGT.TipoSoggettoCommerciale = SRC.TipoSoggettoCommerciale,
        TGT.RagioneSociale = SRC.RagioneSociale,
        TGT.CodiceFiscale = SRC.CodiceFiscale,
        TGT.PartitaIVA = SRC.PartitaIVA,
        TGT.Indirizzo = SRC.Indirizzo,
        TGT.CAP = SRC.CAP,
        TGT.Localita = SRC.Localita,
        TGT.Provincia = SRC.Provincia,
        TGT.Regione = SRC.Regione,
        TGT.Macroregione = SRC.Macroregione,
        TGT.Nazione = SRC.Nazione,
        TGT.Telefono = SRC.Telefono,
        TGT.Cellulare = SRC.Cellulare,
        TGT.Fax = SRC.Fax,
        TGT.TipoCliente = SRC.TipoCliente,
        TGT.Agente = SRC.Agente,
        TGT.PKDataInizioContratto = SRC.PKDataInizioContratto,
        TGT.PKDataFineContratto = SRC.PKDataFineContratto,
        TGT.PKDataDisdetta = SRC.PKDataDisdetta,
        TGT.MotivoDisdetta = SRC.MotivoDisdetta,
        TGT.PKGruppoAgenti = SRC.PKGruppoAgenti,
        TGT.Cognome = SRC.Cognome,
        TGT.Nome = SRC.Nome,
        TGT.IsAttivo = SRC.IsAttivo,
        TGT.IsAbbonato = SRC.IsAbbonato,
        --TGT.IDSoggettoCommerciale_migrazione = SRC.IDSoggettoCommerciale_migrazione,
        --TGT.IDSoggettoCommerciale_migrazione_old = SRC.IDSoggettoCommerciale_migrazione_old,
        TGT.IDProvincia = SRC.IDProvincia,
        TGT.CapoAreaDefault = SRC.CapoAreaDefault,
        TGT.AgenteDefault = SRC.AgenteDefault,
        TGT.HasRoleMySolutionDemo = SRC.HasRoleMySolutionDemo,
        TGT.HasRoleMySolutionInterno = SRC.HasRoleMySolutionInterno,
        TGT.HasAbbonamentoMySolution = SRC.HasAbbonamentoMySolution,
        TGT.HasAbbonamentoMIA = SRC.HasAbbonamentoMIA,
        TGT.IDProfessione = SRC.IDProfessione,
        TGT.Professione = SRC.Professione

    WHEN NOT MATCHED
      THEN INSERT (
        IDSoggettoCommerciale,
        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
        Email,
        IDAnagraficaCometa,
        HasAnagraficaCometa,
        HasAnagraficaNopCommerce,
        HasAnagraficaMySolution,
        ProvenienzaAnagrafica,
        CodiceCliente,
        TipoSoggettoCommerciale,
        RagioneSociale,
        CodiceFiscale,
        PartitaIVA,
        Indirizzo,
        CAP,
        Localita,
        Provincia,
        Regione,
        Macroregione,
        Nazione,
        Telefono,
        Cellulare,
        Fax,
        TipoCliente,
        Agente,
        PKDataInizioContratto,
        PKDataFineContratto,
        PKDataDisdetta,
        MotivoDisdetta,
        PKGruppoAgenti,
        Cognome,
        Nome,
        IsAbbonato,
        --IDSoggettoCommerciale_migrazione,
        --IDSoggettoCommerciale_migrazione_old,
        IDProvincia,
        CapoAreaDefault,
        AgenteDefault,
        HasRoleMySolutionDemo,
        HasRoleMySolutionInterno,
        HasAbbonamentoMySolution,
        HasAbbonamentoMIA,
        IDProfessione,
        Professione
      )
      VALUES (
        SRC.IDSoggettoCommerciale,
        SRC.HistoricalHashKey,
        SRC.ChangeHashKey,
        SRC.HistoricalHashKeyASCII,
        SRC.ChangeHashKeyASCII,
        SRC.InsertDatetime,
        SRC.UpdateDatetime,
        SRC.IsDeleted,
        SRC.Email,
        SRC.IDAnagraficaCometa,
        SRC.HasAnagraficaCometa,
        SRC.HasAnagraficaNopCommerce,
        SRC.HasAnagraficaMySolution,
        SRC.ProvenienzaAnagrafica,
        SRC.CodiceCliente,
        SRC.TipoSoggettoCommerciale,
        SRC.RagioneSociale,
        SRC.CodiceFiscale,
        SRC.PartitaIVA,
        SRC.Indirizzo,
        SRC.CAP,
        SRC.Localita,
        SRC.Provincia,
        SRC.Regione,
        SRC.Macroregione,
        SRC.Nazione,
        SRC.Telefono,
        SRC.Cellulare,
        SRC.Fax,
        SRC.TipoCliente,
        SRC.Agente,
        SRC.PKDataInizioContratto,
        SRC.PKDataFineContratto,
        SRC.PKDataDisdetta,
        SRC.MotivoDisdetta,
        SRC.PKGruppoAgenti,
        SRC.Cognome,
        SRC.Nome,
        SRC.IsAbbonato,
        --SRC.IDSoggettoCommerciale_migrazione,
        --SRC.IDSoggettoCommerciale_migrazione_old,
        SRC.IDProvincia,
        SRC.CapoAreaDefault,
        SRC.AgenteDefault,
        SRC.HasRoleMySolutionDemo,
        SRC.HasRoleMySolutionInterno,
        SRC.HasAbbonamentoMySolution,
        SRC.HasAbbonamentoMIA,
        SRC.IDProfessione,
        SRC.Professione
      )

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        $action AS merge_action,
        'Dim.Cliente' AS full_olap_table_name,
        'IDSoggettoCommerciale = ' + CAST(COALESCE(inserted.IDSoggettoCommerciale, deleted.IDSoggettoCommerciale) AS NVARCHAR(1000)) AS primary_key_description
    INTO audit.merge_log_details;

    --DELETE FROM Dim.Cliente
    --WHERE IsDeleted = CAST(1 AS BIT);

    -- Ricalcolo flag IsAttivo
    IF OBJECT_ID('Fact.Accessi', 'U') IS NOT NULL
    BEGIN

        UPDATE C
        SET C.IsAttivo = CAST(1 AS BIT)
        FROM Dim.Cliente C
        WHERE CURRENT_TIMESTAMP BETWEEN C.PKDataInizioContratto AND C.PKDataFineContratto
            OR EXISTS (SELECT TOP (1) A.PKCliente FROM Fact.Accessi A WHERE A.PKCliente = C.PKCliente AND A.PKData >= DATEADD(MONTH, -1, CAST(CURRENT_TIMESTAMP AS DATE)))

    END;

    ------ Verifica migrazioni da NOPCOMMERCE a COMETA
    ----UPDATE T
    ----SET T.IsDeleted = CAST(1 AS BIT)

    ----FROM Dim.Cliente T
    ----INNER JOIN Staging.Cliente SC ON SC.Email = T.Email
    ----    AND SC.ProvenienzaAnagrafica IN (N'COMETA')
    ----WHERE T.ProvenienzaAnagrafica IN (N'NOPCOMMERCE');

    ----UPDATE CNew
    ----SET CNew.IDSoggettoCommerciale_migrazione_old = COld.IDSoggettoCommerciale

    ----FROM Dim.Cliente CNew
    ----INNER JOIN Staging.Cliente SC ON SC.Email = CNew.Email
    ----    AND SC.ProvenienzaAnagrafica IN (N'COMETA')
    ----INNER JOIN Dim.Cliente COld ON COld.Email = CNew.Email
    ----    AND COld.ProvenienzaAnagrafica IN (N'NOPCOMMERCE')
    ----WHERE CNew.ProvenienzaAnagrafica IN (N'COMETA');

    -- Aggiornamento flag IsClienteFormazione
    WITH ClientiFormazione
    AS (
        SELECT DISTINCT PKCliente
        FROM Fact.Documenti
        WHERE IsProfiloValidoPerStatisticaFatturatoFormazione = CAST(1 AS BIT)
            AND IsDeleted = CAST(0 AS BIT)
    )
    UPDATE C
    SET C.IsClienteFormazione = CASE WHEN CF.PKCliente IS NOT NULL THEN 1 ELSE 0 END
    FROM Dim.Cliente C
    LEFT JOIN ClientiFormazione CF ON CF.PKCliente = C.PKCliente;

    ------ Aggiornamento CapoAreaDefault
    ----WITH CapoAreaDefaultByCAP
    ----AS (
    ----    SELECT
    ----        CCA.IDProvincia,
    ----        CCA.CAP,
    ----        MAX(CCA.CapoArea) AS CapoAreaDefault,
    ----        MAX(CCA.Agente) AS AgenteDefault

    ----    FROM Import.ComuneCAPAgente CCA
    ----    GROUP BY CCA.IDProvincia,
    ----        CCA.CAP
    ----    HAVING COUNT(DISTINCT CCA.CapoArea) = 1
    ----),
    ----CapoAreaDefaultByLocalita
    ----AS (
    ----    SELECT
    ----        CCA.IDProvincia,
    ----        CCA.Comune AS Localita,
    ----        MAX(CCA.CapoArea) AS CapoAreaDefault,
    ----        MAX(CCA.Agente) AS AgenteDefault

    ----    FROM Import.ComuneCAPAgente CCA
    ----    GROUP BY CCA.IDProvincia,
    ----        CCA.Comune
    ----    HAVING COUNT(1) = 1
    ----)
    ----UPDATE C
    ----SET C.CapoAreaDefault = COALESCE(CADBL.CapoAreaDefault, CADBCAP.CapoAreaDefault, PA.CapoArea, N''),
    ----    C.AgenteDefault = COALESCE(CADBL.AgenteDefault, CADBCAP.AgenteDefault, PA.Agente, N'')

    ----FROM Dim.Cliente C
    ----LEFT JOIN Import.ProvinciaAgente PA ON PA.IDProvincia = C.IDProvincia
    ----LEFT JOIN CapoAreaDefaultByCAP CADBCAP ON CADBCAP.IDProvincia = C.IDProvincia AND CADBCAP.CAP = C.CAP
    ----LEFT JOIN CapoAreaDefaultByLocalita CADBL ON CADBL.IDProvincia = C.IDProvincia AND CADBL.Localita = C.Localita;

    -- Aggiornamento PKDataDisdetta
    ----WITH DocumentiMySolution
    ----AS (
    ----    SELECT DISTINCT
    ----        D.IDDocumento,
    ----        D.PKCliente,
    ----        D.PKDataFineContratto,
    ----        D.PKDataDisdetta

    ----    FROM Fact.Documenti D
    ----    WHERE D.IDProfilo = N'ORDCLI'
    ----),
    ----DocumentiMySolutionNumerati
    ----AS (
    ----    SELECT
    ----        DMS.IDDocumento,
    ----        DMS.PKCliente,
    ----        DMS.PKDataFineContratto,
    ----        DMS.PKDataDisdetta,
    ----        ROW_NUMBER() OVER (PARTITION BY DMS.PKCliente ORDER BY DMS.PKDataFineContratto DESC) AS rn

    ----    FROM DocumentiMySolution DMS
    ----)
    ----UPDATE C
    ----SET C.PKDataDisdetta = DMSN.PKDataDisdetta
    ----FROM Dim.Cliente C
    ----INNER JOIN DocumentiMySolutionNumerati DMSN ON DMSN.PKCliente = C.PKCliente
    ----    AND DMSN.rn = 1;

    WITH AnnoCorrente
    AS (
        SELECT
            MIN(D.PKData) AS PKDataInizioPeriodo,
            MAX(D.PKData) AS PKDataFinePeriodo

        FROM Dim.Data D
        WHERE D.Anno = YEAR(CURRENT_TIMESTAMP)
    ),
    ClienteHasAbbonamento
    AS (
        SELECT
            C.PKCliente,
            CAST(MAX(CASE WHEN A.Tipo IN (N'FISCO', N'FULL', N'LAVORO') THEN 1 ELSE 0 END) AS BIT) AS HasAbbonamentoMySolution,
            CAST(MAX(CASE WHEN A.Tipo IN (N'MIAFISCO', N'MIAFULL', N'MIALAVORO') THEN 1 ELSE 0 END) AS BIT) AS HasAbbonamentoMIA

        FROM Fact.Documenti D
        INNER JOIN AnnoCorrente AC ON D.PKDataFineContratto >= AC.PKDataInizioPeriodo AND D.PKDataInizioContratto <= AC.PKDataFinePeriodo
        INNER JOIN Dim.Cliente C ON C.PKCliente = D.PKCliente
            AND C.IsDeleted = CAST(0 AS BIT)
        INNER JOIN Dim.Articolo A ON A.PKArticolo = D.PKArticolo
            AND A.IsDeleted = CAST(0 AS BIT)
        WHERE D.Profilo = N'ORDINE CLIENTE'
            AND D.IsDeleted = CAST(0 AS BIT)
        GROUP BY C.PKCliente
    )
    UPDATE C
    SET C.HasAbbonamentoMySolution = COALESCE(CHA.HasAbbonamentoMySolution, 0),
        C.HasAbbonamentoMIA = COALESCE(CHA.HasAbbonamentoMIA, 0)

    FROM Dim.Cliente C
    LEFT JOIN ClienteHasAbbonamento CHA ON CHA.PKCliente = C.PKCliente;

    UPDATE audit.tables
    SET lastupdated_local = lastupdated_staging
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    COMMIT TRANSACTION;

END;

GO

