
CREATE PROCEDURE Staging.usp_Reload_Cliente
AS
BEGIN

    SET NOCOUNT ON;

    DECLARE @lastupdated_staging DATETIME;
    DECLARE @provider_name NVARCHAR(60) = N'MyDatamartReporting';
    DECLARE @full_table_name sysname = N'Landing.COMETA_SoggettoCommerciale';
    DECLARE @minIDSoggettoCommerciale INT = -1000000;

    SELECT TOP 1 @lastupdated_staging = lastupdated_staging
    FROM audit.tables
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    IF (@lastupdated_staging IS NULL) SET @lastupdated_staging = CAST('19000101' AS DATETIME);

    BEGIN TRANSACTION

    TRUNCATE TABLE Staging.Cliente;

    INSERT INTO Staging.Cliente
    SELECT * FROM Staging.ClienteCometaView;
    --WHERE UpdateDatetime > @lastupdated_staging;

    SELECT @minIDSoggettoCommerciale = CASE WHEN MIN(C.IDSoggettoCommerciale) > @minIDSoggettoCommerciale THEN @minIDSoggettoCommerciale ELSE MIN(C.IDSoggettoCommerciale) END
    FROM Staging.Cliente C;

    INSERT INTO Staging.Cliente
    (
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
        TipoCliente,
        Agente,
        PKDataInizioContratto,
        PKDataFineContratto,
        PKDataDisdetta,
        MotivoDisdetta,
        PKGruppoAgenti,
        Cognome,
        Nome,
        Telefono,
        Cellulare,
        Fax,
        IsAbbonato,
        IDSoggettoCommerciale_migrazione,
        IDSoggettoCommerciale_migrazione_old,
        IDProvincia,
        CapoAreaDefault,
        AgenteDefault,
        HasRoleMySolutionDemo
    )
    SELECT
        -PKClienteNopCommerce AS IDSoggettoCommerciale,
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
        TipoCliente,
        Agente,
        PKDataInizioContratto,
        PKDataFineContratto,
        PKDataDisdetta,
        MotivoDisdetta,
        PKGruppoAgenti,
        Cognome,
        Nome,
        Telefono,
        Cellulare,
        Fax,
        IsAbbonato,
        IDSoggettoCommerciale_migrazione,
        IDSoggettoCommerciale_migrazione_old,
        IDProvincia,
        CapoAreaDefault,
        AgenteDefault,
        HasRoleMySolutionDemo

    FROM Staging.ClienteNopCommerceView;

    SELECT @minIDSoggettoCommerciale = CASE WHEN MIN(C.IDSoggettoCommerciale) > @minIDSoggettoCommerciale THEN @minIDSoggettoCommerciale ELSE MIN(C.IDSoggettoCommerciale) END
    FROM Staging.Cliente C;

    INSERT INTO Staging.Cliente
    (
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
        TipoCliente,
        Agente,
        PKDataInizioContratto,
        PKDataFineContratto,
        PKDataDisdetta,
        MotivoDisdetta,
        PKGruppoAgenti,
        Cognome,
        Nome,
        Telefono,
        Cellulare,
        Fax,
        IsAbbonato,
        IDSoggettoCommerciale_migrazione,
        IDSoggettoCommerciale_migrazione_old,
        IDProvincia,
        CapoAreaDefault,
        AgenteDefault,
        HasRoleMySolutionDemo
    )
    SELECT
        @minIDSoggettoCommerciale - PKClienteMySolution AS IDSoggettoCommerciale,
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
        TipoCliente,
        Agente,
        PKDataInizioContratto,
        PKDataFineContratto,
        PKDataDisdetta,
        MotivoDisdetta,
        PKGruppoAgenti,
        Cognome,
        Nome,
        Telefono,
        Cellulare,
        Fax,
        IsAbbonato,
        IDSoggettoCommerciale_migrazione,
        IDSoggettoCommerciale_migrazione_old,
        IDProvincia,
        CapoAreaDefault,
        AgenteDefault,
        HasRoleMySolutionDemo

    FROM Staging.ClienteMySolutionView;

    SELECT @minIDSoggettoCommerciale = CASE WHEN MIN(C.IDSoggettoCommerciale) > @minIDSoggettoCommerciale THEN @minIDSoggettoCommerciale ELSE MIN(C.IDSoggettoCommerciale) END
    FROM Staging.Cliente C;

    INSERT INTO Staging.Cliente
    (
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
        TipoCliente,
        Agente,
        PKDataInizioContratto,
        PKDataFineContratto,
        PKDataDisdetta,
        MotivoDisdetta,
        PKGruppoAgenti,
        Cognome,
        Nome,
        Telefono,
        Cellulare,
        Fax,
        IsAbbonato,
        IDSoggettoCommerciale_migrazione,
        IDSoggettoCommerciale_migrazione_old,
        IDProvincia,
        CapoAreaDefault,
        AgenteDefault,
        HasRoleMySolutionDemo
    )
    SELECT
        @minIDSoggettoCommerciale - PKClienteAccessi AS IDSoggettoCommerciale,
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
        TipoCliente,
        Agente,
        PKDataInizioContratto,
        PKDataFineContratto,
        PKDataDisdetta,
        MotivoDisdetta,
        PKGruppoAgenti,
        Cognome,
        Nome,
        Telefono,
        Cellulare,
        Fax,
        IsAbbonato,
        IDSoggettoCommerciale_migrazione,
        IDSoggettoCommerciale_migrazione_old,
        IDProvincia,
        CapoAreaDefault,
        AgenteDefault,
        HasRoleMySolutionDemo

    FROM Staging.ClienteAccessiView;

    UPDATE Staging.Cliente
    SET RagioneSociale = Email
    WHERE RagioneSociale = N'';

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

