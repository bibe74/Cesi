
CREATE   PROCEDURE Dim.usp_Merge_ClienteAccessi
AS
BEGIN

    SET NOCOUNT ON;

    BEGIN TRANSACTION 

    --DECLARE @provider_name NVARCHAR(60) = N'MyDatamartReporting';
    --DECLARE @full_table_name sysname = N'Landing.COMETA_SoggettoCommerciale';

    MERGE INTO Dim.ClienteAccessi AS TGT
    USING Dim.ClienteAccessiView (nolock) AS SRC
    ON SRC.Username = TGT.Username

    WHEN MATCHED AND (SRC.ChangeHashKeyASCII <> TGT.ChangeHashKeyASCII)
      THEN UPDATE SET
        TGT.ChangeHashKey = SRC.ChangeHashKey,
        TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII,
        --TGT.InsertDatetime = SRC.InsertDatetime,
        TGT.UpdateDatetime = SRC.UpdateDatetime,
        TGT.IsDeleted = SRC.IsDeleted,
        TGT.IDSoggettoCommerciale = SRC.IDSoggettoCommerciale,
        TGT.PKCliente = SRC.PKCliente,
        TGT.Email = SRC.Email,
        TGT.IDAnagraficaCometa = SRC.IDAnagraficaCometa,
        TGT.HasAnagraficaCometa = SRC.HasAnagraficaCometa,
        TGT.HasAnagraficaNopCommerce = SRC.HasAnagraficaNopCommerce,
        TGT.HasAnagraficaMySolution = SRC.HasAnagraficaMySolution,
        TGT.ProvenienzaAnagrafica = SRC.ProvenienzaAnagrafica,
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
        TGT.IDSoggettoCommerciale_migrazione = SRC.IDSoggettoCommerciale_migrazione,
        TGT.IDSoggettoCommerciale_migrazione_old = SRC.IDSoggettoCommerciale_migrazione_old,
        TGT.IDProvincia = SRC.IDProvincia,
        TGT.IsClienteFormazione = SRC.IsClienteFormazione,
        TGT.CapoAreaDefault = SRC.CapoAreaDefault,
        TGT.AgenteDefault = SRC.AgenteDefault,
        TGT.HasRoleMySolutionDemo = SRC.HasRoleMySolutionDemo,
        TGT.IDMySolution = SRC.IDMySolution

    WHEN NOT MATCHED
      THEN INSERT (
        Username,
        HistoricalHashKey,
        ChangeHashKey,
        HistoricalHashKeyASCII,
        ChangeHashKeyASCII,
        InsertDatetime,
        UpdateDatetime,
        IsDeleted,
        IDSoggettoCommerciale,
        PKCliente,
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
        IsAttivo,
        IsAbbonato,
        IDSoggettoCommerciale_migrazione,
        IDSoggettoCommerciale_migrazione_old,
        IDProvincia,
        IsClienteFormazione,
        CapoAreaDefault,
        AgenteDefault,
        HasRoleMySolutionDemo,
        IDMySolution
      )
      VALUES (
        SRC.Username,
        SRC.HistoricalHashKey,
        SRC.ChangeHashKey,
        SRC.HistoricalHashKeyASCII,
        SRC.ChangeHashKeyASCII,
        SRC.InsertDatetime,
        SRC.UpdateDatetime,
        SRC.IsDeleted,
        SRC.IDSoggettoCommerciale,
        SRC.PKCliente,
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
        SRC.IsAttivo,
        SRC.IsAbbonato,
        SRC.IDSoggettoCommerciale_migrazione,
        SRC.IDSoggettoCommerciale_migrazione_old,
        SRC.IDProvincia,
        SRC.IsClienteFormazione,
        SRC.CapoAreaDefault,
        SRC.AgenteDefault,
        SRC.HasRoleMySolutionDemo,
        SRC.IDMySolution
      )

    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        $action AS merge_action,
        'Dim.ClienteAccessi' AS full_olap_table_name,
        'Username = ' + CAST(COALESCE(inserted.Username, deleted.Username) AS NVARCHAR(1000)) AS primary_key_description
    INTO audit.merge_log_details;

    --UPDATE audit.tables
    --SET lastupdated_local = lastupdated_staging
    --WHERE provider_name = @provider_name
    --    AND full_table_name = @full_table_name;

    COMMIT TRANSACTION;

END;

GO

