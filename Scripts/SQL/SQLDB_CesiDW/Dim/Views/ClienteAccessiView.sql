CREATE   VIEW Dim.ClienteAccessiView
AS
WITH EmailCliente
AS (
    SELECT
        Email,
        PKCliente,
        IDSoggettoCommerciale,
        IsAbbonato,
        ROW_NUMBER() OVER (PARTITION BY Email ORDER BY CASE WHEN IsAbbonato = CAST(1 AS BIT) THEN 0 ELSE 1 END, IDSoggettoCommerciale DESC) AS rn
    
    FROM Dim.Cliente
    WHERE Email <> N''
),
TableData
AS (
    SELECT
        AC.Username,

        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            AC.Username,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            AC.IDSoggettoCommerciale,
            C.PKCliente,
            C.Email,
            C.IDAnagraficaCometa,
            C.HasAnagraficaCometa,
            C.HasAnagraficaNopCommerce,
            C.HasAnagraficaMySolution,
            C.ProvenienzaAnagrafica,
            C.CodiceCliente,
            C.TipoSoggettoCommerciale,
            C.RagioneSociale,
            C.CodiceFiscale,
            C.PartitaIVA,
            C.Indirizzo,
            C.CAP,
            C.Localita,
            C.Provincia,
            C.Regione,
            C.Macroregione,
            C.Nazione,
            C.Telefono,
            C.Cellulare,
            C.Fax,
            C.TipoCliente,
            C.Agente,
            C.PKDataInizioContratto,
            C.PKDataFineContratto,
            C.PKDataDisdetta,
            C.MotivoDisdetta,
            C.PKGruppoAgenti,
            C.Cognome,
            C.Nome,
            C.IsAttivo,
            C.IsAbbonato,
            C.IDSoggettoCommerciale_migrazione,
            C.IDSoggettoCommerciale_migrazione_old,
            C.IDProvincia,
            C.IsClienteFormazione,
            C.CapoAreaDefault,
            C.AgenteDefault,
            C.HasRoleMySolutionDemo,
            AC.IDMySolution,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,

        AC.IDSoggettoCommerciale,
        COALESCE(C.PKCliente, -101) AS PKCliente,
        COALESCE(C.Email, N'') AS Email,
        COALESCE(C.IDAnagraficaCometa, -1) AS IDAnagraficaCometa,
        COALESCE(C.HasAnagraficaCometa, 0) AS HasAnagraficaCometa,
        COALESCE(C.HasAnagraficaNopCommerce, 0) AS HasAnagraficaNopCommerce,
        COALESCE(C.HasAnagraficaMySolution, 0) AS HasAnagraficaMySolution,
        COALESCE(C.ProvenienzaAnagrafica, N'ACCESSI') AS ProvenienzaAnagrafica,
        COALESCE(C.CodiceCliente, N'') AS CodiceCliente,
        COALESCE(C.TipoSoggettoCommerciale, N'') AS TipoSoggettoCommerciale,
        COALESCE(C.RagioneSociale, N'') AS RagioneSociale,
        COALESCE(C.CodiceFiscale, N'') AS CodiceFiscale,
        COALESCE(C.PartitaIVA, N'') AS PartitaIVA,
        COALESCE(C.Indirizzo, N'') AS Indirizzo,
        COALESCE(C.CAP, N'') AS CAP,
        COALESCE(C.Localita, N'') AS Localita,
        COALESCE(C.Provincia, N'') AS Provincia,
        COALESCE(C.Regione, N'') AS Regione,
        COALESCE(C.Macroregione, N'') AS Macroregione,
        COALESCE(C.Nazione, N'') AS Nazione,
        COALESCE(C.Telefono, N'') AS Telefono,
        COALESCE(C.Cellulare, N'') AS Cellulare,
        COALESCE(C.Fax, N'') AS Fax,
        COALESCE(C.TipoCliente, N'') AS TipoCliente,
        COALESCE(C.Agente, N'') AS Agente,
        COALESCE(C.PKDataInizioContratto, CAST('19000101' AS DATE)) AS PKDataInizioContratto,
        COALESCE(C.PKDataFineContratto, CAST('19000101' AS DATE)) AS PKDataFineContratto,
        COALESCE(C.PKDataDisdetta, CAST('19000101' AS DATE)) AS PKDataDisdetta,
        COALESCE(C.MotivoDisdetta, N'') AS MotivoDisdetta,
        COALESCE(C.PKGruppoAgenti, -1) AS PKGruppoAgenti,
        COALESCE(C.Cognome, N'') AS Cognome,
        COALESCE(C.Nome, N'') AS Nome,
        COALESCE(C.IsAttivo, 0) AS IsAttivo,
        COALESCE(C.IsAbbonato, 0) AS IsAbbonato,
        COALESCE(C.IDSoggettoCommerciale_migrazione, -1) AS IDSoggettoCommerciale_migrazione,
        COALESCE(C.IDSoggettoCommerciale_migrazione_old, -1) AS IDSoggettoCommerciale_migrazione_old,
        COALESCE(C.IDProvincia, N'') AS IDProvincia,
        COALESCE(C.IsClienteFormazione, 0) AS IsClienteFormazione,
        COALESCE(C.CapoAreaDefault, N'') AS CapoAreaDefault,
        COALESCE(C.AgenteDefault, N'') AS AgenteDefault,
        COALESCE(C.HasRoleMySolutionDemo, 0) AS HasRoleMySolutionDemo,
        AC.IDMySolution

    FROM Staging.AccessiCustomer AC
    --LEFT JOIN Dim.Cliente C ON C.IDSoggettoCommerciale = AC.IDSoggettoCommerciale
    LEFT JOIN EmailCliente EC ON EC.Email = AC.Username
        AND EC.rn = 1
    LEFT JOIN Dim.Cliente C ON C.PKCliente = EC.PKCliente
)
SELECT
    TD.Username,

    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    TD.IDSoggettoCommerciale,
    TD.PKCliente,
    TD.Email,
    TD.IDAnagraficaCometa,
    TD.HasAnagraficaCometa,
    TD.HasAnagraficaNopCommerce,
    TD.HasAnagraficaMySolution,
    TD.ProvenienzaAnagrafica,
    TD.CodiceCliente,
    TD.TipoSoggettoCommerciale,
    TD.RagioneSociale,
    TD.CodiceFiscale,
    TD.PartitaIVA,
    TD.Indirizzo,
    TD.CAP,
    TD.Localita,
    TD.Provincia,
    TD.Regione,
    TD.Macroregione,
    TD.Nazione,
    TD.Telefono,
    TD.Cellulare,
    TD.Fax,
    TD.TipoCliente,
    TD.Agente,
    TD.PKDataInizioContratto,
    TD.PKDataFineContratto,
    TD.PKDataDisdetta,
    TD.MotivoDisdetta,
    TD.PKGruppoAgenti,
    TD.Cognome,
    TD.Nome,
    TD.IsAttivo,
    TD.IsAbbonato,
    TD.IDSoggettoCommerciale_migrazione,
    TD.IDSoggettoCommerciale_migrazione_old,
    TD.IDProvincia,
    TD.IsClienteFormazione,
    TD.CapoAreaDefault,
    TD.AgenteDefault,
    TD.HasRoleMySolutionDemo,
    TD.IDMySolution

FROM TableData TD;

GO

