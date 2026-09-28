CREATE VIEW Staging.MySolutionUsersView
AS
WITH MySolutionUsersDetail
AS (
    SELECT
        MSU.EMail,
        ROW_NUMBER() OVER (PARTITION BY MSU.EMail ORDER BY MSU.data_inizio_contratto DESC) AS rnDataInizioContrattoDESC,
        ROW_NUMBER() OVER (PARTITION BY MSU.EMail, MSU.codice ORDER BY MSU.data_inizio_contratto DESC) AS rnCodiceDataInizioContrattoDESC,

        COALESCE(MSU.codice, N'') AS CodiceCliente,
        COALESCE(MSU.RagioneSociale, N'') AS RagioneSociale,
        COALESCE(MSU.cod_fiscale, N'') AS CodiceFiscale,
        COALESCE(MSU.par_iva, N'') AS PartitaIVA,
        COALESCE(MSU.localita, N'') AS Localita,
        COALESCE(MSU.provincia, N'') AS Provincia,
        COALESCE(T.num_riferimento, N'') AS Telefono,
        COALESCE(MSU.tipo, N'') AS TipoCliente,
        --MSU.data_inizio_contratto,
        COALESCE(DIC.PKData, CAST('19000101' AS DATE)) AS PKDataInizioContratto,
        --MSU.data_fine_contratto,
        COALESCE(DFC.PKData, CAST('19000101' AS DATE)) AS PKDataFineContratto,
        COALESCE(MSU.Cognome, N'') AS Cognome,
        COALESCE(MSU.Nome, N'') AS Nome,
        COALESCE(MSU.id_documento, -1) AS IDDocumento

    FROM Landing.COMETA_MySolutionUsers MSU
    LEFT JOIN Dim.Data DIC ON DIC.PKData = MSU.data_inizio_contratto
    LEFT JOIN Dim.Data DFC ON DFC.PKData = MSU.data_fine_contratto
    LEFT JOIN Landing.COMETA_Anagrafica A ON A.id_anagrafica = MSU.id_anagrafica
    LEFT JOIN Landing.COMETA_Telefono T ON T.id_telefono = MSU.id_telefono
        AND T.IsDeleted = CAST(0 AS BIT)
),
TableData
AS (
    SELECT
        MSUD.Email,
        MSUD.rnDataInizioContrattoDESC,

        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            MSUD.Email,
            MSUD.rnDataInizioContrattoDESC,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            MSUD.rnDataInizioContrattoDESC,
            MSUD.CodiceCliente,
            MSUD.RagioneSociale,
            MSUD.CodiceFiscale,
            MSUD.PartitaIVA,
            MSUD.Localita,
            MSUD.Provincia,
            MSUD.Telefono,
            MSUD.TipoCliente,
            MSUD.PKDataInizioContratto,
            MSUD.PKDataFineContratto,
            MSUD.Cognome,
            MSUD.Nome,
            MSUD.IDDocumento,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,

        MSUD.rnCodiceDataInizioContrattoDESC,
        MSUD.CodiceCliente,
        MSUD.RagioneSociale,
        MSUD.CodiceFiscale,
        MSUD.PartitaIVA,
        MSUD.Localita,
        MSUD.Provincia,
        MSUD.Telefono,
        MSUD.TipoCliente,
        MSUD.PKDataInizioContratto,
        MSUD.PKDataFineContratto,
        MSUD.Cognome,
        MSUD.Nome,
        MSUD.IDDocumento

    FROM MySolutionUsersDetail MSUD
)
SELECT
    -- Chiavi
    TD.Email,
    TD.rnDataInizioContrattoDESC,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Altri campi
    TD.rnCodiceDataInizioContrattoDESC,
    TD.CodiceCliente,
    TD.RagioneSociale,
    TD.CodiceFiscale,
    TD.PartitaIVA,
    TD.Localita,
    TD.Provincia,
    TD.Telefono,
    TD.TipoCliente,
    TD.PKDataInizioContratto,
    TD.PKDataFineContratto,
    TD.Cognome,
    TD.Nome,
    TD.IDDocumento

FROM TableData TD;

GO

