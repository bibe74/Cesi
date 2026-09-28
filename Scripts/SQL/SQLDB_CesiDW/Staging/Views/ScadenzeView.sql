CREATE VIEW Staging.ScadenzeView
AS
WITH Documenti
AS (
    SELECT
        D.IDDocumento,
        MIN(D.PKDocumenti) AS PKDocumenti

    FROM Fact.Documenti D
    WHERE D.IsDeleted = CAST(0 AS BIT)
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
        AND C.IsDeleted = CAST(0 AS BIT)
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

