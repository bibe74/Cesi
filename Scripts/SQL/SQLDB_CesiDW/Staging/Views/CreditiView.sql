CREATE   VIEW Staging.CreditiView
AS
WITH TableData
AS (
    SELECT
        CA.ID,

        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            CA.ID,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            A.Corso,
            W.PKCorso,
            A.Nome,
            A.Cognome,
            A.CodiceFiscale,
            A.Professione,
            A.Ordine,
            A.DataCreazione,
            DC.PKData,
            CT.Ordine,
            CT.Tipo,
            DTC.TipoCrediti,
            DSC.StatoCrediti,
            CC.CodiceMateria,
            CA.Crediti,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,

        --CA.AutocertificazioneID,
        --A.ID,
        A.Corso AS IDCorso,
        COALESCE(W.PKCorso, CASE WHEN A.Corso = N'' THEN -1 ELSE -101 END) AS PKCorso,
        UPPER(A.Nome) AS Nome,
        UPPER(A.Cognome) AS Cognome,
        UPPER(A.CodiceFiscale) AS CodiceFiscale,
        COALESCE(A.Professione, N'') AS Professione,
        COALESCE(A.Ordine, N'') AS Ordine,
        --A.DataCreazione,
        COALESCE(DC.PKData, CAST('19000101' AS DATE)) AS PKDataCreazione,
        LOWER(A.Email) AS EMail,

        --CA.CreditoTipologiaID,
        --CT.ID,
        CT.Ordine AS EnteAccreditante,
        --CT.Tipo AS TipoCrediti,
        COALESCE(DTC.TipoCrediti, N'<???>') AS TipoCrediti,

        --CA.Stato,
        COALESCE(DSC.StatoCrediti, N'<???>') AS StatoCrediti,

        COALESCE(CC.CodiceMateria, N'') AS CodiceMateria,

        CA.Crediti

    FROM Landing.WEBINARS_CreditoAutocertificazione CA
    INNER JOIN Landing.WEBINARS_WeAutocertificazioni A ON A.ID = CA.AutocertificazioneID
        AND A.IsDeleted = CAST(0 AS BIT)
    INNER JOIN Landing.WEBINARS_CreditoTipologia CT ON CT.ID = CA.CreditoTipologiaID
        AND CT.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Landing.WEBINARS_CreditoCorso CC ON CC.Id = CA.CreditoCorsoID
        AND CC.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Dim.Corso W ON W.IDCorso = A.Corso
    LEFT JOIN Dim.Data DC ON DC.PKData = A.DataCreazione
    LEFT JOIN Import.Decod_StatoCrediti DSC ON DSC.IDStatoCrediti = CA.Stato
    LEFT JOIN Import.Decod_TipoCrediti DTC ON DTC.IDTipoCrediti = CT.Tipo
    WHERE CA.IsDeleted = CAST(0 AS BIT)
)
SELECT
    -- Chiavi
    TD.ID,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.IDCorso,
    TD.PKCorso,
    TD.Nome,
    TD.Cognome,
    TD.CodiceFiscale,
    TD.Professione,
    TD.Ordine,
    TD.PKDataCreazione,
    TD.Email,
    TD.EnteAccreditante,
    TD.TipoCrediti,
    TD.StatoCrediti,
    TD.CodiceMateria,

    -- Misure
    TD.Crediti

FROM TableData TD;

GO

