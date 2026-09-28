CREATE VIEW Staging.CorsiView
AS
WITH TableData
AS (
    SELECT
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            T.OrderItemId,
            T.Partecipant_Id,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            T.PartecipantFirstName,
            T.PartecipantLastName,
            T.PartecipantEmail,
            T.PartecipantFiscalCode,
            T.RootPartecipantEmail,
            T.CustomerUserName,
            U.IDUtente,
            T.CustomerUserName,
            T.CourseName,
            T.CourseCode,
            T.AttCourseCode,
            T.WebinarCode,
            T.AttWebinarCode,
            T.CourseType,
            T.StartDate_text,
            T.StartDate,
            D.PKData,
            T.HasMoreDates,
            T.OrderDescription,
            T.ItemNetUnitPrice,
            T.OrderTotalPrice,
            T.OrderNumber,
            T.OrderStatus,
            DI.PKData,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        T.OrderItemId,
        T.Partecipant_Id,
        T.PartecipantFirstName AS NomePartecipante,
        T.PartecipantLastName AS CognomePartecipante,
        T.PartecipantEmail AS EmailPartecipante,
        T.PartecipantFiscalCode AS CodiceFiscalePartecipante,
        T.RootPartecipantEmail AS EmailPartecipanteRoot,
        T.CustomerUserName AS Utente,
        COALESCE(U.PKUtente, CASE WHEN COALESCE(T.CustomerUserName, N'') = N'' THEN -1 ELSE -101 END) AS PKUtente,
        T.CourseName AS Corso,
        --T.CourseCode AS IDCorso,
        --T.AttCourseCode,
        COALESCE(T.CourseCode, T.AttCourseCode) AS IDCorso,
        --T.WebinarCode AS IDWebinar,
        --T.AttWebinarCode,
        COALESCE(T.WebinarCode, T.AttWebinarCode) AS IDWebinar,
        T.CourseType AS TipoCorso,
        T.StartDate_text,
        T.StartDate,
        COALESCE(D.PKData, CAST('19000101' AS DATE)) AS PKDataInizio,
        T.HasMoreDates AS HasDateMultiple,
        T.OrderDescription AS DescrizioneOrdine,
        T.ItemNetUnitPrice AS PrezzoUnitarioOrdine,
        T.OrderTotalPrice AS ImportoTotaleOrdine,
        T.OrderNumber AS NumeroOrdine,
        T.OrderStatus AS StatoOrdine,
        T.OrderCreatedDate,
        COALESCE(DI.PKData, CAST('19000101' AS DATE)) AS PKDataIscrizione,
        ROW_NUMBER() OVER (PARTITION BY T.OrderItemId, T.Partecipant_Id ORDER BY U.PKUtente) AS rn

    FROM Landing.MYSOLUTION_CoursesData T
    LEFT JOIN Dim.Utente U ON U.Email = T.CustomerUserName
        AND U.IsDeleted = CAST(0 AS BIT)
        AND U.Email <> N''
    LEFT JOIN Dim.Data D ON D.PKData = T.StartDate
    LEFT JOIN Dim.Data DI ON DI.PKData = T.OrderCreatedDate
    WHERE T.IsDeleted = CAST(0 AS BIT)
)
SELECT
    -- Chiavi
    TD.OrderItemId,
    TD.Partecipant_Id,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Altri campi
    TD.NomePartecipante,
    TD.CognomePartecipante,
    TD.EmailPartecipante,
    TD.CodiceFiscalePartecipante,
    TD.EmailPartecipanteRoot,
    TD.Utente,
    TD.PKUtente,
    TD.Corso,
    TD.IDCorso,
    TD.IDWebinar,
    LEFT(TD.TipoCorso, 120) AS TipoCorso,
    --TD.StartDate_text,
    --TD.StartDate,
    TD.PKDataInizio,
    TD.HasDateMultiple,
    TD.DescrizioneOrdine,
    TD.PrezzoUnitarioOrdine,
    TD.ImportoTotaleOrdine,
    TD.NumeroOrdine,
    TD.StatoOrdine,
    TD.PKDataIscrizione

FROM TableData TD
WHERE TD.rn = 1;

GO

