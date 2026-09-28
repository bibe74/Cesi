
CREATE VIEW Landing.MYSOLUTION_CoursesView
AS
WITH TableData
AS (
    SELECT
        C.OrderItemId,
        C.Partecipant_Id,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            C.OrderItemId,
            C.Partecipant_Id,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            C.PartecipantFirstName,
            C.PartecipantLastName,
            C.PartecipantEmail,
            C.PartecipantFiscalCode,
            C.RootPartecipantEmail,
            C.CustomerUserName,
            C.CourseName,
            C.CourseType,
            C.StartDate_text,
            C.StartDate,
            C.OrderNumber,
            C.OrderCreatedDate,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        C.PartecipantFirstName,
        C.PartecipantLastName,
        C.PartecipantEmail,
        C.PartecipantFiscalCode,
        C.RootPartecipantEmail,
        C.CustomerUserName,
        C.CourseName,
        C.CourseType,
        C.StartDate_text,
        C.StartDate,
        C.OrderNumber,
        C.OrderCreatedDate

    FROM MYSOLUTION.Courses C
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

    -- Attributi
    TD.PartecipantFirstName,
    TD.PartecipantLastName,
    TD.PartecipantEmail,
    TD.PartecipantFiscalCode,
    TD.RootPartecipantEmail,
    TD.CustomerUserName,
    TD.CourseName,
    TD.CourseType,
    TD.StartDate_text,
    TD.StartDate,
    TD.OrderNumber,
    TD.OrderCreatedDate

FROM TableData TD;

GO

