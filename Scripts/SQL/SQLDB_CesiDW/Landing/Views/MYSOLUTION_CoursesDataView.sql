
CREATE VIEW Landing.MYSOLUTION_CoursesDataView
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
            C.CourseCode,
            C.AttCourseCode,
            C.WebinarCode,
            C.AttWebinarCode,
            C.CourseType,
            C.StartDate_text,
            C.StartDate,
            C.HasMoreDates,
            C.OrderDescription,
            C.ItemNetUnitPrice,
            C.OrderTotalPrice,
            C.OrderNumber,
            C.OrderStatus,
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
        C.CourseCode,
        C.AttCourseCode,
        C.WebinarCode,
        C.AttWebinarCode,
        C.CourseType,
        C.StartDate_text,
        C.StartDate,
        C.HasMoreDates,
        C.OrderDescription,
        C.ItemNetUnitPrice,
        C.OrderTotalPrice,
        C.OrderNumber,
        C.OrderStatus,
        CAST(C.OrderCreatedDate AS DATE) AS OrderCreatedDate

    FROM MYSOLUTIONPRODUZIONE2.Nop_MySolution.dbo.VW_MySolution_Courses C
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
    TD.CourseCode,
    TD.AttCourseCode,
    TD.WebinarCode,
    TD.AttWebinarCode,
    TD.CourseType,
    TD.StartDate_text,
    TD.StartDate,
    TD.HasMoreDates,
    TD.OrderDescription,
    TD.ItemNetUnitPrice,
    TD.OrderTotalPrice,
    TD.OrderNumber,
    TD.OrderStatus,
    TD.OrderCreatedDate

FROM TableData TD;

GO

