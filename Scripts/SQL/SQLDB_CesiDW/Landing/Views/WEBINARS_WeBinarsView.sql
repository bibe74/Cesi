
/**
 * @table Landing.WEBINARS_WeBinars
 * @description 

 * @depends WEBINARS.WeBinars

SELECT TOP (100) * FROM WEBINARS.WeBinars;
*/

CREATE   VIEW Landing.WEBINARS_WeBinarsView
AS
WITH TableData
AS (
    SELECT
        Source,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            Source,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            VideoStartDate,
            VideoTitle,
            CourseTitle,
            CourseType,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        VideoStartDate,
        VideoTitle,
        CourseTitle,
        CourseType

    FROM WEBINARS.WeBinars
)
SELECT
    -- Chiavi
    TD.Source COLLATE DATABASE_DEFAULT AS Source,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.VideoStartDate,
    TD.VideoTitle COLLATE DATABASE_DEFAULT AS VideoTitle,
    TD.CourseTitle COLLATE DATABASE_DEFAULT AS CourseTitle,
    TD.CourseType COLLATE DATABASE_DEFAULT AS CourseType

FROM TableData TD;

GO

