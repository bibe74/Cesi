CREATE   PROCEDURE Fact.usp_ReportDomandeMIA (
    @StartDate DATE NULL,
    @EndDate DATE NULL
)
AS
BEGIN

    IF (@StartDate IS NULL OR @EndDate IS NULL)
    BEGIN
    
        SELECT @EndDate = DATEADD(DAY, -1, CONVERT(DATE, CURRENT_TIMESTAMP));
        SELECT @StartDate = DATEADD(DAY, -7, @EndDate);
    
    END;

    SELECT
        --PKDomandeMIA,
        --Id,
        --HistoricalHashKey,
        --ChangeHashKey,
        --HistoricalHashKeyASCII,
        --ChangeHashKeyASCII,
        --InsertDatetime,
        --UpdateDatetime,
        --IsDeleted,
        PKDataCreazione,
        Testo,
        IsDomanda,
        Area,
        Email

    FROM Fact.DomandeMIA
    WHERE PKDataCreazione BETWEEN @StartDate AND @EndDate
        AND IsDeleted = CAST(0 AS BIT)
    ORDER BY Id DESC;

END;

GO

