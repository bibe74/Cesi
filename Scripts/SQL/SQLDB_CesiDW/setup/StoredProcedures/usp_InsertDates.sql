
CREATE PROCEDURE setup.usp_InsertDates (
    @StartDate DATE = NULL,
    @EndDate DATE = NULL
)
AS
BEGIN

    SET NOCOUNT ON;

    IF @StartDate IS NULL
    BEGIN

        SELECT @StartDate = MIN(PKData)
        FROM Dim.Data
        WHERE Anno > 1900;

        SELECT @StartDate = COALESCE(@StartDate, DATEADD(DAY, 1-DATEPART(DAYOFYEAR, CURRENT_TIMESTAMP), CAST(CURRENT_TIMESTAMP AS DATE)));

    END;

    SELECT @EndDate = COALESCE(@EndDate, DATEADD(YEAR, 1, DATEADD(DAY, -DATEPART(DAYOFYEAR, CURRENT_TIMESTAMP), CAST(CURRENT_TIMESTAMP AS DATE))));

    WITH DateDaImportare
    AS (
        SELECT
            Date AS PKData,
            FullDateIT AS Data_IT,
            YEAR(Date) AS Anno,
            CAST(Month AS TINYINT) AS Mese,
            MonthNameIT AS Mese_IT,
            YEAR(Date) * 100 + MONTH(Date) AS AnnoMese,
            MonthNameIT + ' ' + Year AS AnnoMese_IT,
            CAST(WeekOfYear AS TINYINT) AS Settimana,
            YEAR(Date) * 100 + CAST(WeekOfYear AS INT) AS AnnoSettimana,
            WeekOfYear + '/' + Year AS AnnoSettimana_IT,
            N'' AS SettimanaDescrizione

        FROM Import.Dates
        WHERE Date BETWEEN @StartDate AND @EndDate
    )
    MERGE INTO Dim.Data AS DST
    USING DateDaImportare AS SRC
    ON SRC.PKData = DST.PKData
    WHEN NOT MATCHED THEN INSERT (
        PKData,
        Data_IT,
        Anno,
        Mese,
        Mese_IT,
        AnnoMese,
        AnnoMese_IT,
        Settimana,
        AnnoSettimana,
        AnnoSettimana_IT,
        SettimanaDescrizione
    )
    VALUES (
        SRC.PKData,
        SRC.Data_IT,
        SRC.Anno,
        SRC.Mese,
        SRC.Mese_IT,
        SRC.AnnoMese,
        SRC.AnnoMese_IT,
        SRC.Settimana,
        SRC.AnnoSettimana,
        SRC.AnnoSettimana_IT,
        SRC.SettimanaDescrizione
    )
    OUTPUT $action, Inserted.PKData;

    WITH Settimane
    AS (
        SELECT
            AnnoSettimana,
            MIN(PKData) AS PKDataLunedi,
            MAX(PKData) AS PKDataDomenica
        FROM Dim.Data
        GROUP BY AnnoSettimana
    )
    UPDATE D
    SET SettimanaDescrizione = CONVERT(NVARCHAR(10), S.PKDataLunedi, 103) + N' - ' + CONVERT(NVARCHAR(10), S.PKDataDomenica, 103)
    FROM Dim.Data D
    INNER JOIN Settimane S ON S.AnnoSettimana = D.AnnoSettimana
    WHERE D.PKData > CAST('19000101' AS DATE);

END;

GO

