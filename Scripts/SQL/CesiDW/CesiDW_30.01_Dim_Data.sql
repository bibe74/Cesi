USE CesiDW;
GO

/*
SET NOEXEC OFF;
--*/ SET NOEXEC ON;
GO

--DROP TABLE IF EXISTS Fact.Documenti; DROP TABLE IF EXISTS Fact.Accessi; DROP TABLE IF EXISTS Dim.Cliente; DROP TABLE IF EXISTS Dim.Data;
GO

IF OBJECT_ID('Dim.Data', 'U') IS NULL
BEGIN

    CREATE TABLE Dim.Data (
        PKData DATE NOT NULL CONSTRAINT PK_Dim_Data PRIMARY KEY CLUSTERED,
        Data_IT VARCHAR(10) NOT NULL,
        Anno INT NOT NULL,
        Mese INT NOT NULL,
        Mese_IT VARCHAR(10) NOT NULL,
        AnnoMese INT NOT NULL,
        AnnoMese_IT VARCHAR(20) NOT NULL,
        Settimana INT NOT NULL,
        AnnoSettimana INT NOT NULL,
        AnnoSettimana_IT VARCHAR(20) NOT NULL,
        SettimanaDescrizione NVARCHAR(24) NOT NULL
    );

    INSERT INTO Dim.Data (
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
    VALUES
    (   CAST('19000101' AS DATE), -- PKData - date
        '',        -- Data_IT - varchar(10)
        1900,         -- Anno - int
        0,         -- Mese - int
        '',        -- Mese_IT - varchar(10)
        190000,         -- AnnoMese - int
        '',        -- AnnoMese_IT - varchar(20)
        0,         -- Settimana - int
        190000,         -- AnnoSettimana - int
        '',        -- AnnoSettimana_IT - varchar(20)
        N''        -- SettimanaDescrizione - nvarchar(24)
    );

    ALTER TABLE Dim.Data ADD IsOrdinazioneChiusa BIT NOT NULL CONSTRAINT DFT_Dim_Data_IsOrdinazioneChiusa DEFAULT (0);
    ALTER TABLE Dim.Data ADD IsOrdinazioneMensileChiusa BIT NOT NULL CONSTRAINT DFT_Dim_Data_IsOrdinazioneMensileChiusa DEFAULT (0);

END;
GO

IF OBJECT_ID('setup.usp_InsertDates', 'P') IS NULL EXEC('CREATE PROCEDURE setup.usp_InsertDates AS RETURN 0;');
GO

ALTER PROCEDURE setup.usp_InsertDates (
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

-- Valorizzazione iniziale (2008-2028).
-- L'elaborazione notturna aggiungerà comunque, come prima query, le date dell'intero anno corrente (il 1° gennaio 2022 apparirà tutto il 2022)
EXEC setup.usp_InsertDates
    @StartDate = '2008-01-01', -- date
    @EndDate = '2028-12-31';   -- date
GO

-- Correzione giorni della settimana
SET DATEFIRST 1;
SELECT
    PKData,
    Settimana,
    DATEPART(WW, PKData) AS Settimana_New,
    AnnoSettimana,
    Anno * 100 + DATEPART(WW, PKData) AS AnnoSettimana_New,
    AnnoSettimana_IT,
    CONVERT(NVARCHAR(2), DATEPART(WW, PKData)) + N'/' + CONVERT(NVARCHAR(4), Anno) AS AnnoSettimana_IT_New

FROM Dim.Data
ORDER BY PKData;
GO

SET DATEFIRST 1;
UPDATE Dim.Data
SET Settimana = DATEPART(WW, PKData),
    AnnoSettimana = Anno * 100 + DATEPART(WW, PKData),
    AnnoSettimana_IT = CONVERT(NVARCHAR(2), DATEPART(WW, PKData)) + N'/' + CONVERT(NVARCHAR(4), Anno)

FROM Dim.Data;
GO

/**
 * @stored_procedure Impostazioni.usp_ChiusuraOrdinazione
 * @description Impostazione flag di chiusura ordinazione:
    - IsOrdinazioneChiusa: vale 1 per tutti i mesi "terminati"
    - IsOrdinazioneMensileChiusa: utilizzare per i confronti YTD
*/

CREATE OR ALTER PROCEDURE Dim.usp_ChiusuraOrdinazione (
    @DataOraEsecuzione DATETIME = NULL
)
AS
BEGIN

    SET NOCOUNT ON;

    IF @DataOraEsecuzione IS NULL SET @DataOraEsecuzione = CURRENT_TIMESTAMP;

    UPDATE Dim.Data SET IsOrdinazioneMensileChiusa = CAST(0 AS BIT), IsOrdinazioneChiusa = CAST(0 AS BIT);

    UPDATE Dim.Data
    SET IsOrdinazioneChiusa = CAST(1 AS BIT)
    WHERE Anno < DATEPART(YEAR, DATEADD(DAY, -DATEPART(DAY, @DataOraEsecuzione), @DataOraEsecuzione))
	    OR (
		    Anno = DATEPART(YEAR, DATEADD(DAY, -DATEPART(DAY, @DataOraEsecuzione), @DataOraEsecuzione))
		    AND Mese <= DATEPART(MONTH, DATEADD(DAY, -DATEPART(DAY, @DataOraEsecuzione), @DataOraEsecuzione))
	    );

    UPDATE Dim.Data
    SET IsOrdinazioneMensileChiusa = CAST(1 AS BIT)
    WHERE Anno <= DATEPART(YEAR, DATEADD(DAY, -DATEPART(DAY, @DataOraEsecuzione), @DataOraEsecuzione))
	    AND Mese <= DATEPART(MONTH, DATEADD(DAY, -DATEPART(DAY, @DataOraEsecuzione), @DataOraEsecuzione));

END;
GO

EXEC Dim.usp_ChiusuraOrdinazione;
GO
