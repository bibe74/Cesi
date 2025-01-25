USE CesiDW;
GO

/*
SET NOEXEC OFF;
--*/ SET NOEXEC ON;
GO

SET LANGUAGE N'English';
GO

/**
 @table Import.Dates
*/

BEGIN TRY
	DROP TABLE Import.Dates
END TRY

BEGIN CATCH
	/*No Action*/
END CATCH

/**********************************************************************************/
CREATE TABLE Import.Dates (
    DateKey INT CONSTRAINT PK_Import_Dates PRIMARY KEY CLUSTERED,
    Date DATE,
    FullDateUK CHAR(10),         -- Date in dd-MM-yyyy format
    FullDateUSA CHAR(10),        -- Date in MM-dd-yyyy format
    DayOfMonth VARCHAR(2),       -- Field will hold day number of Month
    DaySuffix VARCHAR(4),        -- Apply suffix as 1st, 2nd ,3rd etc
    DayName VARCHAR(9),          -- Contains name of the day, Sunday, Monday 
    DayOfWeekUSA CHAR(1),        -- First Day Sunday=1 and Saturday=7
    DayOfWeekUK CHAR(1),         -- First Day Monday=1 and Sunday=7
    DayOfWeekInMonth VARCHAR(2), --1st Monday or 2nd Monday in Month
    DayOfWeekInYear VARCHAR(2),
    DayOfQuarter VARCHAR(3),
    DayOfYear VARCHAR(3),
    WeekOfMonth VARCHAR(1),      -- Week Number of Month 
    WeekOfQuarter VARCHAR(2),    --Week Number of the Quarter
    WeekOfYear VARCHAR(2),       --Week Number of the Year
    Month VARCHAR(2),            --Number of the Month 1 to 12
    MonthName VARCHAR(9),        --January, February etc
    MonthOfQuarter VARCHAR(2),   -- Month Number belongs to Quarter
    Quarter CHAR(1),
    QuarterName VARCHAR(9),      --First,Second..
    Year CHAR(4),                -- Year value of Date stored in Row
    YearName CHAR(7),            --CY 2012,CY 2013
    MonthYear CHAR(10),          --Jan-2013,Feb-2013
    MMYYYY CHAR(6),
    FirstDayOfMonth DATE,
    LastDayOfMonth DATE,
    FirstDayOfQuarter DATE,
    LastDayOfQuarter DATE,
    FirstDayOfYear DATE,
    LastDayOfYear DATE,
    IsHolidayUSA BIT,            -- Flag 1=National Holiday, 0-No National Holiday
    IsWeekday BIT,               -- 0=Week End ,1=Week Day
    HolidayUSA VARCHAR(50),      --Name of Holiday in US
    IsHolidayUK BIT NULL,        -- Flag 1=National Holiday, 0-No National Holiday
    HolidayUK VARCHAR(50) NULL,  --Name of Holiday in UK
    DayNameIT VARCHAR(9),
    MonthNameIT VARCHAR(9),
    IsHolidayIT BIT NULL,        -- Flag 1=National Holiday, 0-No National Holiday
    HolidayIT VARCHAR(50) NULL,  --Name of Holiday in IT
    FullDateIT CHAR(10)          -- Date in dd-MM-yyyy format
);
GO

/********************************************************************************************/
--Specify Start Date and End date here
--Value of Start Date Must be Less than Your End Date 

DECLARE @StartDate DATE = '01/01/1980' --Starting value of Date Range
DECLARE @EndDate DATE = '01/01/2031' --End Value of Date Range

--Temporary Variables To Hold the Values During Processing of Each Date of Year
DECLARE
	@DayOfWeekInMonth INT,
	@DayOfWeekInYear INT,
	@DayOfQuarter INT,
	@WeekOfMonth INT,
	@CurrentYear INT,
	@CurrentMonth INT,
	@CurrentQuarter INT

/*Table Data type to store the day of week count for the month and year*/
DECLARE @DayOfWeek TABLE (DOW INT, MonthCount INT, QuarterCount INT, YearCount INT)

INSERT INTO @DayOfWeek VALUES (1, 0, 0, 0)
INSERT INTO @DayOfWeek VALUES (2, 0, 0, 0)
INSERT INTO @DayOfWeek VALUES (3, 0, 0, 0)
INSERT INTO @DayOfWeek VALUES (4, 0, 0, 0)
INSERT INTO @DayOfWeek VALUES (5, 0, 0, 0)
INSERT INTO @DayOfWeek VALUES (6, 0, 0, 0)
INSERT INTO @DayOfWeek VALUES (7, 0, 0, 0)

--Extract and assign various parts of Values from Current Date to Variable

DECLARE @CurrentDate AS DATETIME = @StartDate
SET @CurrentMonth = DATEPART(MM, @CurrentDate)
SET @CurrentYear = DATEPART(YY, @CurrentDate)
SET @CurrentQuarter = DATEPART(QQ, @CurrentDate)

/********************************************************************************************/
--Proceed only if Start Date(Current date ) is less than End date you specified above

WHILE @CurrentDate < @EndDate
BEGIN
 
/*Begin day of week logic*/

         /*Check for Change in Month of the Current date if Month changed then 
          Change variable value*/
	IF @CurrentMonth != DATEPART(MM, @CurrentDate) 
	BEGIN
		UPDATE @DayOfWeek
		SET MonthCount = 0
		SET @CurrentMonth = DATEPART(MM, @CurrentDate)
	END

        /* Check for Change in Quarter of the Current date if Quarter changed then change 
         Variable value*/

	IF @CurrentQuarter != DATEPART(QQ, @CurrentDate)
	BEGIN
		UPDATE @DayOfWeek
		SET QuarterCount = 0
		SET @CurrentQuarter = DATEPART(QQ, @CurrentDate)
	END
       
        /* Check for Change in Year of the Current date if Year changed then change 
         Variable value*/
	

	IF @CurrentYear != DATEPART(YY, @CurrentDate)
	BEGIN
		UPDATE @DayOfWeek
		SET YearCount = 0
		SET @CurrentYear = DATEPART(YY, @CurrentDate)
	END
	
        -- Set values in table data type created above from variables 

	UPDATE @DayOfWeek
	SET 
		MonthCount = MonthCount + 1,
		QuarterCount = QuarterCount + 1,
		YearCount = YearCount + 1
	WHERE DOW = DATEPART(DW, @CurrentDate)

	SELECT
		@DayOfWeekInMonth = MonthCount,
		@DayOfQuarter = QuarterCount,
		@DayOfWeekInYear = YearCount
	FROM @DayOfWeek
	WHERE DOW = DATEPART(DW, @CurrentDate)
	
/*End day of week logic*/


/* Populate Your Dimension Table with values*/
	
	INSERT INTO Import.Dates
	SELECT
		
		CONVERT (char(8),@CurrentDate,112) as DateKey,
		@CurrentDate AS Date,
		CONVERT (char(10),@CurrentDate,103) as FullDateUK,
		CONVERT (char(10),@CurrentDate,101) as FullDateUSA,
		DATEPART(DD, @CurrentDate) AS DayOfMonth,
		--Apply Suffix values like 1st, 2nd 3rd etc..
		CASE 
			WHEN DATEPART(DD,@CurrentDate) IN (11,12,13) 
			THEN CAST(DATEPART(DD,@CurrentDate) AS VARCHAR) + 'th'
			WHEN RIGHT(DATEPART(DD,@CurrentDate),1) = 1 
			THEN CAST(DATEPART(DD,@CurrentDate) AS VARCHAR) + 'st'
			WHEN RIGHT(DATEPART(DD,@CurrentDate),1) = 2 
			THEN CAST(DATEPART(DD,@CurrentDate) AS VARCHAR) + 'nd'
			WHEN RIGHT(DATEPART(DD,@CurrentDate),1) = 3 
			THEN CAST(DATEPART(DD,@CurrentDate) AS VARCHAR) + 'rd'
			ELSE CAST(DATEPART(DD,@CurrentDate) AS VARCHAR) + 'th' 
			END AS DaySuffix,
		
		DATENAME(DW, @CurrentDate) AS DayName,
		DATEPART(DW, @CurrentDate) AS DayOfWeekUSA,

		-- check for day of week as Per US and change it as per UK format 
		CASE DATEPART(DW, @CurrentDate)
			WHEN 1 THEN 7
			WHEN 2 THEN 1
			WHEN 3 THEN 2
			WHEN 4 THEN 3
			WHEN 5 THEN 4
			WHEN 6 THEN 5
			WHEN 7 THEN 6
			END 
			AS DayOfWeekUK,
		
		@DayOfWeekInMonth AS DayOfWeekInMonth,
		@DayOfWeekInYear AS DayOfWeekInYear,
		@DayOfQuarter AS DayOfQuarter,
		DATEPART(DY, @CurrentDate) AS DayOfYear,
		DATEPART(WW, @CurrentDate) + 1 - DATEPART(WW, CONVERT(VARCHAR, 
		DATEPART(MM, @CurrentDate)) + '/1/' + CONVERT(VARCHAR, 
		DATEPART(YY, @CurrentDate))) AS WeekOfMonth,
		(DATEDIFF(DD, DATEADD(QQ, DATEDIFF(QQ, 0, @CurrentDate), 0), 
		@CurrentDate) / 7) + 1 AS WeekOfQuarter,
		DATEPART(WW, @CurrentDate) AS WeekOfYear,
		DATEPART(MM, @CurrentDate) AS Month,
		DATENAME(MM, @CurrentDate) AS MonthName,
		CASE
			WHEN DATEPART(MM, @CurrentDate) IN (1, 4, 7, 10) THEN 1
			WHEN DATEPART(MM, @CurrentDate) IN (2, 5, 8, 11) THEN 2
			WHEN DATEPART(MM, @CurrentDate) IN (3, 6, 9, 12) THEN 3
			END AS MonthOfQuarter,
		DATEPART(QQ, @CurrentDate) AS Quarter,
		CASE DATEPART(QQ, @CurrentDate)
			WHEN 1 THEN 'First'
			WHEN 2 THEN 'Second'
			WHEN 3 THEN 'Third'
			WHEN 4 THEN 'Fourth'
			END AS QuarterName,
		DATEPART(YEAR, @CurrentDate) AS Year,
		'CY ' + CONVERT(VARCHAR, DATEPART(YEAR, @CurrentDate)) AS YearName,
		LEFT(DATENAME(MM, @CurrentDate), 3) + '-' + CONVERT(VARCHAR, 
		DATEPART(YY, @CurrentDate)) AS MonthYear,
		RIGHT('0' + CONVERT(VARCHAR, DATEPART(MM, @CurrentDate)),2) + 
		CONVERT(VARCHAR, DATEPART(YY, @CurrentDate)) AS MMYYYY,
		CONVERT(DATETIME, CONVERT(DATE, DATEADD(DD, - (DATEPART(DD, 
		@CurrentDate) - 1), @CurrentDate))) AS FirstDayOfMonth,
		CONVERT(DATETIME, CONVERT(DATE, DATEADD(DD, - (DATEPART(DD, 
		(DATEADD(MM, 1, @CurrentDate)))), DATEADD(MM, 1, 
		@CurrentDate)))) AS LastDayOfMonth,
		DATEADD(QQ, DATEDIFF(QQ, 0, @CurrentDate), 0) AS FirstDayOfQuarter,
		DATEADD(QQ, DATEDIFF(QQ, -1, @CurrentDate), -1) AS LastDayOfQuarter,
		CONVERT(DATETIME, '01/01/' + CONVERT(VARCHAR, DATEPART(YY, 
		@CurrentDate))) AS FirstDayOfYear,
		CONVERT(DATETIME, '12/31/' + CONVERT(VARCHAR, DATEPART(YY, 
		@CurrentDate))) AS LastDayOfYear,
		NULL AS IsHolidayUSA,
		CASE DATEPART(DW, @CurrentDate)
			WHEN 1 THEN 0
			WHEN 2 THEN 1
			WHEN 3 THEN 1
			WHEN 4 THEN 1
			WHEN 5 THEN 1
			WHEN 6 THEN 1
			WHEN 7 THEN 0
			END AS IsWeekday,
		NULL AS HolidayUSA,
		NULL AS IsHolidayUK,
		NULL AS HolidayUK,
		DATENAME(DW, @CurrentDate) AS DayNameIT,
		DATENAME(MM, @CurrentDate) AS MonthNameIT,
		NULL AS IsHolidayIT,
		NULL AS HolidayIT,
		CONVERT (char(10),@CurrentDate,103) as FullDateIT

	SET @CurrentDate = DATEADD(DD, 1, @CurrentDate)
END

/********************************************************************************************/
 
/* Step 3.
Update Values of Holiday as per UK Government Declaration for National Holiday.
*/

/*Update HOLIDAY fields of UK as per Govt. Declaration of National Holiday*/
	
-- Good Friday  April 18 
--	UPDATE [dbo].[DimDate]
--		SET HolidayUK = 'Good Friday'
--	WHERE [Month] = 4 AND [DayOfMonth]  = 18
UPDATE D
SET D.HolidayUK = 'Good Friday'

FROM (
	SELECT
		dbo.ufn_GetEasterSunday(Y.Year) AS EasterSunday
	FROM (
		SELECT DISTINCT Year FROM Import.Dates
	) Y
) ES
INNER JOIN Import.Dates D ON D.Date = DATEADD(dd, -2, ES.EasterSunday);

-- Easter Monday  April 21 
--	UPDATE [dbo].[DimDate]
--		SET HolidayUK = 'Easter Monday'
--	WHERE [Month] = 4 AND [DayOfMonth]  = 21
UPDATE D
SET D.HolidayUK = 'Easter Monday'

FROM (
	SELECT
		dbo.ufn_GetEasterSunday(Y.Year) AS EasterSunday
	FROM (
		SELECT DISTINCT Year FROM Import.Dates
	) Y
) ES
INNER JOIN Import.Dates D ON D.Date = DATEADD(dd, 1, ES.EasterSunday);

-- Early May Bank Holiday   May 5 
   UPDATE Import.Dates
		SET HolidayUK = 'Early May Bank Holiday'
	WHERE Month = 5 AND DayOfMonth  = 5

-- Spring Bank Holiday  May 26 
	UPDATE Import.Dates
		SET HolidayUK = 'Spring Bank Holiday'
	WHERE Month = 5 AND DayOfMonth  = 26

-- Summer Bank Holiday  August 25 
    UPDATE Import.Dates
		SET HolidayUK = 'Summer Bank Holiday'
	WHERE Month = 8 AND DayOfMonth  = 25

-- Boxing Day  December 26  	
    UPDATE Import.Dates
		SET HolidayUK = 'Boxing Day'
	WHERE Month = 12 AND DayOfMonth  = 26	

-- Christmas
	UPDATE Import.Dates
		SET HolidayUK = 'Christmas Day'
	WHERE Month = 12 AND DayOfMonth  = 25

--New Years Day
	UPDATE Import.Dates
		SET HolidayUK  = 'New Year''s Day'
	WHERE Month = 1 AND DayOfMonth = 1

--Update flag for UK Holidays 1= Holiday, 0=No Holiday
	
	UPDATE Import.Dates
		SET IsHolidayUK  = CASE WHEN HolidayUK   IS NULL 
		THEN 0 WHEN HolidayUK   IS NOT NULL THEN 1 END
		

/*
Step 4.
Update Values of Holiday as per USA Govt. Declaration for National Holiday.
*/

/*Update HOLIDAY Field of USA In dimension*/
	
 	/*Thanksgiving - Fourth THURSDAY in November*/
	UPDATE Import.Dates
		SET HolidayUSA = 'Thanksgiving Day'
	WHERE
		Month = 11 
		AND DayOfWeekUSA = 'Thursday' 
		AND DayOfWeekInMonth = 4

	/*Christmas*/
	UPDATE Import.Dates
		SET HolidayUSA = 'Christmas Day'
		
	WHERE Month = 12 AND DayOfMonth  = 25

	/*4th of July*/
	UPDATE Import.Dates
		SET HolidayUSA = 'Independance Day'
	WHERE Month = 7 AND DayOfMonth = 4

	/*New Years Day*/
	UPDATE Import.Dates
		SET HolidayUSA = 'New Year''s Day'
	WHERE Month = 1 AND DayOfMonth = 1

	/*Memorial Day - Last Monday in May*/
	UPDATE Import.Dates
		SET HolidayUSA = 'Memorial Day'
	FROM Import.Dates
	WHERE DateKey IN 
		(
		SELECT
			MAX(DateKey)
		FROM Import.Dates
		WHERE
			MonthName = 'May'
			AND DayOfWeekUSA  = 'Monday'
		GROUP BY
			Year,
			Month
		)

	/*Labor Day - First Monday in September*/
	UPDATE Import.Dates
		SET HolidayUSA = 'Labor Day'
	FROM Import.Dates
	WHERE DateKey IN 
		(
		SELECT
			MIN(DateKey)
		FROM Import.Dates
		WHERE
			MonthName = 'September'
			AND DayOfWeekUSA = 'Monday'
		GROUP BY
			Year,
			Month
		)

	/*Valentine's Day*/
	UPDATE Import.Dates
		SET HolidayUSA = 'Valentine''s Day'
	WHERE
		Month = 2 
		AND DayOfMonth = 14

	/*Saint Patrick's Day*/
	UPDATE Import.Dates
		SET HolidayUSA = 'Saint Patrick''s Day'
	WHERE
		Month = 3
		AND DayOfMonth = 17

	/*Martin Luthor King Day - Third Monday in January starting in 1983*/
	UPDATE Import.Dates
		SET HolidayUSA = 'Martin Luthor King Jr Day'
	WHERE
		Month = 1
		AND DayOfWeekUSA  = 'Monday'
		AND Year >= 1983
		AND DayOfWeekInMonth = 3

	/*President's Day - Third Monday in February*/
	UPDATE Import.Dates
		SET HolidayUSA = 'President''s Day'
	WHERE
		Month = 2
		AND DayOfWeekUSA = 'Monday'
		AND DayOfWeekInMonth = 3

	/*Mother's Day - Second Sunday of May*/
	UPDATE Import.Dates
		SET HolidayUSA = 'Mother''s Day'
	WHERE
		Month = 5
		AND DayOfWeekUSA = 'Sunday'
		AND DayOfWeekInMonth = 2

	/*Father's Day - Third Sunday of June*/
	UPDATE Import.Dates
		SET HolidayUSA = 'Father''s Day'
	WHERE
		Month = 6
		AND DayOfWeekUSA = 'Sunday'
		AND DayOfWeekInMonth = 3

	/*Halloween 10/31*/
	UPDATE Import.Dates
		SET HolidayUSA = 'Halloween'
	WHERE
		Month = 10
		AND DayOfMonth = 31

	/*Election Day - The first Tuesday after the first Monday in November*/
	BEGIN
	DECLARE @Holidays TABLE (ID INT IDENTITY(1,1), 
	DateID int, Week TINYINT, YEAR CHAR(4), DAY CHAR(2))

		INSERT INTO @Holidays(DateID, Year,Day)
		SELECT
			DateKey,
			Year,
			DayOfMonth 
		FROM Import.Dates
		WHERE
			Month = 11
			AND DayOfWeekUSA = 'Monday'
		ORDER BY
			YEAR,
			DayOfMonth 

		DECLARE @CNTR INT, @POS INT, @STARTYEAR INT, @ENDYEAR INT, @MINDAY INT

		SELECT
			@CURRENTYEAR = MIN(Year)
			, @STARTYEAR = MIN(Year)
			, @ENDYEAR = MAX(Year)
		FROM @Holidays

		WHILE @CURRENTYEAR <= @ENDYEAR
		BEGIN
			SELECT @CNTR = COUNT(Year)
			FROM @Holidays
			WHERE Year = @CURRENTYEAR

			SET @POS = 1

			WHILE @POS <= @CNTR
			BEGIN
				SELECT @MINDAY = MIN(DAY)
				FROM @Holidays
				WHERE
					Year = @CURRENTYEAR
					AND Week IS NULL

				UPDATE @Holidays
					SET Week = @POS
				WHERE
					Year = @CURRENTYEAR
					AND Day = @MINDAY

				SELECT @POS = @POS + 1
			END

			SELECT @CURRENTYEAR = @CURRENTYEAR + 1
		END

		UPDATE Import.Dates
			SET HolidayUSA  = 'Election Day'				
		FROM Import.Dates DT
			JOIN @Holidays HL ON (HL.DateID + 1) = DT.DateKey
		WHERE
			Week = 1
	END
	--set flag for USA holidays in Dimension
	UPDATE Import.Dates
SET IsHolidayUSA = CASE WHEN HolidayUSA  IS NULL THEN 0 WHEN HolidayUSA  IS NOT NULL THEN 1 END

/*
Step 5: giorni e mesi in italiano
*/

DECLARE @Language NVARCHAR(20);
SELECT @Language = @@LANGUAGE;

SET LANGUAGE Italiano;

UPDATE Import.Dates
SET DayNameIT = DATENAME(DW, Date),
	MonthNameIT = DATENAME(MM, Date);

SET LANGUAGE @Language;

/********************************************************************************************/
 
/* Step 6.
Festività italiane.
*/

/*Festività italiane*/

-- Capodanno 1 gennaio
	UPDATE Import.Dates
		SET HolidayIT = 'Capodanno'
	WHERE Month = 1 AND DayOfMonth  = 1
-- Epifania 6 gennaio
	UPDATE Import.Dates
		SET HolidayIT = 'Epifania'
	WHERE Month = 1 AND DayOfMonth  = 6
-- Liberazione dal nazifascismo 25 aprile
	UPDATE Import.Dates
		SET HolidayIT = 'Liberazione dal nazifascismo'
	WHERE Month = 4 AND DayOfMonth  = 25
-- Festa del lavoro 1 maggio
	UPDATE Import.Dates
		SET HolidayIT = 'Festa del lavoro'
	WHERE Month = 5 AND DayOfMonth  = 1
-- Festa della Repubblica 2 giugno
	UPDATE Import.Dates
		SET HolidayIT = 'Festa della Repubblica'
	WHERE Month = 5 AND DayOfMonth  = 2
-- Assunzione di Maria 15 agosto
	UPDATE Import.Dates
		SET HolidayIT = 'Assunzione di Maria'
	WHERE Month = 8 AND DayOfMonth  = 15
-- Ognissanti 1 novembre
	UPDATE Import.Dates
		SET HolidayIT = 'Ognissanti'
	WHERE Month = 11 AND DayOfMonth  = 1
-- Immacolata Concezione 8 dicembre
	UPDATE Import.Dates
		SET HolidayIT = 'Immacolata Concezione'
	WHERE Month = 12 AND DayOfMonth  = 8
-- Natale di Gesù 25 dicembre
	UPDATE Import.Dates
		SET HolidayIT = 'Natale di Gesù'
	WHERE Month = 12 AND DayOfMonth  = 25
-- Santo Stefano 26 dicembre
	UPDATE Import.Dates
		SET HolidayIT = 'Santo Stefano'
	WHERE Month = 12 AND DayOfMonth  = 26
-- Lunedì di Pasqua
	--UPDATE [dbo].[DimDate]
	--	SET HolidayIT = 'Lunedì di Pasqua'
	--WHERE [Month] = 4 AND [DayOfMonth]  = 25
	UPDATE D
	SET D.HolidayIT = 'Lunedì di Pasqua'

	FROM (
		SELECT
			dbo.ufn_GetEasterSunday(Y.Year) AS EasterSunday
		FROM (
			SELECT DISTINCT Year FROM Import.Dates
		) Y
	) ES
	INNER JOIN Import.Dates D ON D.Date = DATEADD(dd, 1, ES.EasterSunday);

--Update flag for IT Holidays 1= Holiday, 0=No Holiday
	
	UPDATE Import.Dates
		SET IsHolidayIT  = CASE WHEN HolidayIT   IS NULL 
		THEN 0 WHEN HolidayIT   IS NOT NULL THEN 1 END

/*****************************************************************************************/
