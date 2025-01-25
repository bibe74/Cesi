USE CesiDW;
GO

/*
SET NOEXEC OFF;
--*/ SET NOEXEC ON;
GO

/**
 * @function dbo.ufn_GetEasterSunday
*/

IF OBJECT_ID('dbo.ufn_GetEasterSunday', 'FN') IS NULL EXEC('CREATE FUNCTION dbo.ufn_GetEasterSunday () RETURNS INT AS BEGIN RETURN 0; END');
GO

ALTER FUNCTION dbo.ufn_GetEasterSunday (@Year CHAR(4)) 
RETURNS DATE 
AS 
BEGIN 
    DECLARE
		@EpactCalc INT,  
        @PaschalDaysCalc INT, 
        @NumOfDaysToSunday INT, 
        @EasterMonth INT, 
        @EasterDay INT;

    SET @EpactCalc = (24 + 19 * (@Year % 19)) % 30;
    SET @PaschalDaysCalc = @EpactCalc - (@EpactCalc / 28);
    SET @NumOfDaysToSunday = @PaschalDaysCalc - ((@Year + @Year / 4 + @PaschalDaysCalc - 13) % 7); 

    SET @EasterMonth = 3 + (@NumOfDaysToSunday + 40) / 44;

    SET @EasterDay = @NumOfDaysToSunday + 28 - (31 * (@EasterMonth / 4));

    RETURN (SELECT CONVERT(DATE, RTRIM(@Year) + RIGHT('0'+RTRIM(@EasterMonth), 2) + RIGHT('0'+RTRIM(@EasterDay), 2))); 
END;
GO

/**
 * @function dbo.ufn_StripCharacters
*/

IF OBJECT_ID('dbo.ufn_StripCharacters', 'FN') IS NULL EXEC('CREATE FUNCTION dbo.ufn_StripCharacters () RETURNS INT AS BEGIN RETURN 0; END');
GO

ALTER FUNCTION dbo.ufn_StripCharacters (
    @String NVARCHAR(MAX), 
    @MatchExpression VARCHAR(255)
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    SET @MatchExpression =  '%['+@MatchExpression+']%'

    WHILE PatIndex(@MatchExpression, @String) > 0
        SET @String = Stuff(@String, PatIndex(@MatchExpression, @String), 1, '')

    RETURN @String

END;
GO
