
CREATE PROCEDURE Staging.usp_Reload_CreditiOpenAIDettaglio
AS
BEGIN

    SET NOCOUNT ON;

    DECLARE @lastupdated_staging DATETIME;
    DECLARE @provider_name NVARCHAR(60) = N'GPT';
    DECLARE @full_table_name sysname = N'Landing.GPT_OpenAICredito';

    SELECT TOP 1 @lastupdated_staging = lastupdated_staging
    FROM audit.tables
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    IF (@lastupdated_staging IS NULL) SET @lastupdated_staging = CAST('19000101' AS DATETIME);

    BEGIN TRANSACTION

    TRUNCATE TABLE Staging.CreditiOpenAIDettaglio;

    INSERT INTO Staging.CreditiOpenAIDettaglio
    SELECT * FROM Staging.CreditiOpenAIDettaglioView
    WHERE UpdateDatetime > @lastupdated_staging;

    SELECT @lastupdated_staging = MAX(UpdateDatetime) FROM Staging.CreditiOpenAIDettaglio;

    IF (@lastupdated_staging IS NOT NULL)
    BEGIN

    UPDATE audit.tables
    SET lastupdated_staging = @lastupdated_staging
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    END;

    COMMIT

END;

GO

