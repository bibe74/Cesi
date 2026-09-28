
CREATE PROCEDURE Staging.usp_Reload_CapoArea
AS
BEGIN

    SET NOCOUNT ON;

    --DECLARE @lastupdated_staging DATETIME;
    --DECLARE @provider_name NVARCHAR(60) = N'MyDatamartReporting';
    --DECLARE @full_table_name sysname = N'Landing.COMETA_Gruppo_Agenti';

    --SELECT TOP 1 @lastupdated_staging = lastupdated_staging
    --FROM audit.tables
    --WHERE provider_name = @provider_name
    --    AND full_table_name = @full_table_name;

    --IF (@lastupdated_staging IS NULL) SET @lastupdated_staging = CAST('19000101' AS DATETIME);

    BEGIN TRANSACTION

    TRUNCATE TABLE Staging.CapoArea;

    INSERT INTO Staging.CapoArea
    SELECT * FROM Staging.CapoAreaView;
    --WHERE UpdateDatetime > @lastupdated_staging;

    --SELECT @lastupdated_staging = MAX(UpdateDatetime) FROM Staging.CapoArea;

    --IF (@lastupdated_staging IS NOT NULL)
    --BEGIN

    --UPDATE audit.tables
    --SET lastupdated_staging = @lastupdated_staging
    --WHERE provider_name = @provider_name
    --    AND full_table_name = @full_table_name;

    --END;

    COMMIT

END;

GO

