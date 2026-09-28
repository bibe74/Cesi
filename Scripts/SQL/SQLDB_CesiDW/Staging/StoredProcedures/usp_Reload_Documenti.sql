
CREATE PROCEDURE Staging.usp_Reload_Documenti
AS
BEGIN

    SET NOCOUNT ON;

    DECLARE @lastupdated_staging DATETIME;
    DECLARE @provider_name NVARCHAR(60) = N'MyDatamartReporting';
    DECLARE @full_table_name sysname = N'Landing.COMETA_Documento_Riga';

    SELECT TOP 1 @lastupdated_staging = lastupdated_staging
    FROM audit.tables
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    IF (@lastupdated_staging IS NULL) SET @lastupdated_staging = CAST('19000101' AS DATETIME);

    BEGIN TRANSACTION

    TRUNCATE TABLE Staging.Documenti;

    INSERT INTO Staging.Documenti
    SELECT * FROM Staging.DocumentiView
    WHERE UpdateDatetime > @lastupdated_staging;

    SELECT @lastupdated_staging = MAX(UpdateDatetime) FROM Staging.Documenti;

    IF (@lastupdated_staging IS NOT NULL)
    BEGIN

    UPDATE audit.tables
    SET lastupdated_staging = @lastupdated_staging
    WHERE provider_name = @provider_name
        AND full_table_name = @full_table_name;

    END;

    COMMIT

    UPDATE D
    SET D.PKGruppoAgenti = CADGA.PKGruppoAgentiDefault

    FROM Staging.Documenti D
    INNER JOIN Dim.Cliente C ON C.PKCliente = D.PKClienteFattura
    INNER JOIN Import.CapoAreaDefault_GruppoAgenti CADGA ON CADGA.CapoAreaDefault = C.CapoAreaDefault
    WHERE D.Profilo LIKE N'FATTURA%'
        AND D.PKGruppoAgenti < 0;

END;

GO

