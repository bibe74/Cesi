USE CesiDW;
GO

/*
SET NOEXEC OFF;
--*/ SET NOEXEC ON;
GO

IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = N'audit')
BEGIN
	EXEC ('CREATE SCHEMA audit AUTHORIZATION dbo;'); -- audit: tabelle di log
END;
GO

IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = N'setup')
BEGIN
	EXEC ('CREATE SCHEMA setup AUTHORIZATION dbo;'); -- setup: tabelle di setup
END;
GO

IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = N'Import')
BEGIN
	EXEC ('CREATE SCHEMA Import AUTHORIZATION dbo;'); -- Import: tabelle di riferimento per elaborazioni extra-sistema
END;
GO

IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = N'MYSOLUTION')
BEGIN
	EXEC ('CREATE SCHEMA MYSOLUTION AUTHORIZATION dbo;'); -- MYSOLUTION: Sorgenti dati NOP (Nop_MySolution) e accessi (MySolution)
END;
GO

IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = N'COMETA')
BEGIN
	EXEC ('CREATE SCHEMA COMETA AUTHORIZATION dbo;'); -- COMETA: Sorgenti dati COMETA
END;
GO

IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = N'COMETAINTEGRATION')
BEGIN
	EXEC ('CREATE SCHEMA COMETAINTEGRATION AUTHORIZATION dbo;'); -- COMETAINTEGRATION: Sorgenti dati COMETAINTEGRATION
END;
GO

IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = N'WEBINARS')
BEGIN
	EXEC ('CREATE SCHEMA WEBINARS AUTHORIZATION dbo;'); -- WEBINARS: Sorgenti dati WEBINARS
END;
GO

IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = N'Landing')
BEGIN
	EXEC ('CREATE SCHEMA Landing AUTHORIZATION dbo;'); -- Landing: area di sincronizzazione con database di origine (COMETA, ecc.)
END;
GO

IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = N'Staging')
BEGIN
	EXEC ('CREATE SCHEMA Staging AUTHORIZATION dbo;'); -- Staging: area di staging per le elaborazioni
END;
GO

IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = N'Dim')
BEGIN
	EXEC ('CREATE SCHEMA Dim AUTHORIZATION dbo;'); -- Dim: tabelle delle dimensioni di analisi
END;
GO

IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = N'Fact')
BEGIN
	EXEC ('CREATE SCHEMA Fact AUTHORIZATION dbo;'); -- Fact: tabelle dei fatti
END;
GO

IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = N'Bridge')
BEGIN
	EXEC ('CREATE SCHEMA Bridge AUTHORIZATION dbo;'); -- Bridge: tabelle di relazione molti-a-molti
END;
GO
