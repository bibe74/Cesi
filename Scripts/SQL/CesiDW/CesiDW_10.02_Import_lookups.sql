USE CesiDW;
GO

/*
SET NOEXEC OFF;
--*/ SET NOEXEC ON;
GO

/**
 * @table Import.Decod_Sog_Comm
 * @description Decodifica soggetto commerciale
*/

DROP TABLE IF EXISTS Import.Decod_Sog_Comm;
GO

CREATE TABLE Import.Decod_Sog_Comm (
    tipo            CHAR(1) NOT NULL CONSTRAINT PK_Import_Decod_Sog_Comm PRIMARY KEY CLUSTERED,
    descr_sog_com   NVARCHAR(20) NOT NULL
);
GO

INSERT INTO Import.Decod_Sog_Comm
(
    tipo,
    descr_sog_com
)
VALUES ('C', N'CLIENTE'),
    ('F', N'FORNITORE'),
    ('P', N'CLIENTE POTENZIALE');
GO

/**
 * @table Import.Decod_Tipo_Reg
 * @description Decodifica soggetto commerciale
*/

DROP TABLE IF EXISTS Import.Decod_Tipo_Reg;
GO

CREATE TABLE Import.Decod_Tipo_Reg (
    tipo_registro   CHAR(2) NOT NULL CONSTRAINT PK_Import_Decod_Tipo_Reg PRIMARY KEY CLUSTERED,
    descr_tipo_reg  NVARCHAR(20) NOT NULL
);
GO

INSERT INTO Import.Decod_Tipo_Reg
(
    tipo_registro,
    descr_tipo_reg
)
VALUES ('IV', N'IVA VENDITE'),
    ('IA', N'IVA ACQUISTI'),
    ('DV', N'DDT VENDITE'),
    ('DA', N'DDT ACQUISTI'),
    ('GA', N'MOVIMENTI GENERICI'),
    ('GV', N'RESO CLIENTE'),
    ('OA', N'ORDINI ACQUISTI'),
    ('OV', N'ORDINI VENDITE'),
    ('MN', N'REGISTRO MANAGERIALI'),
    ('PN', N'PRIMA NOTA'),
    ('IC', N'IVA CORRISPETTIVI'),
    ('PV', N'PREVENTIVI');
GO

/**
 * @table Import.Provincia
 * @description Province italiane (da https://dati.inail.it/opendata/elements/Provincia)
*/

--DROP TABLE IF EXISTS Import.Provincia;
GO

IF OBJECT_ID('Import.Provincia', 'U') IS NULL
BEGIN

    CREATE TABLE Import.Provincia (
	    Provincia NVARCHAR(50) NOT NULL,
	    CodSiglaProvincia NVARCHAR(50) NOT NULL CONSTRAINT PK_Import_Provincia PRIMARY KEY CLUSTERED,
	    DescrProvincia NVARCHAR(50) NOT NULL,
	    CodCittaMetropolitana NVARCHAR(50) NOT NULL,
	    CodRegione NVARCHAR(50) NOT NULL,
	    DescrRegione NVARCHAR(50) NOT NULL,
	    CodMacroregione NVARCHAR(50) NOT NULL,
	    DescrMacroregione NVARCHAR(50) NOT NULL,
	    CodNazione NVARCHAR(50) NOT NULL,
	    DescrNazione NVARCHAR(50) NOT NULL,
	    DataInizioValidita NVARCHAR(50) NOT NULL,
	    DataFineValidita NVARCHAR(50) NOT NULL
    );

END;
GO

/**
 * @table Import.Decod_StatoCrediti
 * @description Decodifica stato crediti
*/

DROP TABLE IF EXISTS Import.Decod_StatoCrediti;
GO

CREATE TABLE Import.Decod_StatoCrediti (
    IDStatoCrediti  TINYINT NOT NULL CONSTRAINT PK_Import_Decod_StatoCrediti PRIMARY KEY CLUSTERED,
    StatoCrediti    NVARCHAR(40) NOT NULL
);
GO

INSERT INTO Import.Decod_StatoCrediti
(
    IDStatoCrediti,
    StatoCrediti
)
VALUES (0, N'Da verificare dalla Segreteria Corsi'),
    (1, N'Accettati dall''Ente'),
    (2, N'Rifiutati dall''Ente'),
    (3, N'Verificati e da inviare all''Ente');
GO

/**
 * @table Import.Decod_TipoCrediti
 * @description Decodifica Tipo crediti
*/

DROP TABLE IF EXISTS Import.Decod_TipoCrediti;
GO

CREATE TABLE Import.Decod_TipoCrediti (
    IDTipoCrediti  NVARCHAR(40) NOT NULL CONSTRAINT PK_Import_Decod_TipoCrediti PRIMARY KEY CLUSTERED,
    TipoCrediti    NVARCHAR(40) NOT NULL
);
GO

INSERT INTO Import.Decod_TipoCrediti
(
    IDTipoCrediti,
    TipoCrediti
)
VALUES (N'TipoCred-COM', N'CNDCEC'),
    (N'TipoCred-REV', N'REVISORI'),
    (N'TipoCred-CDL', N'CDL'),
    (N'TipoCred-TRI', N'TRIBUTARISTI');
GO
