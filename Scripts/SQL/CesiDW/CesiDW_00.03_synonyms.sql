USE CesiDW;
GO

/*
SET NOEXEC OFF;
--*/ SET NOEXEC ON;
GO

IF NOT EXISTS(SELECT * FROM sys.synonyms WHERE schema_id = SCHEMA_ID('COMETA') AND name = 'Anagrafica') EXEC('CREATE SYNONYM COMETA.Anagrafica FOR SERVER01.MyDatamartReporting.dbo.COMETA_anagrafica;');
IF NOT EXISTS(SELECT * FROM sys.synonyms WHERE schema_id = SCHEMA_ID('COMETA') AND name = 'Articolo') EXEC('CREATE SYNONYM COMETA.Articolo FOR SERVER01.MyDatamartReporting.dbo.COMETA_articolo;');
IF NOT EXISTS(SELECT * FROM sys.synonyms WHERE schema_id = SCHEMA_ID('COMETA') AND name = 'CategoriaCommercialeArticolo') EXEC('CREATE SYNONYM COMETA.CategoriaCommercialeArticolo FOR SERVER01.MyDatamartReporting.dbo.COMETA_cat_com_articolo;');
IF NOT EXISTS(SELECT * FROM sys.synonyms WHERE schema_id = SCHEMA_ID('COMETA') AND name = 'CategoriaFiscale') EXEC('CREATE SYNONYM COMETA.CategoriaFiscale FOR SERVER01.MyDatamartReporting.dbo.COMETA_cat_fiscale;');
IF NOT EXISTS(SELECT * FROM sys.synonyms WHERE schema_id = SCHEMA_ID('COMETA') AND name = 'CategoriaMerceologica') EXEC('CREATE SYNONYM COMETA.CategoriaMerceologica FOR SERVER01.MyDatamartReporting.dbo.COMETA_cat_merceologica;');
IF NOT EXISTS(SELECT * FROM sys.synonyms WHERE schema_id = SCHEMA_ID('COMETA') AND name = 'CondizioniPagamento') EXEC('CREATE SYNONYM COMETA.CondizioniPagamento FOR SERVER01.MyDatamartReporting.dbo.COMETA_con_pagamento;');
IF NOT EXISTS(SELECT * FROM sys.synonyms WHERE schema_id = SCHEMA_ID('COMETA') AND name = 'Documento') EXEC('CREATE SYNONYM COMETA.Documento FOR SERVER01.MyDatamartReporting.dbo.COMETA_documento;');
IF NOT EXISTS(SELECT * FROM sys.synonyms WHERE schema_id = SCHEMA_ID('COMETA') AND name = 'Documento_Riga') EXEC('CREATE SYNONYM COMETA.Documento_Riga FOR SERVER01.MyDatamartReporting.dbo.COMETA_riga_documento;');
IF NOT EXISTS(SELECT * FROM sys.synonyms WHERE schema_id = SCHEMA_ID('COMETA') AND name = 'Documento_Riga_qlv') EXEC('CREATE SYNONYM COMETA.Documento_Riga_qlv FOR SERVER01.MyDatamartReporting.dbo.COMETA_qlv_riga_documento;');
IF NOT EXISTS(SELECT * FROM sys.synonyms WHERE schema_id = SCHEMA_ID('COMETA') AND name = 'Esercizio') EXEC('CREATE SYNONYM COMETA.Esercizio FOR SERVER01.MyDatamartReporting.dbo.COMETA_esercizio;');
IF NOT EXISTS(SELECT * FROM sys.synonyms WHERE schema_id = SCHEMA_ID('COMETA') AND name = 'Gruppo_Agenti') EXEC('CREATE SYNONYM COMETA.Gruppo_Agenti FOR SERVER01.MyDatamartReporting.dbo.COMETA_gruppo_agenti;');
IF NOT EXISTS(SELECT * FROM sys.synonyms WHERE schema_id = SCHEMA_ID('COMETA') AND name = 'GruppoArticoli') EXEC('CREATE SYNONYM COMETA.GruppoArticoli FOR SERVER01.MyDatamartReporting.dbo.COMETA_gruppo_articoli;');
IF NOT EXISTS(SELECT * FROM sys.synonyms WHERE schema_id = SCHEMA_ID('COMETA') AND name = 'Libero_1') EXEC('CREATE SYNONYM COMETA.Libero_1 FOR SERVER01.MyDatamartReporting.dbo.COMETA_libero_1;');
IF NOT EXISTS(SELECT * FROM sys.synonyms WHERE schema_id = SCHEMA_ID('COMETA') AND name = 'Libero_2') EXEC('CREATE SYNONYM COMETA.Libero_2 FOR SERVER01.MyDatamartReporting.dbo.COMETA_libero_2;');
IF NOT EXISTS(SELECT * FROM sys.synonyms WHERE schema_id = SCHEMA_ID('COMETA') AND name = 'Libero_3') EXEC('CREATE SYNONYM COMETA.Libero_3 FOR SERVER01.MyDatamartReporting.dbo.COMETA_libero_3;');
IF NOT EXISTS(SELECT * FROM sys.synonyms WHERE schema_id = SCHEMA_ID('COMETA') AND name = 'MovimentiScadenza') EXEC('CREATE SYNONYM COMETA.MovimentiScadenza FOR SERVER01.MyDatamartReporting.dbo.COMETA_mov_scadenza;');
IF NOT EXISTS(SELECT * FROM sys.synonyms WHERE schema_id = SCHEMA_ID('COMETA') AND name = 'MySolutionContracts') EXEC('CREATE SYNONYM COMETA.MySolutionContracts FOR SERVER01.MyDatamartReporting.dbo.CometaMySolutionContracts;');
IF NOT EXISTS(SELECT * FROM sys.synonyms WHERE schema_id = SCHEMA_ID('COMETA') AND name = 'MySolutionTrascodifica') EXEC('CREATE SYNONYM COMETA.MySolutionTrascodifica FOR SERVER01.MyDatamartReporting.dbo.COMETA_idArticolo_MySolution_transcodifica;');
IF NOT EXISTS(SELECT * FROM sys.synonyms WHERE schema_id = SCHEMA_ID('COMETA') AND name = 'MySolutionUsers') EXEC('CREATE SYNONYM COMETA.MySolutionUsers FOR SERVER01.MyDatamartReporting.dbo.CometaExportMySolutionUsers;');
IF NOT EXISTS(SELECT * FROM sys.synonyms WHERE schema_id = SCHEMA_ID('COMETA') AND name = 'Profilo_Documento') EXEC('CREATE SYNONYM COMETA.Profilo_Documento FOR SERVER01.MyDatamartReporting.dbo.COMETA_prof_documento;');
IF NOT EXISTS(SELECT * FROM sys.synonyms WHERE schema_id = SCHEMA_ID('COMETA') AND name = 'Registro') EXEC('CREATE SYNONYM COMETA.Registro FOR SERVER01.MyDatamartReporting.dbo.COMETA_registro;');
IF NOT EXISTS(SELECT * FROM sys.synonyms WHERE schema_id = SCHEMA_ID('COMETA') AND name = 'Scadenza') EXEC('CREATE SYNONYM COMETA.Scadenza FOR SERVER01.MyDatamartReporting.dbo.COMETA_scadenza;');
IF NOT EXISTS(SELECT * FROM sys.synonyms WHERE schema_id = SCHEMA_ID('COMETA') AND name = 'Semaforo') EXEC('CREATE SYNONYM COMETA.Semaforo FOR SERVER01.MyDatamartReporting.dbo.SEMAFORO;');
IF NOT EXISTS(SELECT * FROM sys.synonyms WHERE schema_id = SCHEMA_ID('COMETA') AND name = 'SoggettoCommerciale') EXEC('CREATE SYNONYM COMETA.SoggettoCommerciale FOR SERVER01.MyDatamartReporting.dbo.COMETA_sog_commerciale;');
IF NOT EXISTS(SELECT * FROM sys.synonyms WHERE schema_id = SCHEMA_ID('COMETA') AND name = 'Telefono') EXEC('CREATE SYNONYM COMETA.Telefono FOR SERVER01.MyDatamartReporting.dbo.COMETA_telefono;');
IF NOT EXISTS(SELECT * FROM sys.synonyms WHERE schema_id = SCHEMA_ID('COMETA') AND name = 'Tipo_Fatturazione') EXEC('CREATE SYNONYM COMETA.Tipo_Fatturazione FOR SERVER01.MyDatamartReporting.dbo.COMETA_tipo_fatturazione;');
IF NOT EXISTS(SELECT * FROM sys.synonyms WHERE schema_id = SCHEMA_ID('MYSOLUTION') AND name = 'Address') EXEC('CREATE SYNONYM MYSOLUTION.Address FOR MYSOLUTIONPRODUZIONE2.Nop_MySolution.dbo.Address;');
IF NOT EXISTS(SELECT * FROM sys.synonyms WHERE schema_id = SCHEMA_ID('MYSOLUTION') AND name = 'Country') EXEC('CREATE SYNONYM MYSOLUTION.Country FOR MYSOLUTIONPRODUZIONE2.Nop_MySolution.dbo.Country;');
IF NOT EXISTS(SELECT * FROM sys.synonyms WHERE schema_id = SCHEMA_ID('MYSOLUTION') AND name = 'Courses') EXEC('CREATE SYNONYM MYSOLUTION.Courses FOR MYSOLUTIONPRODUZIONE2.Nop_MySolution.dbo.VW_MySolution_Courses;');
IF NOT EXISTS(SELECT * FROM sys.synonyms WHERE schema_id = SCHEMA_ID('MYSOLUTION') AND name = 'Customer') EXEC('CREATE SYNONYM MYSOLUTION.Customer FOR MYSOLUTIONPRODUZIONE2.Nop_MySolution.dbo.Customer;');
IF NOT EXISTS(SELECT * FROM sys.synonyms WHERE schema_id = SCHEMA_ID('MYSOLUTION') AND name = 'CustomerAddresses') EXEC('CREATE SYNONYM MYSOLUTION.CustomerAddresses FOR MYSOLUTIONPRODUZIONE2.Nop_MySolution.dbo.CustomerAddresses;');
IF NOT EXISTS(SELECT * FROM sys.synonyms WHERE schema_id = SCHEMA_ID('MYSOLUTION') AND name = 'CustomerPartecipants') EXEC('CREATE SYNONYM MYSOLUTION.CustomerPartecipants FOR MYSOLUTIONPRODUZIONE2.Nop_MySolution.dbo.CustomerPartecipants;');
IF NOT EXISTS(SELECT * FROM sys.synonyms WHERE schema_id = SCHEMA_ID('MYSOLUTION') AND name = 'CustomerRole') EXEC('CREATE SYNONYM MYSOLUTION.CustomerRole FOR MYSOLUTIONPRODUZIONE2.Nop_MySolution.dbo.CustomerRole;');
IF NOT EXISTS(SELECT * FROM sys.synonyms WHERE schema_id = SCHEMA_ID('MYSOLUTION') AND name = 'Customer_CustomerRole_Mapping') EXEC('CREATE SYNONYM MYSOLUTION.Customer_CustomerRole_Mapping FOR MYSOLUTIONPRODUZIONE2.Nop_MySolution.dbo.Customer_CustomerRole_Mapping;');
IF NOT EXISTS(SELECT * FROM sys.synonyms WHERE schema_id = SCHEMA_ID('MYSOLUTION') AND name = 'GenericAttribute') EXEC('CREATE SYNONYM MYSOLUTION.GenericAttribute FOR MYSOLUTIONPRODUZIONE2.Nop_MySolution.dbo.GenericAttribute;');
IF NOT EXISTS(SELECT * FROM sys.synonyms WHERE schema_id = SCHEMA_ID('MYSOLUTION') AND name = 'LogsEpiServer') EXEC('CREATE SYNONYM MYSOLUTION.LogsEpiServer FOR MYSOLUTIONPRODUZIONE2.MySolution.dbo.LogsEpiServer;');
IF NOT EXISTS(SELECT * FROM sys.synonyms WHERE schema_id = SCHEMA_ID('MYSOLUTION') AND name = 'Order') EXEC('CREATE SYNONYM MYSOLUTION.[Order] FOR MYSOLUTIONPRODUZIONE2.Nop_MySolution.dbo.[Order];');
IF NOT EXISTS(SELECT * FROM sys.synonyms WHERE schema_id = SCHEMA_ID('MYSOLUTION') AND name = 'OrderItem') EXEC('CREATE SYNONYM MYSOLUTION.OrderItem FOR MYSOLUTIONPRODUZIONE2.Nop_MySolution.dbo.OrderItem;');
IF NOT EXISTS(SELECT * FROM sys.synonyms WHERE schema_id = SCHEMA_ID('MYSOLUTION') AND name = 'OrderItem_Partecipants') EXEC('CREATE SYNONYM MYSOLUTION.OrderItem_Partecipants FOR MYSOLUTIONPRODUZIONE2.Nop_MySolution.dbo.OrderItem_Partecipants;');
IF NOT EXISTS(SELECT * FROM sys.synonyms WHERE schema_id = SCHEMA_ID('MYSOLUTION') AND name = 'Partecipant') EXEC('CREATE SYNONYM MYSOLUTION.Partecipant FOR MYSOLUTIONPRODUZIONE2.Nop_MySolution.dbo.Partecipant;');
IF NOT EXISTS(SELECT * FROM sys.synonyms WHERE schema_id = SCHEMA_ID('MYSOLUTION') AND name = 'Product') EXEC('CREATE SYNONYM MYSOLUTION.Product FOR MYSOLUTIONPRODUZIONE2.Nop_MySolution.dbo.Product;');
IF NOT EXISTS(SELECT * FROM sys.synonyms WHERE schema_id = SCHEMA_ID('MYSOLUTION') AND name = 'ProductAttributeCombination') EXEC('CREATE SYNONYM MYSOLUTION.ProductAttributeCombination FOR MYSOLUTIONPRODUZIONE2.Nop_MySolution.dbo.ProductAttributeCombination;');
IF NOT EXISTS(SELECT * FROM sys.synonyms WHERE schema_id = SCHEMA_ID('MYSOLUTION') AND name = 'StateProvince') EXEC('CREATE SYNONYM MYSOLUTION.StateProvince FOR MYSOLUTIONPRODUZIONE2.Nop_MySolution.dbo.StateProvince;');
IF NOT EXISTS(SELECT * FROM sys.synonyms WHERE schema_id = SCHEMA_ID('MYSOLUTION') AND name = 'Users') EXEC('CREATE SYNONYM MYSOLUTION.Users FOR MYSOLUTIONPRODUZIONE2.Nop_MySolution.dbo.VW_MySolution_Users;');
IF NOT EXISTS(SELECT * FROM sys.synonyms WHERE schema_id = SCHEMA_ID('COMETAINTEGRATION') AND name = 'ArticleBIData') EXEC('CREATE SYNONYM COMETAINTEGRATION.ArticleBIData FOR MYSOLUTIONPRODUZIONE2.CometaIntegration.dbo.ArticleBIData;');
IF NOT EXISTS(SELECT * FROM sys.synonyms WHERE schema_id = SCHEMA_ID('WEBINARS') AND name = 'WeAutocertificazioni') EXEC('CREATE SYNONYM WEBINARS.WeAutocertificazioni FOR MYSOLUTIONPRODUZIONE2.dbWebinars.dbo.WeAutocertificazioni;');
IF NOT EXISTS(SELECT * FROM sys.synonyms WHERE schema_id = SCHEMA_ID('WEBINARS') AND name = 'WeBinars') EXEC('CREATE SYNONYM WEBINARS.WeBinars FOR MYSOLUTIONPRODUZIONE2.dbWebinars.dbo.WeBinars;');
IF NOT EXISTS(SELECT * FROM sys.synonyms WHERE schema_id = SCHEMA_ID('WEBINARS') AND name = 'CreditoAutocertificazione') EXEC('CREATE SYNONYM WEBINARS.CreditoAutocertificazione FOR MYSOLUTIONPRODUZIONE2.dbWebinars.dbo.CreditoAutocertificazione;');
IF NOT EXISTS(SELECT * FROM sys.synonyms WHERE schema_id = SCHEMA_ID('WEBINARS') AND name = 'CreditoCorso') EXEC('CREATE SYNONYM WEBINARS.CreditoCorso FOR MYSOLUTIONPRODUZIONE2.dbWebinars.dbo.CreditoCorso;');
IF NOT EXISTS(SELECT * FROM sys.synonyms WHERE schema_id = SCHEMA_ID('WEBINARS') AND name = 'CreditoTipologia') EXEC('CREATE SYNONYM WEBINARS.CreditoTipologia FOR MYSOLUTIONPRODUZIONE2.dbWebinars.dbo.CreditoTipologia;');
GO
