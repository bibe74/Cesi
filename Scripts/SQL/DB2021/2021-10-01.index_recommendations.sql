USE Nop_MySolution;
CREATE NONCLUSTERED INDEX IX_dbo_GenericAttribute_EntityId_KeyGroup ON dbo.GenericAttribute (EntityId, KeyGroup);
CREATE NONCLUSTERED INDEX IX_dbo_Customer_CustomerGuid ON dbo.Customer (CustomerGuid);
GO

USE MySolution;
-- Verificare -- CREATE NONCLUSTERED INDEX IX_dbo_LogsEpiserver_DataOra_Ramo_PageType_IDDocument ON dbo.LogsEpiserver (DataOra, Ramo, PageType, IDDocument);
CREATE NONCLUSTERED INDEX IX_dbo_cesi_Correlazioni_EPISERVER_type1_id2_type2 ON dbo.cesi_Correlazioni_EPISERVER (type1, id2, type2);
CREATE NONCLUSTERED INDEX IX_dbo_cesi_Correlazioni_EPISERVER_id1_type1_type2 ON dbo.cesi_Correlazioni_EPISERVER (id1, type1, type2);
GO
