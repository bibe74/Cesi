SELECT
    C.CodiceCliente,
    C.RagioneSociale,
    C.Indirizzo,
    C.Localita,
    C.Provincia,
    C.PartitaIVA,
    ----GAD.CapoArea AS CapoAreaDocumento,
    GADR.CapoArea AS CapoAreaDocumento_Riga,
    ----GAC.CapoArea AS CapoAreaCliente,
    ----GACF.CapoArea AS CapoAreaClienteFattura,
    N'TODO' AS Azione,
    N'TODO' AS Rinnovo,
    D.PKDataInizioContratto,
    DIC.Data_IT AS DataInizioContratto,
    D.PKDataFineContratto,
    DFC.Data_IT AS DataFineContratto,
    D.PKDataRegistrazione,
    DR.Data_IT AS DataRegistrazione,
    D.NumeroDocumento,
    N'TODO' AS Pagamento,
    D.Libero2 AS Progetto,
    A.Codice AS CodiceArticolo,
    A.Descrizione AS TipoAbbonamento,
    D.ImportoTotale,
    N'TODO' AS ImportoInsoluto,
    C.PKDataDisdetta,
    C.MotivoDisdetta

FROM Fact.Documenti D
INNER JOIN Dim.Data DD ON DD.PKData = D.PKDataDocumento
INNER JOIN Dim.Data DIC ON DIC.PKData = D.PKDataInizioContratto
INNER JOIN Dim.Data DFC ON DFC.PKData = D.PKDataFineContratto
INNER JOIN Dim.Data DR ON DR.PKData = D.PKDataRegistrazione

    AND DR.Anno = 2021

----INNER JOIN Dim.GruppoAgenti GAD ON GAD.PKGruppoAgenti = D.PKGruppoAgenti
INNER JOIN Dim.GruppoAgenti GADR ON GADR.PKGruppoAgenti = D.PKGruppoAgenti_Riga
INNER JOIN Dim.Cliente C ON C.PKCliente = D.PKCliente

--AND C.CodiceCliente = N'CL010276'
--AND C.CodiceCliente = N'CL010343'

----INNER JOIN Dim.GruppoAgenti GAC ON GAC.PKGruppoAgenti = C.PKGruppoAgenti
----INNER JOIN Dim.Cliente CF ON CF.PKCliente = D.PKClienteFattura
----INNER JOIN Dim.GruppoAgenti GACF ON GACF.PKGruppoAgenti = CF.PKGruppoAgenti
INNER JOIN Dim.Articolo A ON A.PKArticolo = D.PKArticolo

WHERE D.ImportoTotale <> 0.0
    --AND D.Profilo LIKE N'FATTURA%'
    --AND D.Profilo <> N'FATTURA SEMINARI'
--AND D.PKDocumenti IN (62603, 62604)
--AND D.PKDocumenti IN (62443, 62444)
--AND GAD.CapoArea <> GADR.CapoArea
--AND GADR.CapoArea <> GAC.CapoArea


SELECT TOP 100 * FROM SERVER01.MyDatamartReporting.dbo.COMETA_scadenza;
SELECT TOP 100 * FROM SERVER01.MyDatamartReporting.dbo.mov_scadenza;
