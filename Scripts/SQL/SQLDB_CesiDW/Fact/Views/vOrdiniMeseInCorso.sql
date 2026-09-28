
CREATE   VIEW Fact.vOrdiniMeseInCorso
AS
WITH MeseCorrente
AS (
    SELECT
        MIN(D.PKData) AS PKDataInizioMese,
        MAX(D.PKData) AS PKDataFineMese

    FROM Dim.Data CTD
    INNER JOIN Dim.Data D ON D.Anno = CTD.Anno AND D.Mese = CTD.Mese
    WHERE CTD.PKData = CAST(CURRENT_TIMESTAMP AS DATE)
),
Insoluti
AS (
    SELECT
        D.PKCliente,
        SUM(S.ImportoResiduo) AS Insoluto
    FROM Fact.Scadenze S
    INNER JOIN Fact.Documenti D ON D.PKDocumenti = S.PKDocumenti
    GROUP BY D.PKCliente
    HAVING SUM(S.ImportoResiduo) > 0.0
)
SELECT TOP (100)
    C.CodiceCliente,
    C.RagioneSociale,
    C.Indirizzo,
    C.Localita AS Citta,
    C.Provincia,
    C.PartitaIVA,
    GA.GruppoAgenti AS Agente,
    GA.CapoArea AS AgenteAssegnato,
    D.NoteIntestazione AS Azione,
    D.RinnovoAutomatico AS Rinnovo,
    D.PKDataInizioContratto,
    DIC.Data_IT AS DataInizioContratto,
    D.PKDataFineContratto,
    DFC.Data_IT AS DataFineContratto,
    D.PKDataCompetenza AS PKDataDocumento,
    DC.Data_IT AS DataDocumento,
    D.NumeroDocumento,
    D.CondizioniPagamento AS Pagamento,
    D.Libero2 AS Progetto,
    A.Codice AS CodiceArticolo,
    A.Descrizione AS TipoAbbonamento,
    D.ImportoTotale AS TotaleDocumento,
    COALESCE(I.Insoluto, 0.0) AS Insoluto,
    C.PKDataDisdetta,
    DDIS.Data_IT AS DataDisdetta,
    ----C.StatoDisdetta,
    C.MotivoDisdetta

FROM Fact.Documenti D
INNER JOIN MeseCorrente MC ON D.PKDataCompetenza BETWEEN MC.PKDataInizioMese AND MC.PKDataFineMese
INNER JOIN Dim.Cliente C ON C.PKCliente = D.PKCliente
INNER JOIN Dim.GruppoAgenti GA ON GA.PKGruppoAgenti = D.PKGruppoAgenti
INNER JOIN Dim.GruppoAgenti GAR ON GAR.PKGruppoAgenti = D.PKGruppoAgenti_Riga
INNER JOIN Dim.Data DIC ON DIC.PKData = D.PKDataInizioContratto
INNER JOIN Dim.Data DFC ON DFC.PKData = D.PKDataFineContratto
INNER JOIN Dim.Data DC ON DC.PKData = D.PKDataCompetenza
INNER JOIN Dim.Articolo A ON A.PKArticolo = D.PKArticolo
INNER JOIN Dim.Data DDIS ON DDIS.PKData = C.PKDataDisdetta
LEFT JOIN Insoluti I ON I.PKCliente = D.PKCliente
WHERE D.Profilo = N'ORDINE CLIENTE'
ORDER BY AgenteAssegnato,
    C.CodiceCliente,
    D.NumeroDocumento,
    D.NumeroRiga;

GO

