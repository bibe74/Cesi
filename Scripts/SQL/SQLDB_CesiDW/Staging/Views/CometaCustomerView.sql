/**
 * @table Staging.CometaCustomer
 * @description 

 * @depends Landing.COMETA_SoggettoCommerciale
 * @depends Landing.COMETA_Anagrafica
 * @depends Landing.COMETA_Telefono

SELECT TOP (1) * FROM Landing.COMETA_SoggettoCommerciale;
SELECT TOP (1) * FROM Landing.COMETA_Anagrafica;
SELECT TOP (1) * FROM Landing.COMETA_Telefono;
*/

CREATE   VIEW Staging.CometaCustomerView
AS
WITH AbbonamentiAttivi
AS (
    SELECT DISTINCT
        D.id_sog_commerciale,
        D.id_documento,
        D.num_documento,
        D.data_documento,
        D.data_inizio_contratto,
        DATEADD(DAY, 1, D.data_fine_contratto) AS data_fine_contratto

    FROM Landing.COMETA_Documento D
    INNER JOIN Landing.COMETA_SoggettoCommerciale SC ON SC.id_sog_commerciale = D.id_sog_commerciale
        AND SC.tipo = 'C'
        AND SC.IsDeleted = CAST(0 AS BIT)
    INNER JOIN Landing.COMETA_Documento_Riga DR ON DR.id_documento = D.id_documento
        AND DR.IsDeleted = CAST(0 AS BIT)
    INNER JOIN Landing.COMETA_Articolo A ON A.id_articolo = DR.id_articolo
        AND A.IsDeleted = CAST(0 AS BIT)
    INNER JOIN Landing.COMETA_MySolutionTrascodifica MST ON MST.codice = A.codice
        AND MST.IsDeleted = CAST(0 AS BIT)
    WHERE D.id_prof_documento IN (1, 43) -- 1: ORDINE CLIENTE, 43: ORDINE CLIENTE
        AND CONVERT(DATE, CURRENT_TIMESTAMP) BETWEEN D.data_inizio_contratto AND DATEADD(DAY, 1, D.data_fine_contratto)
        AND D.IsDeleted = CAST(0 AS BIT)
),
AbbonamentiDettaglio
AS (
    SELECT
        AA.id_sog_commerciale,
        AA.id_documento,
        AA.num_documento,
        AA.data_documento,
        AA.data_inizio_contratto,
        AA.data_fine_contratto,
        CAST(1 AS BIT) AS HasSconto,
        ROW_NUMBER() OVER (PARTITION BY AA.id_sog_commerciale ORDER BY AA.data_fine_contratto DESC, AA.data_inizio_contratto DESC) AS rnDESC

    FROM AbbonamentiAttivi AA
),
SoggettoCommercialeMailDettaglio
AS (
    SELECT
        SC.id_sog_commerciale,
        LTRIM(RTRIM(E.num_riferimento)) AS Email,
        E.nome,
        E.cognome,
        COALESCE(E.ruolo, 1) as Quote,
        COALESCE(E.descrizione, N'') as telefono_descrizione,
        E.id_telefono,
        ROW_NUMBER() OVER (PARTITION BY SC.id_sog_commerciale ORDER BY CASE E.descrizione WHEN N'ABBONATO' THEN 0 WHEN N'PRINCIPALE' THEN 1 ELSE 2 END, E.id_telefono) AS rn

    FROM Landing.COMETA_SoggettoCommerciale SC
    INNER JOIN Landing.COMETA_Anagrafica A ON A.id_anagrafica = SC.id_anagrafica
        AND A.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Landing.COMETA_Telefono E ON E.id_anagrafica = A.id_anagrafica
        AND E.tipo = 'E'
        --AND E.descrizione = N'ABBONATO'
        AND E.num_riferimento LIKE N'%@%'
        AND E.IsDeleted = CAST(0 AS BIT)
    WHERE SC.tipo = 'C'
        AND SC.IsDeleted = CAST(0 AS BIT)
),
Disdette
AS (
    SELECT DISTINCT
        D.id_sog_commerciale,
        D.id_documento,
        D.num_documento,
        D.data_documento,
        D.data_inizio_contratto,
        DATEADD(DAY, 1, D.data_fine_contratto) AS data_fine_contratto,
        D.data_disdetta,
        D.motivo_disdetta

    FROM Landing.COMETA_Documento D
    INNER JOIN Landing.COMETA_SoggettoCommerciale SC ON SC.id_sog_commerciale = D.id_sog_commerciale
        AND SC.tipo = 'C'
        AND SC.IsDeleted = CAST(0 AS BIT)
    INNER JOIN Landing.COMETA_Documento_Riga DR ON DR.id_documento = D.id_documento
        AND DR.IsDeleted = CAST(0 AS BIT)
    INNER JOIN Landing.COMETA_Articolo A ON A.id_articolo = DR.id_articolo
        AND A.IsDeleted = CAST(0 AS BIT)
    INNER JOIN Landing.COMETA_MySolutionTrascodifica MST ON MST.codice = A.codice
        AND MST.IsDeleted = CAST(0 AS BIT)
    WHERE D.id_prof_documento IN (1, 43) -- 1: ORDINE CLIENTE, 43: ORDINE CLIENTE
        --AND CONVERT(DATE, CURRENT_TIMESTAMP) BETWEEN D.data_inizio_contratto AND DATEADD(DAY, 1, D.data_fine_contratto)
        AND D.IsDeleted = CAST(0 AS BIT)
        AND D.data_disdetta IS NOT NULL
),
DisdetteDettaglio
AS (
    SELECT
        D.id_sog_commerciale,
        D.id_documento,
        D.num_documento,
        D.data_documento,
        D.data_inizio_contratto,
        D.data_fine_contratto,
        D.data_disdetta,
        D.motivo_disdetta,
        ROW_NUMBER() OVER (PARTITION BY D.id_sog_commerciale ORDER BY D.data_fine_contratto DESC, D.data_inizio_contratto DESC) AS rnDESC
    FROM Disdette D
),
TelefonoDettaglio
AS (
    SELECT
        id_anagrafica,
        tipo,
        num_riferimento,
        ROW_NUMBER() OVER (PARTITION BY id_anagrafica, tipo ORDER BY id_telefono DESC) AS rn

    FROM Landing.COMETA_Telefono T
    WHERE tipo IN ('T', 'C', 'F')
        AND COALESCE(num_riferimento, N'') <> N''
        AND T.IsDeleted = CAST(0 AS BIT)
),
TableData
AS (
    SELECT
        SC.id_sog_commerciale,

        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            SC.id_sog_commerciale,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            SC.codice,
            SC.id_anagrafica,
            SC.tipo,
            SC.id_gruppo_agenti,
            A.rag_soc_1,
            A.rag_soc_2,
            A.indirizzo,
            A.cap,
            A.localita,
            A.provincia,
            A.nazione,
            A.cod_fiscale,
            A.par_iva,
            SCMD.Email,
            SCMD.nome,
            SCMD.cognome,
            SCMD.Quote,
            SCMD.telefono_descrizione,
            SCMD.id_telefono,
            AD.id_documento,
            AD.num_documento,
            AD.data_documento,
            AD.data_inizio_contratto,
            AD.data_fine_contratto,
            AD.HasSconto,
            DD.data_disdetta,
            DD.motivo_disdetta,
            TDT.num_riferimento,
            TDC.num_riferimento,
            TDF.num_riferimento,
            SC.id_cat_com_sc,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,

        COALESCE(SC.codice, N'') AS codice,
        COALESCE(SC.id_anagrafica, -1) AS id_anagrafica,
        COALESCE(SC.tipo, 'C') AS tipo,
        COALESCE(SC.id_gruppo_agenti, -1) AS id_gruppo_agenti,
        COALESCE(A.rag_soc_1, N'') + COALESCE(A.rag_soc_2, N'') AS RagioneSociale,
        COALESCE(A.indirizzo, N'') AS indirizzo,
        COALESCE(A.cap, N'') AS cap,
        COALESCE(A.localita, N'') AS localita,
        COALESCE(A.provincia, N'') AS provincia,
        COALESCE(A.nazione, N'') AS nazione,
        COALESCE(A.cod_fiscale, N'') AS cod_fiscale,
        COALESCE(A.par_iva, N'') AS par_iva,
        COALESCE(SCMD.Email, N'') AS Email,
        COALESCE(SCMD.nome, N'') AS nome,
        COALESCE(SCMD.cognome, N'') AS cognome,
        COALESCE(SCMD.Quote, 0) AS Quote,
        COALESCE(SCMD.telefono_descrizione, N'') AS telefono_descrizione,
        COALESCE(SCMD.id_telefono, -1) AS id_telefono,
        COALESCE(AD.id_documento, -1) AS id_documento,
        COALESCE(AD.num_documento, N'') AS num_documento,
        AD.data_documento,
        AD.data_inizio_contratto,
        AD.data_fine_contratto,
        COALESCE(AD.HasSconto, 0) AS HasSconto,
        DD.data_disdetta,
        COALESCE(DD.motivo_disdetta, N'') AS motivo_disdetta,
        COALESCE(TDT.num_riferimento, N'') AS Telefono,
        COALESCE(TDC.num_riferimento, N'') AS Cellulare,
        COALESCE(TDF.num_riferimento, N'') AS Fax,
        COALESCE(SC.id_cat_com_sc, N'') AS IDProfessione,
        COALESCE(CCSC.descrizione, CASE WHEN COALESCE(SC.id_cat_com_sc, N'') = N'' THEN N'' ELSE N'<???>' END) AS Professione

    FROM Landing.COMETA_SoggettoCommerciale SC
    INNER JOIN Landing.COMETA_Anagrafica A ON A.id_anagrafica = SC.id_anagrafica
        AND A.IsDeleted = CAST(0 AS BIT)
    INNER JOIN SoggettoCommercialeMailDettaglio SCMD ON SCMD.id_sog_commerciale = SC.id_sog_commerciale
        AND SCMD.rn = 1
    LEFT JOIN AbbonamentiDettaglio AD ON AD.id_sog_commerciale = SC.id_sog_commerciale
        AND AD.rnDESC = 1
    LEFT JOIN DisdetteDettaglio DD ON DD.id_sog_commerciale = SC.id_sog_commerciale
        AND DD.rnDESC = 1
    LEFT JOIN TelefonoDettaglio TDT ON TDT.id_anagrafica = SC.id_anagrafica
        AND TDT.tipo = 'T'
        AND TDT.rn = 1
    LEFT JOIN TelefonoDettaglio TDC ON TDC.id_anagrafica = SC.id_anagrafica
        AND TDC.tipo = 'C'
        AND TDC.rn = 1
    LEFT JOIN TelefonoDettaglio TDF ON TDF.id_anagrafica = SC.id_anagrafica
        AND TDF.tipo = 'F'
        AND TDF.rn = 1
    LEFT JOIN Landing.COMETA_CategoriaCommercialeSoggettoCommerciale CCSC ON CCSC.id_cat_com_sc = SC.id_cat_com_sc
        AND CCSC.IsDeleted = CAST(0 AS BIT)
)
SELECT
    -- Chiavi
    TD.id_sog_commerciale,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Altri campi
    TD.codice,
    TD.id_anagrafica,
    TD.tipo,
    TD.id_gruppo_agenti,
    TD.RagioneSociale,
    TD.indirizzo,
    TD.cap,
    TD.localita,
    TD.provincia,
    TD.nazione,
    TD.cod_fiscale,
    TD.par_iva,
    TD.Email,
    TD.nome,
    TD.cognome,
    TD.Quote,
    TD.telefono_descrizione,
    TD.id_telefono,
    TD.id_documento,
    TD.num_documento,
    TD.data_documento,
    TD.data_inizio_contratto,
    TD.data_fine_contratto,
    TD.HasSconto,
    TD.data_disdetta,
    TD.motivo_disdetta,
    TD.Telefono,
    TD.Cellulare,
    TD.Fax,
    TD.IDProfessione,
    TD.Professione

FROM TableData TD;

GO

