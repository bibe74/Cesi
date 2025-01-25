WITH Profilo_documento
AS (
    SELECT
        id_prof_documento,
        codice as codice_profilo_doc,
        descrizione
    FROM SERVER01.MyDatamartReporting.dbo.COMETA_prof_documento
),
documento
AS (
SELECT anno_enasarco,
    archiviato,
    asp_beni,
    avviso_telefonico,
    bloccato,
    cambio,
    cambio_riferimento,
    chius_forzata,
    contabilizzato,
    creato_in_esenzione,
    data_competenza,
    data_dec_pagamento,
    data_documento,
    data_fine_competenza,
    data_inizio_competenza,
    data_prev_consegna,
    data_provvigioni,
    data_registrazione,
    Month (data_registrazione) AS mese,
    data_scadenza,
    data_trasporto,
    des_contabile,
    doc_globale,
    evaso_totalmente,
    flag_mora,
    forza_mov_comm,
    forza_pesi,
    forza_peso_trasp,
    id_azienda,
    id_banca_sog_commerciale,
    id_caus_magazzino,
    id_caus_trasporto,
    id_cod_cons,
    id_con_pagamento,
    id_destinazione,
    id_documento,
    id_documento_padre,
    id_gruppo_agenti AS id_gruppo_agenti_documento,
    id_lingua,
    id_linguaggio,
    id_listino,
    id_locazione_1,
    id_locazione_2,
    id_magazzino,
    id_magazzino_2,
    id_modo_trasp,
    id_nat_transaz,
    id_packing_list,
    id_prof_documento,
    id_prof_registrazione,
    id_registro,
    id_sog_commerciale,
    id_sog_commerciale_fattura,
    id_sog_commerciale_vettore,
    id_sog_commerciale_vettore_2,
    id_tab_iva_acconto,
    id_tipo_pag_acconto,
    id_tipo_pagamento,
    id_tipo_trasporto,
    id_unita_misura_peso,
    id_unita_misura_volume,
    id_utente,
    id_val_acconto,
    id_valuta,
    id_valuta_riferimento,
    imp_acconto,
    importo_contrassegno,
    interlocutore,
    mezzo_trasporto,
    modalita_incasso_contr,
    note_blocco,
    note_decisionali,
    note_intestazione,
    num_colli,
    num_documento,
    num_progressivo,
    oggetto,
    ora_registrazione,
    ora_trasporto,
    origine,
    oscillazione_riferimento,
    peso_lordo,
    peso_netto,
    prog_globale,
    rateo_risconto,
    sconto_fin_cassa,
    sconto_fin_incondizionato,
    sconto_finanziario,
    segno_doc,
    segno_iva,
    sospeso,
    stampato,
    targa_mezzo_trasporto,
    tipo_fattura,
    tras_mezzo,
    trasferito,
    trimestre_enasarco,
    validita_offerta,
    libero_4,
    id_libero_1,
    id_libero_2,
    id_libero_3,
    id_tipo_fatturazione,
    data_inizio_contratto AS data_inizio_contratto, 
    data_fine_contratto AS data_fine_contratto, 
    year(data_fine_contratto) AS anno_fc,
    month(data_fine_contratto) AS mese_fc,
    rinnovo_automatico, 
    adeguamento_istat,
    motivo_disdetta, 
    data_disdetta AS data_disdetta
FROM SERVER01.MyDatamartReporting.dbo.COMETA_documento
),
Decod_sog_comm
AS (
    SELECT
        'C' AS tipo,
        N'CLIENTE' AS descr_sog_com

    UNION ALL SELECT 'F', N'FORNITORE'
    UNION ALL SELECT 'P', 'CLIENTE POTENZIALE'
),
Decod_Tipo_Reg
AS (
    SELECT
        'IV' AS tipo_registro,
        N'IVA VENDITE' AS Descr_Tipo_Reg

    UNION ALL SELECT 'IA', N'IVA ACQUISTI'
    UNION ALL SELECT 'DV', N'DDT VENDITE'
    UNION ALL SELECT 'DA', N'DDT ACQUISTI'
    UNION ALL SELECT 'GA', N'MOVIMENTI GENERICI'
    UNION ALL SELECT 'GV', N'RESO CLIENTE'
    UNION ALL SELECT 'OA', N'ORDINI ACQUISTI'
    UNION ALL SELECT 'OV', N'ORDINI VENDITE'
    UNION ALL SELECT 'MN', N'REGISTRO MANAGERIALI'
    UNION ALL SELECT 'PN', N'PRIMA NOTA'
    UNION ALL SELECT 'IC', N'IVA CORRISPETTIVI'
    UNION ALL SELECT 'PV', N'PREVENTIVI'
),
esercizio
AS (
    SELECT bloccato,
        codice,
        data_fine,
        data_inizio,
        id_azienda,
        id_esercizio,
        id_valuta,
        num_progressivo,
        riporto,
        ult_bollato,
        ult_data_stampa_bollato

    FROM SERVER01.MyDatamartReporting.dbo.COMETA_esercizio
),
Gruppo_Agenti
AS (
    SELECT codice,
        descrizione,
        fat_ult_elab,
        gestione_insoluti,
        id_azienda,
        id_gruppo_agenti,
        id_listino,
        id_sog_com_agente,
        id_sog_com_capo_area,
        id_sog_com_sub_agente,
        inc_ult_elab,
        mod_liq_provvigione,
        rip_per_fatturato,
        rip_per_incassato,
        sconto_fin_cassa,
        sconto_fin_incondizionato,
        sconto_finanziario,
        sconto_riga,
        stampa_sollecito

    FROM SERVER01.MyDatamartReporting.dbo.COMETA_gruppo_agenti
),
sog_commerciale
AS (
    SELECT
        codice,
        con_fattura,
        con_fido,
        con_scaduto,
        crea_gruppo_agenti,
        data_acquisizione,
        data_dec_pagamenti,
        data_lr_prot_documenti,
        data_ns_prot_documenti,
        data_trasformazione,
        extrafido,
        fat_sos_iva,
        fat_sos_iva_x_ricerca,
        fido,
        giorni_franchigia,
        giorno_fisso_pagamenti,
        id_anagrafica,
        id_att_svolta,
        id_azienda,
        id_banca_sog_commerciale AS sog_id_banca_sog_commerciale,
        id_cat_attivita,
        id_cat_com_sc,
        id_cat_zona,
        --id_con_pagamento,
        id_conto,
        id_gruppo_agenti,
        id_linguaggio,
        id_listino,
        id_magazzino,
        id_prof_doc_prop_ord,
        id_prof_registrazione,
        id_sog_com_vettore,
        id_sog_commerciale,
        id_tab_iva,
        id_tab_iva_fat_prov,
        id_tipo_agente,
        id_tipo_trasporto,
        id_tributo,
        id_utente,
        id_val_prov_agente,
        id_valuta,
        imp_fisso,
        imp_fisso_ult_elab,
        imp_premio_budget,
        importanza_cliente,
        intento,
        modo_fatturazione,
        ns_cod_soggetto,
        num_lr_prot_documenti,
        num_ns_prot_documenti,
        per_fatturazione,
        prec_elab_definitivo,
        scad_raggruppate,
        stato_cliente,
        stato_fornitore,
        tipo,
        uff_iva,
        ult_elab_definitivo,
        web
    FROM SERVER01.MyDatamartReporting.dbo.COMETA_sog_commerciale
    WHERE tipo in ('C','F','P')
),
anagrafica
AS (
    SELECT cap,
        cap_ritenute,
        cassa_previdenza,
        cod_fiscale,
        cog_ritenute,
        con_previdenziale,
        data_nas_ritenute,
        id_anagrafica,
        id_att_svolta,
        id_azienda,
        id_cod_attivita,
        id_lingua,
        id_linguaggio,
        id_localita,
        id_nazione,
        id_tipo_trasporto,
        id_titolo,
        id_tributo,
        id_valuta,
        ind_ritenute,
        indirizzo,
        isc_enasarco,
        loc_ritenute,
        localita,
        luogo_nas_ritenute,
        modificato,
        nat_giuridica,
        naz_nas_ritenute,
        naz_ritenute,
        nazione,
        nome_ritenute,
        par_iva,
        prov_contabile,
        prov_nas_ritenute,
        prov_ritenute,
        provincia,
        rag_soc_1,
        rag_soc_2,
        rit_previdenziale,
        sesso

    FROM SERVER01.MyDatamartReporting.dbo.COMETA_anagrafica
)

SELECT A.rag_soc_1, GA.* FROM Gruppo_Agenti GA
LEFT JOIN sog_commerciale SC ON SC.id_sog_commerciale = GA.id_sog_com_capo_area
LEFT JOIN anagrafica A ON A.id_anagrafica = SC.id_anagrafica

,
sog_commerciale_gen
AS (
    SELECT
        SC.codice,
        SC.con_fattura,
        SC.con_fido,
        SC.con_scaduto,
        SC.crea_gruppo_agenti,
        SC.data_acquisizione,
        SC.data_dec_pagamenti,
        SC.data_lr_prot_documenti,
        SC.data_ns_prot_documenti,
        SC.data_trasformazione,
        SC.extrafido,
        SC.fat_sos_iva,
        SC.fat_sos_iva_x_ricerca,
        SC.fido,
        SC.giorni_franchigia,
        SC.giorno_fisso_pagamenti,
        SC.id_anagrafica,
        SC.id_att_svolta,
        SC.id_azienda,
        SC.sog_id_banca_sog_commerciale,
        SC.id_cat_attivita,
        SC.id_cat_com_sc,
        SC.id_cat_zona,
        SC.id_conto,
        SC.id_gruppo_agenti,
        SC.id_linguaggio,
        SC.id_listino,
        SC.id_magazzino,
        SC.id_prof_doc_prop_ord,
        SC.id_prof_registrazione,
        SC.id_sog_com_vettore,
        SC.id_sog_commerciale,
        SC.id_tab_iva,
        SC.id_tab_iva_fat_prov,
        SC.id_tipo_agente,
        SC.id_tipo_trasporto,
        SC.id_tributo,
        SC.id_utente,
        SC.id_val_prov_agente,
        SC.id_valuta,
        SC.imp_fisso,
        SC.imp_fisso_ult_elab,
        SC.imp_premio_budget,
        SC.importanza_cliente,
        SC.intento,
        SC.modo_fatturazione,
        SC.ns_cod_soggetto,
        SC.num_lr_prot_documenti,
        SC.num_ns_prot_documenti,
        SC.per_fatturazione,
        SC.prec_elab_definitivo,
        SC.scad_raggruppate,
        SC.stato_cliente,
        SC.stato_fornitore,
        SC.tipo,
        SC.uff_iva,
        SC.ult_elab_definitivo,
        SC.web,

        A.cap,
        A.cap_ritenute,
        A.cassa_previdenza,
        A.cod_fiscale,
        A.cog_ritenute,
        A.con_previdenziale,
        A.data_nas_ritenute,
        --A.id_anagrafica,
        --A.id_att_svolta,
        --A.id_azienda,
        --A.id_cod_attivita,
        --A.id_lingua,
        --A.id_linguaggio,
        --A.id_localita,
        --A.id_nazione,
        --A.id_tipo_trasporto,
        --A.id_titolo,
        --A.id_tributo,
        --A.id_valuta,
        --A.ind_ritenute,
        --A.indirizzo,
        --A.isc_enasarco,
        --A.loc_ritenute,
        --A.localita,
        --A.luogo_nas_ritenute,
        --A.modificato,
        --A.nat_giuridica,
        --A.naz_nas_ritenute,
        --A.naz_ritenute,
        --A.nazione,
        --A.nome_ritenute,
        --A.par_iva,
        --A.prov_contabile,
        --A.prov_nas_ritenute,
        --A.prov_ritenute,
        --A.provincia,
        A.rag_soc_1,
        A.rag_soc_2,
        A.rit_previdenziale,
        A.sesso
    FROM sog_commerciale SC
    LEFT JOIN anagrafica A ON A.id_anagrafica = SC.id_anagrafica
),
Capo_area
AS (
    SELECT id_sog_commerciale AS id_sog_com_capo_area_riga_documento,
           codice AS codice_capo_area,
           rag_soc_1 AS nome_capo_area 
    FROM sog_commerciale_gen
    WHERE id_sog_commerciale IN (SELECT DISTINCT id_sog_com_capo_area FROM Gruppo_Agenti)
),
qlv_riga_documento
AS (
    SELECT cambio,
        cambio_riferimento,
        costo_ricavo_commessa,
        da_stampare,
        data_fine_competenza,
        data_inizio_competenza,
        data_prevista_consegna,
        descrizione,
        dim_h,
        dim_l,
        dim_p,
        elaborata_distinta,
        fat_con_globale,
        fat_con_unita_base,
        flag_rat_ris,
        forza_chius_evasione,
        forza_provvigioni,
        id_articolo,
        id_azienda,
        id_caus_magazzino,
        id_caus_trasporto,
        id_commessa,
        id_con_lis_articolo,
        -- id_conto as id_conto_contropartita,
        id_des_articolo,
        id_documento,
        id_gruppo_agenti AS id_gruppo_agenti_riga_documento,
        id_lavorazione_commessa,
        id_listino,
        id_locazione_1,
        id_locazione_2,
        id_nomenclatura,
        id_riga_doc_padre,
        id_riga_doc_provenienza,
        id_riga_documento,
        id_riga_documento as id_riga_documento_cp,
        id_riga_lotto,
        id_tab_iva,
        id_unita_confezione,
        id_valuta,
        id_valuta_riferimento,
        num_colli,
        num_riga,
        oscillazione_riferimento,
        peso_lordo,
        peso_netto,
        prezzo,
        prezzo_netto,
        provv_agente,
        provv_carea,
        provv_imp_agente,
        provv_imp_carea,
        provv_imp_subage,
        provv_subage,
        qta_evasa,
        qta_evasa_un_base,
        qta_sconto_merce,
        qta_un_base,
        quantita,
        sconto,
        sconto_riga,
        sospesa,
        stampato_su_intra,
        tipo_riga_commessa,
        tot_riga_val_az,
        tot_riga_val_doc,
        tot_riga_val_riga,
        val_attribuito,
        totale_riga,
        CASE WHEN (forza_chius_evasione = 'S' OR ((quantita)-(qta_evasa)) <=0) THEN 'S' ELSE 'N' END AS evaso

    FROM SERVER01.MyDatamartReporting.dbo.COMETA_qlv_riga_documento
    Where quantita > 0
),
registro
AS (
    SELECT
        descrizione,
        ges_sco_giornaliero,
        id_attivita,
        id_azienda,
        id_esercizio,
        id_mod_registro,
        id_registro,
        iva_sospensione,
        numero,
        perc_prorata,
        --tipo_registro,
        usa_prorata

    FROM SERVER01.MyDatamartReporting.dbo.COMETA_registro
),
mod_registro
AS (
    SELECT
        descrizione,
        ges_sco_giornaliero,
        id_attivita,
        id_azienda,
        id_desc_cont_bollati,
        id_mod_registro,
        iva_sospensione,
        numero,
        tipo_registro,
        usa_prorata
    FROM SERVER01.MyDatamartReporting.dbo.COMETA_mod_registro
)
SELECT
    D.anno_fc,
    D.mese_fc,
    SUM(RD.totale_riga) AS totale_riga

FROM qlv_riga_documento RD
INNER JOIN documento D ON D.id_documento = RD.id_documento
    AND D.anno_fc IN (2017, 2018)
INNER JOIN Profilo_documento PD ON PD.id_prof_documento = D.id_prof_documento
    AND PD.codice_profilo_doc = N'ORDCLI'
INNER JOIN sog_commerciale SC ON SC.id_sog_commerciale = D.id_sog_commerciale
INNER JOIN Decod_sog_comm DSC ON DSC.tipo = SC.tipo
    AND DSC.descr_sog_com = N'CLIENTE'
INNER JOIN registro R ON R.id_registro = D.id_registro
INNER JOIN mod_registro MR ON MR.id_mod_registro = R.id_mod_registro
INNER JOIN Decod_Tipo_Reg DTR ON DTR.tipo_registro = MR.tipo_registro
    AND DTR.Descr_Tipo_Reg = N'ORDINI VENDITE'
GROUP BY D.anno_fc,
    D.mese_fc,
    PD.descrizione


    SELECT * FROM SERVER01.MyDatamartReporting.dbo.COMETA_sog_commerciale SC
    INNER JOIN SERVER01.MyDatamartReporting.dbo.COMETA_anagrafica A ON A.id_anagrafica = SC.id_anagrafica
        AND A.rag_soc_1 = N'LOPREVITE ANTONIO'