//---------------------------------------//
//Script caricamento personalizzato Cesi //
//---------------------------------------//

Profilo_documento:
SQL SELECT id_prof_documento,
codice as codice_profilo_doc,
descrizione
FROM COMETA.prof_documento;

SQL SELECT cap,
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
FROM cometa.anagrafica;

localita:
SQL SELECT id_localita,
localita As DescrLocalita,
cap As DescrCap,
provincia As DescrProvincia,
id_regione
FROM localita;

regione:
SQL SELECT id_regione,
descrizione As DescrRegione
FROM regione;

SQL SELECT codice,
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
    //id_con_pagamento,
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
FROM cometa.sog_commerciale
WHERE tipo in ('C','F','P');

Banca_soggetto:
SQL SELECT id_banca_sog_commerciale AS sog_id_banca_sog_commerciale,
conto_corrente,
cin,
iban,
cod_abi,
cod_cab
FROM cometa.banca_sog_commerciale JOIN cometa.banca_appoggio ON cometa.banca_sog_commerciale.id_banca_appoggio = cometa.banca_appoggio.id_banca_appoggio;

cat_attivita:
SQL SELECT id_cat_attivita, codice,
descrizione As DescrizAttivita
FROM cat_attivita;

cat_com_sc:
SQL SELECT id_cat_com_sc, codice,
descrizione As DescrizCatComSc
FROM cat_com_sc;

cat_zona:
SQL SELECT id_cat_zona, codice,
descrizione As DescrizCatZona
FROM cat_zona;

Gruppo_Agenti_1:
SQL SELECT codice,
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
FROM cometa.gruppo_agenti;

Gruppo_Agenti_2:
SQL SELECT codice,
    descrizione,
    fat_ult_elab,
    gestione_insoluti,
    id_azienda,
    id_gruppo_agenti AS id_gruppo_agenti_documento,
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
FROM cometa.gruppo_agenti;

Gruppo_Agenti_3:
SQL SELECT codice,
    descrizione,
    fat_ult_elab,
    gestione_insoluti,
    id_azienda,
    id_gruppo_agenti AS id_gruppo_agenti_riga_documento,
    id_listino,
    id_sog_com_agente,
    id_sog_com_capo_area AS id_sog_com_capo_area_riga_documento, 
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
FROM cometa.gruppo_agenti;


Capo_area:
SQL SELECT id_sog_commerciale AS id_sog_com_capo_area_riga_documento,
       codice AS codice_capo_area,
       rag_soc_1 AS nome_capo_area 
FROM sog_commerciale_gen 
WHERE id_sog_commerciale IN(SELECT DISTINCT id_sog_com_capo_area FROM gruppo_agenti);




SQL SELECT anno_enasarco,
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
    date(data_inizio_contratto) AS data_inizio_contratto, 
    date(data_fine_contratto) AS data_fine_contratto, 
    year(data_fine_contratto) AS anno_fc,
    month(data_fine_contratto) AS mese_fc,
    rinnovo_automatico, 
    adeguamento_istat,
    motivo_disdetta, 
    date(data_disdetta) AS data_disdetta
FROM cometa.documento;

Condizioni_pagamento:
SQL SELECT con_pagamento.id_con_pagamento,
con_pagamento.codice as codice_pagamento,
con_pagamento.descrizione
FROM cometa.con_pagamento;

SQL SELECT id_documento,
    id_tab_iva,
    val_imp_scontato,
    val_imponibile,
    val_omaggi,
    val_spese,
    val_tot_imp_l,
    val_tot_imp_v,
    val_tot_iva_l,
    val_tot_iva_v,
    imponibile,
	imponibile_scontato
FROM cometa.qlv_cast_iva_documento;
//WHERE val_imponibile > 0;

SQL SELECT cambio,
    cambio_riferimento,
    costo_ricavo_commessa,
    da_stampare,
    data_fine_competenza,
    data_inizio_competenza,
    data_prevista_consegna,
//    MONTH (data_prevista_consegna) AS mese_pc,
//    YEAR(data_prevista_consegna) AS anno_pc,
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
   // id_conto as id_conto_contropartita,
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
if (qlv_riga_documento.forza_chius_evasione = 'S' or ((qlv_riga_documento.quantita)-(qlv_riga_documento.qta_evasa)) <=0) then 'S' else 'N' end if as evaso
FROM cometa.qlv_riga_documento
Where quantita >0; //and tot_riga_val_az > 0;

SQL SELECT descrizione,
    ges_sco_giornaliero,
    id_attivita,
    id_azienda,
    id_esercizio,
    id_mod_registro,
    id_registro,
    iva_sospensione,
    numero,
    perc_prorata,
    //tipo_registro,
    usa_prorata
FROM cometa.registro;

SQL SELECT bloccato,
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
FROM cometa.esercizio;

SQL SELECT descrizione,
    ges_sco_giornaliero,
    id_attivita,
    id_azienda,
    id_desc_cont_bollati,
    id_mod_registro,
    iva_sospensione,
    numero,
    tipo_registro,
    usa_prorata
FROM cometa.mod_registro;

SQL SELECT id_destinazione,
rag_soc_1 as dest_rag_soc_1,
rag_soc_2 as dest_rag_soc_2,
indirizzo as dest_indirizzo ,
cap as dest_cap ,
localita as dest_localita ,
provincia as dest_provincia ,
nazione as dest_nazione
FROM cometa.destinazione;

SQL SELECT id_articolo,
descrizione as artdescr,
id_cat_com_articolo,
id_cat_merceologica,
id_gruppo_articoli,
id_cat_fiscale,
codice as codicearticolo
FROM articolo;

SQL SELECT id_cat_com_articolo, descrizione as catcomdescr
FROM cat_com_articolo;

SQL SELECT id_cat_merceologica, descrizione as catmercdescr
FROM cat_merceologica;

SQL SELECT id_cat_fiscale, descrizione as catfiscdescr
FROM cat_fiscale;

SQL SELECT id_gruppo_articoli, descrizione as grupartdescr
FROM gruppo_articoli;


SQL SELECT id_anagrafica, 
  CASE WHEN tipo = 'T' THEN 'Tel'
       WHEN tipo = 'C' THEN 'Cell'
       WHEN tipo = 'F' THEN 'Fax'
       WHEN tipo = 'E' THEN 'Email'
       WHEN tipo = 'W' THEN 'Web'
       ELSE tipo END AS tipo_num, num_riferimento 
FROM telefono;



Quarter:
LOAD * INLINE [
    mese, nome, quarter, semestre
    1, Gen, Q1, S1
    2, Feb, Q1, S1
    3, Mar, Q1, S1
    4, Apr, Q2, S1
    5, Mag, Q2, S1
    6, Giu, Q2, S1
    7, Lug, Q3, S2
    8, Ago, Q3, S2
    9, Set, Q3, S2
    10, Ott, Q4, S2
    11, Nov, Q4, S2
    12, Dic, Q4, S2
];


Quarter_fc:
LOAD * INLINE [
    mese_fc, nome_fc, quarter_fc, semestre_fc
    1, Gen, Q1, S1
    2, Feb, Q1, S1
    3, Mar, Q1, S1
    4, Apr, Q2, S1
    5, Mag, Q2, S1
    6, Giu, Q2, S1
    7, Lug, Q3, S2
    8, Ago, Q3, S2
    9, Set, Q3, S2
    10, Ott, Q4, S2
    11, Nov, Q4, S2
    12, Dic, Q4, S2
];



Decod_Sog_Comm:

LOAD * INLINE [
    tipo, descr_sog_com 
    C, CLIENTE
    F, FORNITORE
    P, CLIENTE POTENZIALE


];

Decod_Tipo_Reg:
LOAD * INLINE [
    tipo_registro, Descr_Tipo_Reg
    IV, IVA VENDITE
    IA, IVA ACQUISTI
    DV, DDT VENDITE
    DA, DDT ACQUISTI
    GA, MOVIMENTI GENERICI
    GV, RESO CLIENTE
    OA, ORDINI ACQUISTI
    OV, ORDINI VENDITE
    MN, REGISTRO MANAGERIALI
    PN, PRIMA NOTA
    IC, IVA CORRISPETTIVI
    PV, PREVENTIVI    

];

Sconti:
LOAD * INLINE [
    tipo_sconto
    Sconto Cassa
    Sconto Incondizionato
    Sconto Finanziario
	Visualizza Tutti
];

riepilogo:
SQL  SELECT //id_esercizio,
	riepilogo.id_articolo,
	riepilogo.id_locazione as id_locazione_riep,
	riepilogo.anno as anno_riepilogo,
	mese as mese_riepilogo,
	riepilogo.ult_costo,
	riepilogo.data_ult_costo,
	riepilogo.primo_costo,
	riepilogo.data_primo_costo,
	riepilogo.qta_rim_iniziale,
	riepilogo.val_rim_iniziale,
	riepilogo.qta_caricata,
	riepilogo.qta_scaricata,
	riepilogo.val_caricato,
	riepilogo.val_scaricato,
	riepilogo.qta_ord_clienti,
	riepilogo.qta_ord_fornitori,
	riepilogo.qta_vis_clienti,
	riepilogo.qta_vis_fornitori,
	riepilogo.qta_imp_produzione,
	riepilogo.qta_ord_produzione,
	riepilogo.qta_cl_clienti,
	riepilogo.qta_cl_fornitori,
	riepilogo.qta_cl_terzi,
    articolo.stampa_in_inventario
FROM riepilogo 
JOIN articolo ON articolo.id_articolo=riepilogo.id_articolo AND articolo.stampa_in_inventario='S';

locazione:
SQL SELECT locazione.id_locazione AS id_locazione_riep,
    locazione.id_magazzino,
    locazione.id_area,
    locazione.corsia,
    locazione.scaffale,
    locazione.ripiano,
    locazione.posizione,
    locazione.id_azienda,
    magazzino.codice AS codmagaz,
    magazzino.descrizione AS descmag,
    magazzino.id_tipo_magazzino
FROM locazione
JOIN magazzino ON magazzino.id_magazzino=locazione.id_magazzino;	


Conto:
SQL SELECT id_conto as id_conto_contropartita,
	codice,
	id_sottomastro,
	descrizione,
	id_gruppo,
	ges_par_aperte,
	id_azienda,
	id_bilancio_cee
	FROM conto;
	
	
Riga_contropartita:
SELECT riga_documento_contropartita.id_riga_documento_contropartita,
id_riga_documento as id_riga_documento_cp,
id_conto as id_conto_contropartita,
importo,
forzato,
perc
FROM riga_documento_contropartita;

	
Commessa:
Sql select commessa_anagrafica.id_commessa,
commessa_anagrafica.codice,
commessa_anagrafica.descrizione,
commessa_anagrafica.data_apertura,
commessa_anagrafica.data_commessa,
commessa_anagrafica.stato_commessa,
commessa_anagrafica.note,
commessa_anagrafica.data_prevista_consegna,
commessa_anagrafica.data_chiusura
FROM commessa_anagrafica;

Riga_Commessa:
SELECT commessa_righe.id_commessa,
commessa_righe.prev_cons,
commessa_righe.dare,
commessa_righe.avere,
commessa_righe.totale_ore_dip,
commessa_righe.paga_oraria,
commessa_righe.id_riga_documento,
commessa_righe.id_articolo_nf,
commessa_righe.prezzo_netto_nf,
commessa_righe.quantita_nf,
conto.codice,
conto.descrizione 
FROM commessa_righe 
LEFT OUTER JOIN conto ON commessa_righe.id_conto = conto.id_conto;

SQL SELECT id_libero_1, codice, descrizione FROM cometa.libero_1;
SQL SELECT id_libero_2, codice, descrizione FROM cometa.libero_2;
SQL SELECT id_libero_3, codice, descrizione FROM cometa.libero_3;
SQL SELECT id_tipo_fatturazione, codice, descrizione FROM tipo_fatturazione;


SQL SELECT scadenza.id_scadenza,
   scadenza.tipo_scadenza,
   scadenza.id_sog_commerciale,
   scadenza.data_scadenza,
   scadenza.importo,
   sum(COALESCE(mov_scadenza.importo,0)) AS saldo_scadenza,
   scadenza.importo-sum(COALESCE(mov_scadenza.importo,0)) AS residuo,
   id_documento AS id_documento_scad
FROM scadenza 
LEFT JOIN mov_scadenza ON scadenza.id_scadenza = mov_scadenza.id_scadenza
WHERE esito_pagamento IN('E','I') AND data_scadenza<now() AND stato_scadenza='D'
GROUP BY scadenza.id_scadenza,
   scadenza.tipo_scadenza,
   scadenza.id_sog_commerciale,
   scadenza.data_scadenza,
   scadenza.importo,
   id_documento
