create view vw_UtentiCorsiFullNamePartecipanti as
select distinct utente, CognomePartecipante + ' ' + NomePartecipante as FullName1, NomePartecipante + ' ' + CognomePartecipante as FullName2
from fact.corsi

GO

