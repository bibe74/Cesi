CREATE view vw_UtentiCorsiEmailDomains as
select distinct utente, substring (Utente, CHARINDEX( '@', Utente) + 1,LEN(Utente)) as UtenteEmailDomain
from fact.corsi
where utente not like ('%gmail.com') and utente not like ('%virgilio.it') and utente not like ('%libero.it') 
and utente not like ('%tiscali.it') and utente not like ('%tin.it') and utente not like ('%hotmail.it')
and utente not like ('%hotmail.com') and utente not like ('%fastwebnet.it') and utente not like ('%yahoo.it')
and utente not like ('%inwind.it') and utente not like ('%alice.it') and utente not like ('%yahoo.com')
and utente not like ('%live.com') and utente not like ('%icloud.com') and utente not like ('%email.it')
and utente not like ('%tiscalinet.it') and utente not like ('%outlook.com') and utente not like ('%outlook.it')
and utente not like ('%live.it') and utente not like ('%interfree.it') and utente not like ('%pec.it')
and utente not like ('%tim.it') and utente not like ('%iol.it') and utente not like ('%aruba.it')
and utente not like ('%katamail.com')

GO

