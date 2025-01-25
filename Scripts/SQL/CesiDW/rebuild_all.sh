#!bin/sh

for i in $(ls -1 CesiDW_[0-6]*.sql |egrep -v "database_creation|stub"); do echo "-- $i"; cat $i |sed -e '1,7d'; echo; done > CesiDW_99.99_rebuild_all.sql
