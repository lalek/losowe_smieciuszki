# Statystyki wieku, okregow, decyle:
```
  $ bash extract_user_data_from_paneldb.sh
  $ TYGODNI=52 MINIMUM_ODWIEDZIN=4 bash percentyle.sh | psql -F, --no-align  -h db1 -U $USER discourse &> statystyki_roczne.txt
  $ TYGODNI=13 MINIMUM_ODWIEDZIN=2 bash percentyle.sh | psql -F, --no-align  -h db1 -U $USER discourse &> statystyki_kwartalne.txt
```
# Statystyki zmian metryk kobiet/mezczyzn w czasie, globalne/per region
# (straszny smietnik)
```
  $ bash smietnik.sh
```
