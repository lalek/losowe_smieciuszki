#!/bin.bash

# Skrypt czyta baze danych panelu i produkuje plik z mapowaniem email=>plec,region,
# ktorego potem uzyjemy czytajac baze danych forum.

set -e

# Haslo do bazy danych
. config.sh

function paneldb() { mysql -h db1 -u $USER -p$PASSWORD panel "$@"; }

OUTPUT_FILE=${OUTPUT_FILE:-panel_data.sql}

# Czytamy dane pelnych, aktywnych czlonkow (status==2).
# Produkujemy tymczasowa tabele postresowa z rekordami wygladajaca +-
# tak:
#
# create temp table extras as select * from ( VALUES 
#   [...]
#   (0,'alek.lewandowski@gmail.com','Aleksander',11,'Warszawa'),
#   [...]
# ) as t (kobieta, email, first_name, region_id, region_name);
#
# Plec determinujemy patrzac na pierwsza litere imienia. Jesli to 'a' to
# klasyfikujemy jako kobiete, w przeciwnym przypadku jako mezczyzne. Przejrzalem
# wszystkie imiona i nie zgadza sie dla 2 zagranicznych imion, pomijalne.
paneldb -N -e "
  select 
    (case substring(substring_index(SUBSTRING_INDEX(m.first_name, ' ', 1), ',', 1), -1) = 'a' when true then true else false end) kobieta,
    m.email,
    substring_index(SUBSTRING_INDEX(m.first_name, ' ', 1), ',', 1) first_name,
    region_id,
  	case
    	when m.birth_year < 1953 then '1930-52'
    	when m.birth_year >= 1953 and m.birth_year < 1963 then '1953-62'
    	when m.birth_year >= 1963 and m.birth_year < 1973 then '1963-72'
    	when m.birth_year >= 1973 and m.birth_year < 1983 then '1973-82'
    	when m.birth_year >= 1983 and m.birth_year < 1993 then '1983-92'
    	when m.birth_year >= 1993 and m.birth_year < 2000 then '1993-99'
    	else 'wiek_nieznany'
  	end as birth_decade,
    r.name region_name
  from members m join regions r on m.region_id = r.id where
  m.status = 2" | awk "{printf \"(%s,'%s','%s',%s,'%s','%s'),\n\", \$1,\$2,\$3,\$4,\$5,\$6\$7}" > $OUTPUT_FILE.tmp

(
  echo 'create temp table extras as select * from ( VALUES '
  cat ${OUTPUT_FILE}.tmp | tr "\n" "%" | rev | cut -c3- | rev | tr "%" "\n"
  echo ") as t (kobieta, email, first_name, region_id, birth_decade, region_name);"
) > ${OUTPUT_FILE}

rm -f ${OUTPUT_FILE}.tmp

