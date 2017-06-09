#!/bin/bash

# Skrypt analizuje daze danych forum (joinujac z plciami wyciagnietymi
# wczesniej z bazy danych panelu) i wylicza na jej podstawie rozne
# metryki dotyczace uzycia forum przez poszczegolne grupu demograficzne.

set -e

# Zmienne ktore mozna przekazac przy uruchamianiu skryptu
PERCENTYLI="${PERCENTYLI:-10}"
GRUPUJ_WEDLUG="${GRUPUJ_WEDLUG:-posts_read}"
TYGODNI="${TYGODNI:-13}"
MINIMUM_ODWIEDZIN="${MINIMUM_ODWIEDZIN:-2}"
DATA_KONCOWA="${DATA_KONCOWA:-date '2017-05-28' + time '23:59:59'}"

echo "
select '
-- PARAMETRY:
-- PERCENTYLI: $PERCENTYLI
-- GRUPUJ_WEDLUG: $GRUPUJ_WEDLUG
-- TYGODNI: $TYGODNI
-- MINIMUM_ODWIEDZIN: $MINIMUM_ODWIEDZIN
-- DATA_KONCOWA: $(echo $DATA_KONCOWA | tr -d "'")
';
"

REGIONY_XL="'Warszawa'"
REGIONY_L="'Kraków', 'Wrocław', 'Poznań'"
REGIONY_M="'Białystok', 'Gdańsk', 'Gdynia', 'Katowice', 'Świętokrzyski', 'Łódź', 'Lubelskie', 'Szczecin', 'Bielsko-Biała'"

REGIONY_XL_COND="(region_name in ($REGIONY_XL))"
REGIONY_L_COND="(region_name in ($REGIONY_L))"
REGIONY_M_COND="(region_name in ($REGIONY_M))"
REGIONY_S_COND="(region_name not in ($REGIONY_XL, $REGIONY_L, $REGIONY_M))"

REGION_EXPR="
  (case when $REGIONY_XL_COND then 'XL' else
    (case when $REGIONY_L_COND then 'L' else
      (case when $REGIONY_M_COND then 'M' else
        (case when $REGIONY_S_COND then 'S' else
          'UNKNOWN'
        end)
      end)
    end)
  end)"

SINCE="$DATA_KONCOWA - INTERVAL '$TYGODNI week'"
TO="$DATA_KONCOWA"

# Dla kazdego user_id policz liczbe postow, tematow, lajkow itd. w danym okresie.
# Tabela 'extras' zostala wyeksportowana z bazy danych panelu, jest wczytywana
# ponizej.
USER_ACTIVITY_DATA=" (
  SELECT
  u.id user_id,
  SUM(CASE WHEN ua.action_type = 2 THEN 1 ELSE 0 END) likes_received,
  SUM(CASE WHEN ua.action_type = 1 THEN 1 ELSE 0 END) likes_given,
  COALESCE((SELECT COUNT(topic_id) FROM topic_views AS v WHERE v.user_id = u.id AND v.viewed_at >= $SINCE AND v.viewed_at <= $TO), 0) topics_entered,
  COALESCE((SELECT COUNT(id) FROM user_visits AS uv WHERE uv.user_id = u.id AND uv.visited_at >= $SINCE AND uv.visited_at <= $TO), 0) days_visited,
  COALESCE((SELECT SUM(posts_read) FROM user_visits AS uv2 WHERE uv2.user_id = u.id AND uv2.visited_at >= $SINCE AND uv2.visited_at <= $TO), 0) posts_read,
  SUM(CASE WHEN ua.action_type = 4 THEN 1 ELSE 0 END) topic_count,
  SUM(CASE WHEN ua.action_type = 5 THEN 1 ELSE 0 END) post_count,
  SUM(CASE WHEN ua.action_type = 5 THEN length(p.raw) ELSE 0 END) post_length
  FROM users AS u
  LEFT OUTER JOIN user_actions AS ua ON ua.user_id = u.id
  LEFT OUTER JOIN topics AS t ON ua.target_topic_id = t.id AND t.archetype = 'regular'
  LEFT OUTER JOIN posts AS p ON ua.target_post_id = p.id
  LEFT OUTER JOIN categories AS c ON t.category_id = c.id
  WHERE u.active
  AND NOT u.blocked
  AND COALESCE(ua.created_at, $SINCE) >= $SINCE AND COALESCE(ua.created_at, $TO) <= $TO
  AND t.deleted_at IS NULL
  AND COALESCE(t.visible, true)
  AND p.deleted_at IS NULL
  AND (NOT (COALESCE(p.hidden, false)))
  AND COALESCE(p.post_type, 1) = 1
  AND u.id > 0
  GROUP BY u.id
) "

PLEC="(case when kobieta=1 then 'kobieta' else 'mezczyzna' end)"
PLEC1="(case when e1.kobieta=1 then 'kobieta' else 'mezczyzna' end)"
PLEC2="(case when e2.kobieta=1 then 'kobieta' else 'mezczyzna' end)"

METRICS="
likes_received, ntile($PERCENTYLI) over (order by likes_received) likes_received_percentile,
likes_given,    ntile($PERCENTYLI) over (order by likes_given) likes_given_percentile,
topics_entered, ntile($PERCENTYLI) over (order by topics_entered) topics_entered_percentile,
days_visited,   ntile($PERCENTYLI) over (order by days_visited) days_visited_percentile,
posts_read,     ntile($PERCENTYLI) over (order by posts_read) posts_read_percentile,
post_length,    ntile($PERCENTYLI) over (order by post_length) post_length_percentile,
topic_count,    ntile($PERCENTYLI) over (order by topic_count) topic_count_percentile,
post_count,     ntile($PERCENTYLI) over (order by post_count) post_count_percentile"

echo "select 'LADUJEMY DANE Z PANELU, KTORYCH NIE MA W BAZIE FORUM:';"
cat panel_data.sql

# Dla kazdego uzytkownika (user_id) przygotowujemy zestaw metryk:
ACTIVE_USERS_DATA="
SELECT
  u.id user_id,
  u.email,
  e.birth_decade,
  $REGION_EXPR region,
  $PLEC plec,
  $METRICS
from users u
full join extras e
on u.email = e.email         
full join $USER_ACTIVITY_DATA s
on u.id = s.user_id
where  
  e.email is not null
  and days_visited >= $MINIMUM_ODWIEDZIN"

function mediana() {
  echo "percentile_cont(0.5) WITHIN GROUP (ORDER BY $1)"
}

echo "  
select 'JAK POSZCZEGOLNE GRUPY WIEKOWE KORZYSTAJA Z FORUM';
select birth_decade,
  count(*) wielkosc_grupy,
  $(mediana posts_read) mediana_przeczytanych_postow,
  sum(post_count) sumarycznie_postow_napisanych,
  sum(topic_count) sumarycznie_watkow_zalozonych,
  sum(post_length) sumaryczna_dlugosc_postow
from ($ACTIVE_USERS_DATA) s
group by birth_decade
order by birth_decade;

select 'JAK POSZCZEGOLNE GRUPY WIEKOWE KORZYSTAJA Z FORUM, Z PODZIALEM NA PLCI';
select birth_decade, plec,
  count(*) wielkosc_grupy,
  $(mediana posts_read) mediana_przeczytanych_postow,
  sum(post_count) sumarycznie_postow_napisanych,
  sum(topic_count) sumarycznie_watkow_zalozonych,
  sum(post_length) sumaryczna_dlugosc_postow
from ($ACTIVE_USERS_DATA) s
group by birth_decade, plec
order by birth_decade, plec;

select 'JAK KOBIETY/MEZCZYZNI LAJKUJA SIEBIE NAWZAJEM';
select
  $PLEC1 dostajacy_lajka,
  $PLEC2 dajaca_lajka,
  count(*) danych_lajkow
from users u1
full join user_actions ua on ua.user_id = u1.id
full join extras e1 on u1.email = e1.email         
full join users u2 on ua.acting_user_id = u2.id
full join extras e2 on u2.email = e2.email
where e1.email is not null and e2.email is not null and ua.action_type = 2
and COALESCE(ua.created_at, $SINCE) >= $SINCE AND COALESCE(ua.created_at, $TO) <= $TO
group by dostajacy_lajka, dajaca_lajka;

select 'DECYLE PRZECZYTANYCH POSTOW';
select region, plec, posts_read_percentile, count(*) from ($ACTIVE_USERS_DATA) s
group by posts_read_percentile, region, plec
order by posts_read_percentile, region, plec;

select 'DECYLE NAPISANYCH POSTOW';
select region, plec, post_count_percentile, count(*) from ($ACTIVE_USERS_DATA) s
group by post_count_percentile, region, plec
order by post_count_percentile, region, plec;

select 'DECYLE SUMARYCZNEJ DLUGOSCI NAPISANYCH POSTOW';
select region, plec, post_length_percentile, count(*) from ($ACTIVE_USERS_DATA) s
group by post_length_percentile, region, plec
order by post_length_percentile, region, plec;

select 'DECYLE ROZPOCZETYCH WATKOW';
select region, plec, topic_count_percentile, count(*) from ($ACTIVE_USERS_DATA) s
group by topic_count_percentile, region, plec
order by topic_count_percentile, region, plec;

select 'JAK DUZO PISZA POSZCZEGOLNE PLCI';
select plec,
  avg(post_count) srednia_ilosc_postow_napisanych,
  avg(post_length/(post_count+1)) srednia_dlugosc_posta,
  sum(post_length) sumaryczna_dlugosc_postow,
  count(*) aktywnych_uzytkownikow_czek
from ($ACTIVE_USERS_DATA) s
group by plec;

select 'JAK DUZO PISZA POSZCZEGOLNE PLCI W POSZCZEGOLNYCH REGIONACH';
select region, plec,
  avg(post_count) srednia_ilosc_postow_napisanych,
  avg(post_length/(post_count+1)) srednia_dlugosc_posta,
  sum(post_length) sumaryczna_dlugosc_postow,
  count(*) aktywnych_uzytkownikow_czek
from ($ACTIVE_USERS_DATA) s
group by region, plec
order by region, plec;

-- select 'ZANONIMIZOWANE DANE UZYTKOWNIKOW';
-- select region, plec, $METRICS from ($ACTIVE_USERS_DATA) s;
"
