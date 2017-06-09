db="psql -F, --no-align  -h db1 -U $USER discourse"

set -e

T=${T:-1}
M=${M:-1}

(cat preprocessed/panel_data.sql
echo "select period_name, minimum_odwiedzin minimum_dni_odwiedzin from period_names";
) | $db

for minimum_odwiedzin in $M 0; do
  for regiony in `
    ` "region_name = region_name " `
    ` "region_name in ('Warszawa') " `
    ` "region_name in ('Kraków', 'Wrocław', 'Poznań')" `
    ` "region_name in ('Białystok', 'Gdańsk', 'Gdynia', 'Katowice', 'Świętokrzyski', 'Łódź', 'Lubelskie', 'Szczecin', 'Bielsko-Biała')" `
    ` "region_name not in ('Białystok', 'Gdańsk', 'Gdynia', 'Katowice', 'Świętokrzyski', 'Łódź', 'Lubelskie', 'Szczecin', 'Bielsko-Biała', 'Kraków', 'Wrocław', 'Poznań', 'Warszawa')"
  do
    echo
    echo "$regiony - okres $T tygodnii" | tr -d "'" | tr "," ";"
    if [ $minimum_odwiedzin != "0" ]; then
      echo tylko osoby z minimum $M odwiedzin na $T tygodnii
    else
      echo wszystkie osoby w tym te bez minimum odwiedzin
    fi
    max_weeks_back=104
    DB="$db"
    for weeks_back in $(seq $max_weeks_back -1 0); do
      since="date '2017-05-28' + time '23:59:59' - INTERVAL '$((weeks_back + $T)) week'"
      to="date '2017-05-28' + time '23:59:59' - INTERVAL '$weeks_back week'"
          (cat preprocessed/panel_data.sql;
           echo "select s.date date,";
           for metric in 'sum(1 * ppp) #licznosc'\
                'round(sum(topics_entered * ppp)*1./sum(1 * ppp),3) #watkow_czytanych'\
                'round(sum(days_visited * ppp)*1./sum(1 * ppp),3) #dni_odwiedzin'\
                'round(sum(posts_read * ppp)*1./sum(1 * ppp),3) #postow_przeczytanych'\
                'round(sum(likes_given * ppp)*1./sum(1 * ppp),3) #lajkow_danych'\
                'round(sum(likes_received * ppp)*1./sum(1 * ppp),3) #lajkow_otrzymanych'\
                'round(sum(post_count * ppp)*1./sum(1 * ppp),3) #postow_napisanych'\
                'round(sum(topic_count * ppp)*1./sum(1 * ppp),3) #watkow_zalozonych'; do
             forumula="$(echo "$metric" | cut -d'#' -f-1)"
             name="$(echo "$metric" | cut -d'#' -f2-)"
             for kobieta in 1 0; do
               p="(case when e.kobieta=$kobieta then 1 else 0 end)"
               echo -n "(COALESCE($forumula,1))" | sed -e "s/ppp/$p/g"
               if [ $kobieta = "1" ]; then 
                 echo -n ' * 100. / '
               else
                 echo "${name}_k_do_m,"
               fi
             done
           done
           echo "
            '${T}_tygodni' okres
            from users u
            full join extras e
            on u.email = e.email         
            full join 
      (
      SELECT
      $to date,
      u.id user_id,
      SUM(CASE WHEN ua.action_type = 2 THEN 1 ELSE 0 END) likes_received,
      SUM(CASE WHEN ua.action_type = 1 THEN 1 ELSE 0 END) likes_given,
      COALESCE((SELECT COUNT(topic_id) FROM topic_views AS v WHERE v.user_id = u.id AND v.viewed_at >= $since AND v.viewed_at <= $to), 0) topics_entered,
      COALESCE((SELECT COUNT(id) FROM user_visits AS uv WHERE uv.user_id = u.id AND uv.visited_at >= $since AND uv.visited_at <= $to), 0) days_visited,
      COALESCE((SELECT SUM(posts_read) FROM user_visits AS uv2 WHERE uv2.user_id = u.id AND uv2.visited_at >= $since AND uv2.visited_at <= $to), 0) posts_read,
      SUM(CASE WHEN ua.action_type = 4 THEN 1 ELSE 0 END) topic_count,
      SUM(CASE WHEN ua.action_type = 5 THEN 1 ELSE 0 END) post_count
      FROM users AS u
      LEFT OUTER JOIN user_actions AS ua ON ua.user_id = u.id
      LEFT OUTER JOIN topics AS t ON ua.target_topic_id = t.id AND t.archetype = 'regular'
      LEFT OUTER JOIN posts AS p ON ua.target_post_id = p.id
      LEFT OUTER JOIN categories AS c ON t.category_id = c.id
      WHERE u.active
      AND NOT u.blocked
      AND COALESCE(ua.created_at, $since) >= $since AND COALESCE(ua.created_at, $to) <= $to
      AND t.deleted_at IS NULL
      AND COALESCE(t.visible, true)
      AND p.deleted_at IS NULL
      AND (NOT (COALESCE(p.hidden, false)))
      AND COALESCE(p.post_type, 1) = 1
      AND u.id > 0
      GROUP BY u.id
      ) s
            on u.id = s.user_id
      where  
            e.email is not null
            and days_visited >= $minimum_odwiedzin
            and $regiony
            group by s.date order by okres; 
          ") |  $DB | { grep -v SELECT || true; }
      DB="$db -t"
    done
  done
done


