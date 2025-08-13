// queries.js
// q1: Concentración de mercado por década (Top-5 productoras por revenue)
// q2: ROI vs decil de presupuesto por década
// q3: "Efecto debut" de productoras (ROI y popularidad debut vs posteriores)
// q4: Hidden gems por década (top 10% calificación, bottom 20% popularidad)
// q5: Estabilidad de calificación según nº de géneros

export const QUERIES = {
  q1: `
  WITH e AS (
    SELECT
      TRY_CAST(substr(release_date,1,4) AS integer) AS y,
      TRY_CAST(revenue AS double) AS rev,
      production_companies AS pc
    FROM ev02.movies
    WHERE release_date IS NOT NULL
      AND TRY_CAST(revenue AS double) > 0
  ),
  arr AS (
    SELECT
      y, rev,
      regexp_extract_all(pc, '"name"\\s*:\\s*"([^"]+)"')      AS names_dq,
      regexp_extract_all(pc, '''name''\\s*:\\s*''([^'']+)''') AS names_sq
    FROM e
  ),
  cnt AS (
    SELECT
      y, rev,
      COALESCE(cardinality(names_dq),0) + COALESCE(cardinality(names_sq),0) AS n_comp
    FROM arr
  ),
  pc_dq AS (
    SELECT
      (y - (y % 10)) AS decade,
      name AS company,
      CASE WHEN c.n_comp > 0 THEN rev / c.n_comp END AS rev_share
    FROM arr a
    JOIN cnt c USING (y, rev)
    CROSS JOIN UNNEST(COALESCE(a.names_dq, CAST(ARRAY[] AS ARRAY(VARCHAR)))) AS t(name)
    WHERE y IS NOT NULL
  ),
  pc_sq AS (
    SELECT
      (y - (y % 10)) AS decade,
      name AS company,
      CASE WHEN c.n_comp > 0 THEN rev / c.n_comp END AS rev_share
    FROM arr a
    JOIN cnt c USING (y, rev)
    CROSS JOIN UNNEST(COALESCE(a.names_sq, CAST(ARRAY[] AS ARRAY(VARCHAR)))) AS t(name)
    WHERE y IS NOT NULL
  ),
  pc AS (
    SELECT * FROM pc_dq WHERE company IS NOT NULL AND company <> ''
    UNION ALL
    SELECT * FROM pc_sq WHERE company IS NOT NULL AND company <> ''
  ),
  agg AS (
    SELECT decade, company, SUM(rev_share) AS revenue
    FROM pc
    GROUP BY 1,2
  ),
  tot AS (
    SELECT (y - (y % 10)) AS decade, SUM(rev) AS total_rev
    FROM e
    GROUP BY 1
  ),
  ranked AS (
    SELECT
      a.decade,
      a.company,
      a.revenue,
      t.total_rev,
      (a.revenue / t.total_rev) AS pct_of_total,
      row_number() OVER (PARTITION BY a.decade ORDER BY a.revenue DESC, a.company ASC) AS rn
    FROM agg a
    JOIN tot t ON a.decade = t.decade
  ),
  t5 AS (
    SELECT decade, SUM(revenue) AS top5_rev
    FROM ranked
    WHERE rn <= 5
    GROUP BY decade
  ),
  final AS (
    SELECT
      r.decade,
      r.rn AS rank,
      r.company,
      r.revenue,
      r.pct_of_total,
      (t5.top5_rev / r.total_rev) AS top5_share
    FROM ranked r
    JOIN t5 ON r.decade = t5.decade
    WHERE r.rn <= 5
  )
  SELECT decade, top5_share, rank, company, revenue, pct_of_total
  FROM final
  ORDER BY decade, rank
  `,
  
  
  q1_general: `
  WITH e AS (
    SELECT
      TRY_CAST(substr(release_date,1,4) AS integer) AS y,
      TRY_CAST(revenue AS double) AS rev,
      production_companies
    FROM ev02.movies
    WHERE release_date IS NOT NULL
      AND TRY_CAST(revenue AS double) > 0
  ),
  arr AS (
    SELECT
      y, rev,
      regexp_extract_all(production_companies, '"name"\\s*:\\s*"[^"]+"')       AS a_dq,
      regexp_extract_all(production_companies, '''name''\\s*:\\s*''[^'']+''')  AS a_sq
    FROM e
  ),
  cnt AS (
    SELECT y, rev,
           COALESCE(cardinality(a_dq),0) + COALESCE(cardinality(a_sq),0) AS n_comp
    FROM arr
  ),
  pc_dq AS (
    SELECT (y - (y % 10)) AS decade,
           COALESCE(regexp_extract(s, '"name"\\s*:\\s*"([^"]+)"'),
                    regexp_replace(s, '^.*"name"\\s*:\\s*"', ''),
                    s) AS company,
           CASE WHEN c.n_comp>0 THEN rev / c.n_comp END AS rev_share
    FROM arr a
    JOIN cnt c USING (y, rev)
    CROSS JOIN UNNEST(COALESCE(a.a_dq, CAST(ARRAY[] AS ARRAY(VARCHAR)))) AS t(s)
    WHERE y IS NOT NULL
  ),
  pc_sq AS (
    SELECT (y - (y % 10)) AS decade,
           COALESCE(regexp_extract(s, '''name''\\s*:\\s*''([^'']+)'''),
                    regexp_replace(s, '^.*''name''\\s*:\\s*''', ''),
                    s) AS company,
           CASE WHEN c.n_comp>0 THEN rev / c.n_comp END AS rev_share
    FROM arr a
    JOIN cnt c USING (y, rev)
    CROSS JOIN UNNEST(COALESCE(a.a_sq, CAST(ARRAY[] AS ARRAY(VARCHAR)))) AS t(s)
    WHERE y IS NOT NULL
  ),
  pc AS (
    SELECT * FROM pc_dq WHERE company IS NOT NULL AND company <> ''
    UNION ALL
    SELECT * FROM pc_sq WHERE company IS NOT NULL AND company <> ''
  ),
  agg AS (
    SELECT decade, company, SUM(rev_share) AS revenue
    FROM pc
    GROUP BY 1,2
  ),
  top5 AS (
    SELECT decade, SUM(revenue) AS top5_rev
    FROM (
      SELECT decade, company, revenue,
             row_number() OVER (PARTITION BY decade ORDER BY revenue DESC) AS rn
      FROM agg
    )
    WHERE rn <= 5
    GROUP BY decade
  ),
  tot AS (
    SELECT (y - (y % 10)) AS decade, SUM(rev) AS total_rev
    FROM e
    GROUP BY 1
  )
  SELECT tot.decade AS decade, (top5.top5_rev / tot.total_rev) AS top5_share
  FROM tot
  JOIN top5 ON tot.decade = top5.decade
  ORDER BY tot.decade
  `,
  
  q1_detalle: `
  WITH e AS (
    SELECT
      TRY_CAST(substr(release_date,1,4) AS integer) AS y,
      TRY_CAST(revenue AS double) AS rev,
      production_companies
    FROM ev02.movies
    WHERE release_date IS NOT NULL
      AND TRY_CAST(revenue AS double) > 0
  ),
  arr AS (
    SELECT
      y, rev,
      regexp_extract_all(production_companies, '"name"\\s*:\\s*"[^"]+"')        AS a_dq,
      regexp_extract_all(production_companies, '''name''\\s*:\\s*''[^'']+''')   AS a_sq
    FROM e
  ),
  cnt AS (
    SELECT y, rev,
           COALESCE(cardinality(a_dq),0) + COALESCE(cardinality(a_sq),0) AS n_comp
    FROM arr
  ),
  pc_dq AS (
    SELECT (y - (y % 10)) AS decade,
           trim(BOTH '"'  FROM regexp_replace(s, '^.*"name"\\s*:\\s*"', '')) AS company,
           CASE WHEN c.n_comp>0 THEN rev / c.n_comp END AS rev_share
    FROM arr a
    JOIN cnt c USING (y, rev)
    CROSS JOIN UNNEST(COALESCE(a.a_dq, CAST(ARRAY[] AS ARRAY(VARCHAR)))) AS t(s)
    WHERE y IS NOT NULL
  ),
  pc_sq AS (
    SELECT (y - (y % 10)) AS decade,
           trim(BOTH '''' FROM regexp_replace(s, '^.*''name''\\s*:\\s*''', '')) AS company,
           CASE WHEN c.n_comp>0 THEN rev / c.n_comp END AS rev_share
    FROM arr a
    JOIN cnt c USING (y, rev)
    CROSS JOIN UNNEST(COALESCE(a.a_sq, CAST(ARRAY[] AS ARRAY(VARCHAR)))) AS t(s)
    WHERE y IS NOT NULL
  ),
  pc AS (
    SELECT * FROM pc_dq WHERE company IS NOT NULL AND company <> ''
    UNION ALL
    SELECT * FROM pc_sq WHERE company IS NOT NULL AND company <> ''
  ),
  agg AS (
    SELECT decade, company, SUM(rev_share) AS revenue
    FROM pc
    GROUP BY 1,2
  ),
  tot AS (
    SELECT (y - (y % 10)) AS decade, SUM(rev) AS total_rev
    FROM e
    GROUP BY 1
  ),
  ranked AS (
    SELECT a.decade, a.company, a.revenue, t.total_rev,
           row_number() OVER (PARTITION BY a.decade ORDER BY a.revenue DESC) AS rn
    FROM agg a
    JOIN tot t ON a.decade = t.decade
  )
  SELECT
    decade,
    rn AS rank,
    company,
    revenue,
    (revenue / total_rev) AS pct_of_total
  FROM ranked
  WHERE rn <= 5
  ORDER BY decade, rank
  `,

  q2: `
  WITH base AS (
    SELECT
      (TRY_CAST(substr(release_date,1,4) AS integer) - (TRY_CAST(substr(release_date,1,4) AS integer) % 10)) AS decade,
      TRY_CAST(revenue AS double) AS rev,
      TRY_CAST(budget  AS double) AS bud
    FROM ev02.movies
    WHERE TRY_CAST(budget AS double) > 0 AND release_date IS NOT NULL
  ),
  w AS (
    SELECT *,
          ntile(10) OVER (PARTITION BY decade ORDER BY bud) AS bud_decile,
          ((rev - bud) / NULLIF(bud,0)) AS roi
    FROM base
  )
  SELECT decade, bud_decile, AVG(roi) AS avg_roi, COUNT(*) AS n
  FROM w
  GROUP BY 1,2
  ORDER BY decade, bud_decile;
  `,

  q3: `
  WITH e AS (
    SELECT
      CAST(try(date_parse(release_date, '%Y-%m-%d')) AS date) AS rdate,
      TRY_CAST(revenue AS double)    AS rev,
      TRY_CAST(budget  AS double)    AS bud,
      TRY_CAST(popularity AS double) AS pop,
      production_companies
    FROM ev02.movies
  ),
  arr AS (
    SELECT
      rdate, rev, bud, pop,
      regexp_extract_all(production_companies, '"name"\\s*:\\s*"[^"]+"')        AS a_dq,
      regexp_extract_all(production_companies, '''name''\\s*:\\s*''[^'']+''')   AS a_sq
    FROM e
  ),
  pc_dq AS (
    SELECT
      rdate,
      pop,
      CASE WHEN bud > 0 THEN (rev - bud) / bud END AS roi,   -- ROI consistente con q5
      trim(BOTH '"' FROM regexp_replace(s, '^.*"name"\\s*:\\s*"', '')) AS company
    FROM arr
    CROSS JOIN UNNEST(COALESCE(a_dq, CAST(ARRAY[] AS ARRAY(VARCHAR)))) AS t(s)
  ),
  pc_sq AS (
    SELECT
      rdate,
      pop,
      CASE WHEN bud > 0 THEN (rev - bud) / bud END AS roi,   -- ROI consistente con q5
      trim(BOTH '''' FROM regexp_replace(s, '^.*''name''\\s*:\\s*''', '')) AS company
    FROM arr
    CROSS JOIN UNNEST(COALESCE(a_sq, CAST(ARRAY[] AS ARRAY(VARCHAR)))) AS t(s)
  ),
  pc_clean AS (
    SELECT rdate, pop, roi, company
    FROM (
      SELECT * FROM pc_dq
      UNION ALL
      SELECT * FROM pc_sq
    )
    WHERE company IS NOT NULL AND company <> '' AND rdate IS NOT NULL
  ),
  ranked AS (
    SELECT
      company, rdate, roi, pop,
      row_number() OVER (PARTITION BY company ORDER BY rdate ASC) AS rn
    FROM pc_clean
  )
  SELECT
    company,
    MIN(CASE WHEN rn = 1 THEN rdate END)        AS debut_date,   -- agregado
    AVG(CASE WHEN rn = 1 THEN roi END)          AS debut_roi,
    AVG(CASE WHEN rn > 1 THEN roi END)          AS post_roi,
    AVG(CASE WHEN rn = 1 THEN pop END)          AS debut_pop,
    AVG(CASE WHEN rn > 1 THEN pop END)          AS post_pop,
    SUM(CASE WHEN rn = 1 THEN 1 ELSE 0 END)     AS n_debuts,
    SUM(CASE WHEN rn > 1 THEN 1 ELSE 0 END)     AS n_post,
    (AVG(CASE WHEN rn = 1 THEN roi END) - AVG(CASE WHEN rn > 1 THEN roi END)) AS delta_roi,
    (AVG(CASE WHEN rn = 1 THEN pop END) - AVG(CASE WHEN rn > 1 THEN pop END)) AS delta_pop
  FROM ranked
  GROUP BY company
  HAVING SUM(CASE WHEN rn > 1 THEN 1 ELSE 0 END) >= 1
    AND AVG(CASE WHEN rn = 1 THEN roi END) IS NOT NULL
  ORDER BY delta_roi DESC
  LIMIT 200
  `,


  q4: (minVotes = 10) => `
  WITH base AS (
    SELECT
      (TRY_CAST(substr(release_date,1,4) AS integer)
        - (TRY_CAST(substr(release_date,1,4) AS integer) % 10)) AS decade,
      id,
      original_title,
      TRY_CAST(vote_average AS double) AS va,
      TRY_CAST(popularity  AS double)  AS pop,
      TRY_CAST(vote_count  AS bigint)  AS vc
    FROM ev02.movies
  ),
  f AS (
    SELECT * FROM base
    WHERE decade BETWEEN 1870 AND 2100
      AND va  IS NOT NULL
      AND pop IS NOT NULL
      AND vc >= ${Number.isFinite(+minVotes) ? Math.max(0, Math.floor(+minVotes)) : 100}
  ),
  pct AS (
    SELECT decade,
          approx_percentile(va, 0.90) AS p90_va,
          approx_percentile(pop, 0.10) AS p10_pop
    FROM f GROUP BY decade
  )
  SELECT f.decade AS decade, f.id, f.original_title, f.va, f.pop, f.vc
  FROM f
  JOIN pct ON f.decade = pct.decade
  WHERE f.va >= pct.p90_va AND f.pop <= pct.p10_pop
  ORDER BY f.decade, f.va DESC, f.pop ASC
  LIMIT 500
`,



  q5: `
  WITH base AS (
    SELECT
      (TRY_CAST(substr(release_date,1,4) AS integer)
        - (TRY_CAST(substr(release_date,1,4) AS integer) % 10)) AS decade,
      TRY_CAST(vote_average AS double) AS va,
      TRY_CAST(vote_count  AS bigint)  AS vc,
      genres
    FROM ev02.movies
    WHERE vote_average IS NOT NULL
  ),
  gcnt AS (
    SELECT
      decade, va,
      COALESCE(vc, 0) AS vc,
      GREATEST(
        COALESCE(cardinality(regexp_extract_all(genres, '"name"\\s*:\\s*"[^"]+"')), 0),
        COALESCE(cardinality(regexp_extract_all(genres, '''name''\\s*:\\s*''[^'']+''')), 0)
      ) AS n_genres
    FROM base
  ),
  buck AS (
    SELECT
      decade,
      CASE WHEN n_genres >= 3 THEN '3+' ELSE CAST(n_genres AS varchar) END AS bucket,
      va, vc
    FROM gcnt
    WHERE n_genres >= 1
  ),
  w AS (
    SELECT
      decade, bucket,
      SUM(vc)          AS W,
      SUM(vc * va)     AS S1,
      SUM(vc * va*va)  AS S2,
      COUNT(*)         AS n_movies
    FROM buck
    GROUP BY 1,2
  )
  SELECT
    decade, bucket,
    (S1/W)                                       AS mean_va,
    sqrt(GREATEST( (S2/W) - (S1/W)*(S1/W), 0 ))  AS sd_va,
    n_movies                                     AS n
  FROM w
  WHERE W > 0
  ORDER BY decade, bucket
  `
};
