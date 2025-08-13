import { runAthenaQuery } from "./athenaClient.js";

(async () => {
  try {

    const sql = `
SELECT original_title,
       regexp_extract_all(production_companies, '"name"\\s*:\\s*"([^"]+)"') AS names_dq,
       regexp_extract_all(production_companies, '''name''\\s*:\\s*''([^'']+)''') AS names_sq
FROM ev02.movies
WHERE production_companies IS NOT NULL AND production_companies <> ''
LIMIT 5;

`;
    const res = await runAthenaQuery(sql, { database: "ev02" });
    console.table(res.rows);
  } catch (e) {
    console.error(e);
    process.exit(1);
  }
})();
