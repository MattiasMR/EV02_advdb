// setupAthena.js
import { runAthenaQuery } from "./athenaClient.js";

async function main() {
  await runAthenaQuery(`CREATE DATABASE IF NOT EXISTS ev02;`, { database: undefined });
  console.log("OK: CREATE DATABASE ev02");

  const createTable = `
CREATE EXTERNAL TABLE IF NOT EXISTS ev02.movies (
  id                       string,
  original_title           string,
  release_date             string,
  budget                   double,
  revenue                  double,
  popularity               double,
  vote_average             string,
  vote_count               string,
  production_companies     string,
  genres                   string
)
STORED AS PARQUET
LOCATION 's3://ev02-mattiasmorales/input/';
  `;
  await runAthenaQuery(createTable, { database: "ev02" });
  console.log("OK: CREATE TABLE ev02.movies");

  // 3) Smoke test
  const smoke = await runAthenaQuery(`
    SELECT count(*) AS n,
           min(release_date) AS min_date,
           max(release_date) AS max_date
    FROM ev02.movies
  `, { database: "ev02" });
  console.table(smoke.rows);
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
