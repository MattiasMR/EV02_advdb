// testScript.js
import { runAthenaQuery } from "./athenaClient.js";
import { QUERIES } from "./queries.js";

const qname = process.env.Q || process.argv[2] || "q1";
const rawMin = process.env.MIN_VOTES ?? process.argv[3]; // opcional
const minVotes = Number.isFinite(+rawMin) ? Math.max(0, Math.floor(+rawMin)) : undefined;

(async () => {
  try {
    const builder = QUERIES[qname];
    if (!builder) {
      console.error(`Nombre de query inválido: ${qname}`);
      console.error("Disponibles:", Object.keys(QUERIES).join(", "));
      process.exit(1);
    }
    const sql = typeof builder === "function" ? builder(minVotes) : builder;
    const res = await runAthenaQuery(sql, { database: "ev02" });
    console.table(res.rows);
  } catch (e) {
    console.error(e);
    process.exit(1);
  }
})();
