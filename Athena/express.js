import express from "express";
import { runAthenaQuery } from "./athenaClient.js";
import { QUERIES } from "./queries.js";

const app = express();

app.get("/q/:name", async (req, res) => {
  try {
    const name = req.params.name;
    const builder = QUERIES[name];
    if (!builder) return res.status(400).json({ error: "query desconocida", disponibles: Object.keys(QUERIES) });

    const rawMin = req.query.min_votes;
    const minVotes = Number.isFinite(+rawMin) ? Math.max(0, Math.floor(+rawMin)) : undefined;
    const sql = typeof builder === "function" ? builder(minVotes) : builder;

    const data = await runAthenaQuery(sql, { database: "ev02" });
    res.json(data);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.listen(3000, () => console.log("http://localhost:3000"));
