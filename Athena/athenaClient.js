// athenaClient.js
import {
  AthenaClient,
  StartQueryExecutionCommand,
  GetQueryExecutionCommand,
  GetQueryResultsCommand
} from "@aws-sdk/client-athena";
import { readFileSync, existsSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));

// Función para cargar configuración
function loadConfig() {
  const configPath = join(__dirname, '.env.json');
  if (existsSync(configPath)) {
    try {
      const config = JSON.parse(readFileSync(configPath, 'utf8'));
      return config;
    } catch (error) {
      console.warn("Warning: No se pudo leer .env.json, usando valores por defecto");
    }
  }
  return null;
}

// Función para obtener configuración actual (recarga dinámicamente)
function getCurrentConfig() {
  return loadConfig();
}

const initialConfig = loadConfig();

const REGION   = process.env.AWS_REGION      || "us-east-1";
const DATABASE = process.env.ATHENA_DB       || "ev02";
const OUTPUT   = process.env.ATHENA_OUTPUT   || initialConfig?.resultsLocation || "s3://ev02-mattiasmorales/results/";
const WORKGROUP= process.env.ATHENA_WORKGROUP|| "primary";
const CATALOG  = process.env.ATHENA_CATALOG  || "AwsDataCatalog";

const athena = new AthenaClient({ region: REGION });

export async function runAthenaQuery(query, { database = DATABASE, resultsLocation = null } = {}) {
  // Si no se especifica resultsLocation, intentar usar la configuración actual
  let outputLocation = resultsLocation;
  if (!outputLocation) {
    const currentConfig = getCurrentConfig();
    outputLocation = currentConfig?.resultsLocation || OUTPUT;
  }
  
  const ctx = database
    ? { Catalog: CATALOG, Database: database }
    : { Catalog: CATALOG };

  const params = {
    QueryString: query,
    QueryExecutionContext: ctx,
    ResultConfiguration: { OutputLocation: outputLocation },
    WorkGroup: WORKGROUP
  };

  const start = await athena.send(new StartQueryExecutionCommand(params));
  const qid = start.QueryExecutionId;

  for (;;) {
    await new Promise(r => setTimeout(r, 1000));
    const st = await athena.send(new GetQueryExecutionCommand({ QueryExecutionId: qid }));
    const state = st.QueryExecution.Status.State;
    if (state === "SUCCEEDED") break;
    if (state === "FAILED" || state === "CANCELLED") {
      const reason = st.QueryExecution.Status.AthenaError?.ErrorMessage || state;
      throw new Error(`Consulta falló: ${reason}`);
    }
  }

  let nextToken;
  const rows = [];
  let headers = [];
  do {
    const res = await athena.send(new GetQueryResultsCommand({
      QueryExecutionId: qid,
      NextToken: nextToken
    }));
    nextToken = res.NextToken;

    if (!headers.length && res.ResultSet?.ResultSetMetadata?.ColumnInfo) {
      headers = res.ResultSet.ResultSetMetadata.ColumnInfo.map(c => c.Name);
    }
    for (const r of res.ResultSet?.Rows || []) {
      const vals = (r.Data || []).map(d => d.VarCharValue ?? null);
      if (headers.length && vals.join("|") === headers.join("|")) continue; // salta cabecera
      rows.push(Object.fromEntries(headers.map((h, i) => [h, vals[i]])));
    }
  } while (nextToken);

  return { queryId: qid, columns: headers, rows };
}
