import { runAthenaQuery } from "./athenaClient.js";
import { S3Client, HeadBucketCommand, PutObjectCommand } from "@aws-sdk/client-s3";
import { readFileSync, writeFileSync, existsSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REGION = process.env.AWS_REGION || "us-east-1";
const s3 = new S3Client({ region: REGION });

async function verifyBucketAccess(bucketName) {
  try {
    await s3.send(new HeadBucketCommand({ Bucket: bucketName }));
    console.log(`OK: Bucket '${bucketName}' existe y es accesible`);
    return true;
  } catch (error) {
    if (error.name === 'NotFound') {
      console.log(`ERROR: El bucket '${bucketName}' no existe`);
      console.log(`   Crea el bucket manualmente desde la consola AWS antes de continuar`);
      return false;
    } else if (error.name === 'AccessDenied' || error.name === 'Forbidden') {
      console.log(`ERROR: No tienes acceso al bucket '${bucketName}'`);
      console.log(`   Verifica los permisos o usa un bucket diferente`);
      return false;
    } else {
      console.log(`WARNING: Error verificando bucket: ${error.message}`);
      console.log(`   Continuando asumiendo que el bucket existe...`);
      return true;
    }
  }
}

async function uploadParquetFile(bucketName, parquetPath) {
  try {
    const fileContent = readFileSync(parquetPath);
    const fileName = "movies_metadata_final.parquet";
    
    await s3.send(new PutObjectCommand({
      Bucket: bucketName,
      Key: `input/${fileName}`,
      Body: fileContent,
      ContentType: "application/octet-stream"
    }));
    
    console.log(`OK: Archivo '${fileName}' subido a s3://${bucketName}/input/`);
  } catch (error) {
    throw new Error(`Error subiendo archivo parquet: ${error.message}`);
  }
}

async function createFolders(bucketName) {
  try {
    // Crear carpeta input/ (vacía)
    await s3.send(new PutObjectCommand({
      Bucket: bucketName,
      Key: "input/",
      Body: "",
    }));
    
    // Crear carpeta results/ (vacía)
    await s3.send(new PutObjectCommand({
      Bucket: bucketName,
      Key: "results/",
      Body: "",
    }));
    
    console.log(`OK: Carpetas 'input/' y 'results/' creadas en bucket '${bucketName}'`);
  } catch (error) {
    throw new Error(`Error creando carpetas: ${error.message}`);
  }
}

function saveConfig(bucketName) {
  const config = {
    bucketName,
    inputLocation: `s3://${bucketName}/input/`,
    resultsLocation: `s3://${bucketName}/results/`,
    setupDate: new Date().toISOString()
  };
  
  const configPath = join(__dirname, '.env.json');
  writeFileSync(configPath, JSON.stringify(config, null, 2));
  console.log(`OK: Configuración guardada en .env.json`);
  return config;
}

async function main() {
  const bucketName = process.argv[2];
  
  if (!bucketName) {
    console.error("ERROR: Debes proporcionar el nombre del bucket como argumento");
    console.log("Uso: npm run setup <nombre-del-bucket>");
    console.log("Ejemplo: npm run setup mi-bucket-ev02");
    process.exit(1);
  }
  
  console.log(`Iniciando setup para bucket: ${bucketName}`);
  
  try {
    // 1. Verificar acceso al bucket
    const bucketAccessible = await verifyBucketAccess(bucketName);
    if (!bucketAccessible) {
      process.exit(1);
    }
    
    // 2. Crear carpetas input/ y results/
    await createFolders(bucketName);
    
    // 3. Subir archivo parquet
    const parquetPath = join(__dirname, "movies_metadata_final.parquet");
    if (!existsSync(parquetPath)) {
      throw new Error(`Archivo parquet no encontrado en: ${parquetPath}`);
    }
    await uploadParquetFile(bucketName, parquetPath);
    
    // 4. Guardar configuración
    const config = saveConfig(bucketName);
    
    // 5. Crear base de datos Athena
    await runAthenaQuery(`CREATE DATABASE IF NOT EXISTS ev02;`, { 
      database: undefined,
      resultsLocation: config.resultsLocation 
    });
    console.log("OK: Base de datos 'ev02' creada/verificada");

    // 6. Crear tabla Athena
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
LOCATION '${config.inputLocation}';
    `;
    await runAthenaQuery(createTable, { 
      database: "ev02",
      resultsLocation: config.resultsLocation 
    });
    console.log("OK: Tabla 'ev02.movies' creada/verificada");

    // 7. Verificar datos
    console.log("\nVerificando datos...");
    const smoke = await runAthenaQuery(`
      SELECT count(*) AS total_movies,
             min(release_date) AS fecha_minima,
             max(release_date) AS fecha_maxima
      FROM ev02.movies
    `, { 
      database: "ev02",
      resultsLocation: config.resultsLocation 
    });
    console.table(smoke.rows);
    
    console.log("\nSetup completado exitosamente!");
    console.log(`Bucket: ${bucketName}`);
    console.log(`Datos en: ${config.inputLocation}`);
    console.log(`Resultados en: ${config.resultsLocation}`);
    console.log("\nEjecuta el siguiente comando para iniciar el servidor:");
    console.log("   npm run serve");
    console.log("\nEl servidor estará disponible en: http://localhost:3000");
    
  } catch (error) {
    console.error("Error durante el setup:", error.message);
    process.exit(1);
  }
}

main().catch(err => {
  console.error("ERROR inesperado:", err);
  process.exit(1);
});
