# Athena Setup Automatizado

Este proyecto configura automáticamente AWS S3 y Athena para trabajar con datasets de películas.

## Prerequisitos

1. **AWS CLI configurado** con credenciales válidas
2. **Bucket S3 existente** al que tengas acceso de lectura/escritura
3. **Node.js** instalado

## Setup Inicial (Solo una vez)

```bash
# 1. Instalar dependencias
npm install

# 2. Configurar todo automáticamente (reemplaza 'mi-bucket-nombre' con tu bucket existente)
npm run setup mi-bucket-nombre

# 3. Iniciar el servidor
npm run serve
```

### Qué hace el comando `setup`:
- ✅ Verifica acceso al bucket S3 especificado
- ✅ Crea carpetas `input/` y `results/` en el bucket
- ✅ Sube el archivo `movies_metadata_final.parquet` a `input/`
- ✅ Crea la base de datos `ev02` en Athena
- ✅ Crea la tabla `movies` apuntando a los datos en S3
- ✅ Verifica que los datos estén disponibles
- ✅ Guarda la configuración en `.env.json`

## Uso Rápido

```bash
# Setup completo (solo la primera vez)
npm run setup ev02-mattiasmorales

# Iniciar servidor
npm run serve
```

El servidor estará disponible en `http://localhost:3000`

## Endpoints Disponibles

- `GET /q/:name` - Ejecutar consultas predefinidas
- Parámetro opcional: `?min_votes=N` para filtrar por votos mínimos

## Ejemplo de Uso

```bash
# Setup con bucket existente (solo la primera vez)
npm run setup mi-bucket-ev02-2025

# Iniciar servidor
npm run serve

# En otra terminal, probar endpoints
curl http://localhost:3000/q/q1
curl "http://localhost:3000/q/q2?min_votes=1000"
```

## Archivos Importantes

- `.env.json` - Configuración del bucket y rutas S3 (se crea automáticamente)
- `movies_metadata_final.parquet` - Dataset de películas (debe estar presente)

## Notas

- **El bucket S3 debe existir previamente** - no se crean buckets automáticamente
- Asegúrate de tener permisos de lectura/escritura en el bucket
- El setup solo necesita ejecutarse una vez por bucket
