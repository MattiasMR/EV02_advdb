# EV02 — MapReduce README 

Dataset de entrada (CSV limpio **UTF-8**): columnas mínimas
```
id, original_title, release_date, budget, revenue, popularity, vote_average, vote_count, production_companies, genres
```
> `production_companies` y `genres` vienen como listas estilo Python (se parsean en el mapper con `ast.literal_eval`).


---

## 1) Concentración de mercado por década (Top-5 productoras)

**Qué:** participación % de revenue de las **5** productoras con mayor facturación en cada década, referida al **TOTAL único** de la década (suma de `revenue` por película, sin sobrecontar coproducciones).  
**Campos:** `production_companies`, `revenue`, `release_date`.

**Lógica (resumen):**
- Mapper emite por película: `MC|TOTAL|<decade> \t <revenue>` (denominador único) y por productora **prorrateado** `rev/n_prod`.
- Reducer agrega por productora y calcula **Top-5** y su **%** sobre el total único.

**Salida (formato):**
- Resumen por década:  
  `Década - Etiqueta (Top 5) - % Suma de participaciones de las top 5 productoras - Etiqueta ("Total") - Total`
- Detalle Top-5:  
  `Década - Ranking - ID Productora - Nombre Productora - Revenue de la productora - % de participación` (i=1..5)

---

## 2) ROI vs decil de presupuesto por década

**Qué:** promedio de **ROI** por cada **decil** de **budget** dentro de la década (1=presupuestos más bajos, 10=más altos).  
**Campos:** `budget`, `revenue`, `release_date` (solo `budget > 0`).

**Lógica (resumen):**
- El mapper emite budgets (`ROI|BUDGET`) y pares película (`ROI|MOV` con `budget|revenue`).
- El reducer calcula **deciles** por década (nearest-rank) y promedia `ROI=(revenue-budget)/budget` por decil.  
  Soporta **umbrales de muestra** por `-cmdenv` (p. ej. `MIN_MOVIES_ROI`, `MIN_BUDGETS`).


**Salida (formato):**
```
Década - Decil - Promedio ROI (revenue−budget) / budget - cantidad de películas
```

---

## 3) “Efecto debut” de productoras

**Qué:** comparar **ROI** y **popularidad** de la **primera película** (por fecha) de cada productora vs el **promedio** de sus siguientes películas.  
**Campos:** `production_companies`, `release_date`, `revenue`, `budget`, `popularity`.

**Lógica (resumen):**
- Mapper emite por productora: `DEBUT|<cid>|<cname> \t <date>|<roi>|<pop>` (una línea por película).  
- Reducer:
  - **Excluye** productoras cuyo **debut** no tenga ROI válido.
  - Calcula promedios para el **resto** y añade **ΔROI** y **ΔPop** (debut − promedio resto).


**Salida (formato):**
```
idProductora|nombreProductora|fechaEstreno|debutROI|debutPopularidad|noDebutPromedioROI|noDebutPromedioPopularidad|nPeliculasNoDebut|deltaROI|deltaPopularidad
```

---

## 4) “Hidden gems” por década

**Qué:** películas con **alta calificación** (≥ p90 de `vote_average`) y **baja popularidad** (≤ p10 de `popularity`) **dentro de la década**, con **mínimo de votos** requerido.  
**Campos:** `vote_average`, `popularity`, `release_date` (y `vote_count` para el filtro).

**Lógica (resumen):**
- Mapper emite `HG|MOV|<decade> \t <id>|<title>|<va>|<pop>|<vote_count>` y listas `HG|VA`/`HG|POP` para percentiles por década.
- Reducer calcula p90/p10 por década y **filtra por `MIN_VOTES`** antes de listar.


**Salida (formato):**
```
Década - idPelicula - titulo - promedioVotos - popularidad - nVotos
```

---

## 5) Estabilidad de calificación según nº de géneros

**Qué:** `stddev(vote_average)` por **bucket** de cantidad de géneros (1, 2, 3+) y década, **ponderado por `vote_count`** (varianza/σ poblacional).  
**Campos:** `genres`, `vote_average`, `release_date` (y `vote_count` para el peso).

**Lógica (resumen):**
- Mapper emite: `STAB|<decade>|<bucket> \t <vote_average>|<vote_count>`
- Reducer acumula con pesos (ignora `vote_count ≤ 0`) y calcula **media** y **desv. estándar poblacional ponderada**.



**Salida (formato):**
```
década - bucket de géneros - promedioVotos - std de promedio de votos - nPeliculas
```

---
