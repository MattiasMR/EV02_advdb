Para la reproducción de los resultados obtenidos en este estudio, se proporcionan las siguientes instrucciones:

### Requisitos Previos
> Tener Node.js versión 18.20.8 instalado. 

El proyecto no ha sido probado con otras versiones.

- Configurar credenciales AWS en `~/.aws/credentials`

> region=us-east-1

--- 

### 4.1 Hadoop (MapReduce)

**Infraestructura requerida:**
- Instancia EC2: `t3.micro`
- OS: Ubuntu Server 24.04 LTS x64
- Almacenamiento: 1x8 GiB gp3

#### Mapper y Reducers:
      mapper.py

      reducerMC.py
      reducerROI.py
      reducerDEBUT.py
      reducerHG.py
      reducerSTAB.py
#### Salidas: 
      ./hadoop/mc
      ./hadoop/roi
      ./hadoop/debut
      ./hadoop/hg
      ./hadoop/stab.

4.1.0. **Instalar Hadoop**
```bash
sudo apt update -y && sudo apt-get upgrade -y
sudo apt install openjdk-8-jdk
wget https://downloads.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
tar -xvzf hadoop-3.3.6.tar.gz
mv hadoop-3.3.6 hadoop
vim .bashrc
```
Te vas al final del archivo y pegas lo siguiente:
```
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=$HOME/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
```
Guardas el archivo

Y terminas con:
```bash
source .bashrc
```

4.1.1. **Clonar el proyecto**
```bash
git init
git remote add origin https://github.com/MattiasMR/EV02_advdb.git
git config core.sparseCheckout true
echo "ApacheHadoop/*" > .git/info/sparse-checkout
git pull origin main
cd ApacheHadoop/
```

4.1.2. **Crear directorio y mover el dataset**
```bash
hdfs dfs -mkdir -p $HADOOP_HOME/input
hdfs dfs -put movies_metadata_final.csv $HADOOP_HOME/input/
``` 

4.1.3. **Limpiar directorios para inicio limpio**
```
hdfs dfs -rm -r $HADOOP_HOME/mc 2>/dev/null 
hdfs dfs -rm -r $HADOOP_HOME/roi 2>/dev/null  
hdfs dfs -rm -r $HADOOP_HOME/debut 2>/dev/null
hdfs dfs -rm -r $HADOOP_HOME/hg 2>/dev/null  
hdfs dfs -rm -r $HADOOP_HOME/stab 2>/dev/null
```

4.1.4. **Correr las querys**
```
# 1) MC 8.3s
hadoop jar "$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar" \
  -input $HADOOP_HOME/input/movies_metadata_final.csv \
  -output $HADOOP_HOME/mc \
  -mapper "python3 mapper.py" \
  -reducer "python3 reducerMC.py"

# 2) ROI 9.239s 
hadoop jar "$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar" \
  -input $HADOOP_HOME/input/movies_metadata_final.csv \
  -output $HADOOP_HOME/roi \
  -mapper "python3 mapper.py" \
  -reducer "python3 reducerROI.py"

# 3) DEBUT 10.228s
hadoop jar "$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar" \
  -input $HADOOP_HOME/input/movies_metadata_final.csv \
  -output $HADOOP_HOME/debut \
  -mapper "python3 mapper.py" \
  -reducer "python3 reducerDEBUT.py"

# 4) Joyitas escondidas 9.319s 
hadoop jar "$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar" \
  -input $HADOOP_HOME/input/movies_metadata_final.csv \
  -output $HADOOP_HOME/hg \
  -mapper "python3 mapper.py" \
  -reducer "python3 reducerHG.py" \
  -cmdenv MIN_VOTES=10

# 5) Estabilidad 8.859s 
hadoop jar "$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar" \
  -input $HADOOP_HOME/input/movies_metadata_final.csv \
  -output $HADOOP_HOME/stab \
  -mapper "python3 mapper.py" \
  -reducer "python3 reducerSTAB.py"
```

4.1.5. **Visualizar los outputs de las querys**
```
# Década - Etiqueta - % Suma de participaciones de las top 5 productoras - Etiqueta - Total
# Década - Ranking - ID Productora - Nombre Productora - Revenue de la productora - % de participación 
hdfs dfs -cat $HADOOP_HOME/mc/part-00000 | tr '\t' '|' | column -s '|' -t 


# Década - Decil - Promedio ROI (revenue−budget)/budget - cantidad de películas
hdfs dfs -cat $HADOOP_HOME/roi/part-00000  | tr '\t' '|' | column -s '|' -t


# idProductora|nombreProductora|fechaEstreno|debutROI|debutPopularidad|noDebutPromedioROI|noDebutPromedioPopularidad|nPeliculasNoDebut|deltaROI|deltaPopularidad
hdfs dfs -cat $HADOOP_HOME/debut/part-00000 | tr '\t' '|' | column -s '|' -t | head


# Década - idPelicula - titulo - promedioVotos - popularidad - nVotos
hdfs dfs -cat $HADOOP_HOME/hg/part-00000  | tr '\t' '|' | column -s '|' -t 


# década - bucket de géneros - promedioVotos - std de promedio de votos - nPeliculas
hdfs dfs -cat $HADOOP_HOME/stab/part-00000 | tr '\t' '|' | column -s '|' -t 
```

### 4.2 Athena (NodeJS)

**Infraestructura requerida:**
- Bucket S3: Uso general, importante recordar el nombre ya que es la ruta.

Ej. ev02-mattiasmorales
> s3://ev02-mattiasmorales

4.2.1. **Clonar el proyecto**
```bash
git init
git remote add origin https://github.com/MattiasMR/EV02_advdb.git
git config core.sparseCheckout true
echo "Athena/*" > .git/info/sparse-checkout
git pull origin main
cd Athena/
```

4.2.2. **Setup de Athena y Servidor con Endpoints**
```bash
npm install
# reemplazar testathenareplicacion con el nombre de su bucket
npm run setup testathenareplicacion && npm run serve 
```
4.2.3. **Probar endpoints**
```bash
curl http://localhost:3000/q/q1 2>/dev/null | head -3
curl http://localhost:3000/q/q2 2>/dev/null | head -3
curl http://localhost:3000/q/q3 2>/dev/null | head -3
curl http://localhost:3000/q/q4 2>/dev/null | head -3
curl http://localhost:3000/q/q5 2>/dev/null | head -3
```
