# BICIMAD SPARK

## Motivación
A partir de los datos de BICIMAD del mes de julio de 2017 y utilizando spark vamos a analizar:
- [*01_ciclos.py*]
 
  > La lista de viajes en los que se sale y llega al mismo sitio (misma estación de salida y llegada).
- [*02_media_viaje.py - 03_media_usuario.py*]
  > Duración media de viaje y duración media por usuario al dia.
- [*04_ranking_duracion.py*]
  > Cuales son los viajes más largos y más cortos.
- [*05_populares.py*]
  > Las estaciones más concurridas (salida y llegada).
- [*06_mensual.py*]
  > Estudio de los usos por cada día por medio de una gráfica.
- [*07_edades.py*]
  > Estudio de los usos según la edad por medio de una gráfica.


## Instrucciones de uso
Para realizar cada análisis únicamente es necesario ejecutar el comando *python3 <archivo.py> <muestra.json>* en la términal. A continuación aparecerán en cada caso los resultados en el archivo de texto con el número correspondiente.

Salvo el análisis 5 (que necesita los datos de todo un mes) y el análisis 2, todos los tests hacen uso de una muestra aleatoria de 5000 elementos tomada de los datos desde abril de 2017 hasta junio de 2018, ambos inclusive. Por lo tanto, dos ejemplos de uso podrían ser:

> python3 02_media_viaje.py 2017_movements.json

> python3 04_ranking_duracion.py sample_5000.json

Para ello, los archivos \*.json deben estar en la misma ruta que el archivo \*.py. Las muestras que se han usado provienen de picluster01, en el sistema de archivos de hadoop: *hdfs:///public_data/bicimad/\*.json*

## Inicio de estudio:
Ver memoria_bicimad_spark.ipynb