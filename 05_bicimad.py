from pyspark import SparkContext
import json
import sys

def mapper(line):
    data = json.loads(line)
    u_t = data['user_type']
    u_c = data['user_day_code']
    start = data['idunplug_station']
    end = data['idplug_station']
    time = data['travel_time']
    return u_c, start, end, time, u_t


def main(sc, filename):
    # Carga el archivo
    rdd_base = sc.textFile(filename)
    
    # Filtra los usuarios de tipo 1 y 2
    rdd = rdd_base.map(mapper)\
                .filter(lambda x: x[4] in [1, 2])\
                .map(lambda x: x[:4])
    
    # Ordena las estaciones según aparición y distingue entre estaciones de salida o llegada
    rdd_stations_out = rdd.map(lambda x: (x[1], 1))\
                            .groupByKey()\
                            .mapValues(sum)\
                            .sortBy(lambda x: x[1], ascending=False)\
                            .map(lambda x: x[0])

    rdd_stations_in = rdd.map(lambda x: (x[2], 1))\
                            .groupByKey()\
                            .mapValues(sum)\
                            .sortBy(lambda x: x[1], ascending=False)\
                            .map(lambda x: x[0])

    list_out = rdd_stations_out.take(10)
    list_in = rdd_stations_in.take(10)

    out1 = open(f'05_out_unplug.txt', 'w')
    out1.write(f'Estaciones de salida mas usadas:\n{list_out}')
    out1.close
    
    out2 = open(f'05_out_plug.txt', 'w')
    out2.write(f'Estaciones de llegada mas usadas:\n{list_in}')
    out2.close
                                 
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"Usage: python3 {sys.argv[0]} <filename.json>")
        exit(1)
    filename = sys.argv[1]
    sc = SparkContext()
    main(sc, filename)