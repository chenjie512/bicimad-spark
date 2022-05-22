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
    
    # Extracción de tiempos de viaje por identificador de persona
    rdd_travelTime_id = rdd.map(lambda x: (x[0], x[3]))\
                        .groupByKey()\
                        .mapValues(sum)\
                        .values()

    # Tiempo medio por identificador diario (persona)
    total_travels_id = rdd_travelTime_id.count()
    total_travelTime_id = rdd_travelTime_id.sum()
    average_travelTime_id = total_travelTime_id/total_travels_id
    
    out = open(f'02_out.txt', 'w')
    out.write(f'Personas (id): {total_travels_id}\n')
    out.write('Duración total: ' + format(total_travelTime_id/3600, ".2f") + ' horas\n')
    out.write('Media por persona: ' + format(average_travelTime_id/60, ".2f") + ' minutos\n')
    out.close()
  
    
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <filename>")
        exit(1)
    filename = sys.argv[1]
    sc = SparkContext()
    main(sc, filename)