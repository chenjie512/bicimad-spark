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
    #carga el archivo
    rdd_base = sc.textFile(filename)
    
    #filtra los usuarios de tipo 1 y 2
    rdd = rdd_base.map(mapper)\
        .filter(lambda x: x[4] in [1, 2])\
        .map(lambda x: x[:4])
    
    #realiza los calculos y los guarda en el archivo de salida
    
    #tiempo medio por viaje
    rdd_travelTime = rdd.map(lambda x: x[3])

    total_travels = rdd_travelTime.count()
    total_travelTime = rdd_travelTime.sum()
    average_travelTime = total_travelTime/total_travels
    
    out = open(f'02_out.txt', 'w')
    out.write(f'viajes: {total_travels}\n')
    out.write(f'tiempo: {total_travelTime}\n')
    out.write(f'media por viaje: {average_travelTime} segundos\n')
    out.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <filename>")
        exit(1)
    filename = sys.argv[1]
    sc = SparkContext()
    main(sc, filename)