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

    # Filtra los ciclos y los escribe en el archivo de salida
    rdd_ciclos = rdd.filter(lambda x: x[1] == x[2])
    list_ciclos = rdd_ciclos.collect()
    
    out = open(f'01_out.json', 'w')
    for data in list_ciclos:
        out.write(f'{data}\n')
    out.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"Usage: python3 {sys.argv[0]} <filename.json>")
        exit(1)
    filename = sys.argv[1]
    sc = SparkContext()
    main(sc, filename)