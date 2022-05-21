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
    
    #ordena por duracion, tomando los 10 primeros y los escribe en el archivo de salida
    list_longest = rdd.sortBy(lambda x: x[3], ascending=False).take(10)
    list_shortest = rdd.sortBy(lambda x: x[3], ascending=True).take(10)
    
    outl = open(f'04_out_longest.json', 'w')
    for data in list_longest:
        outl.write(f'{data}\n')
    outl.close()
    
    outs = open(f'04_out_shortest.json', 'w')
    for data in list_shortest:
        outs.write(f'{data}\n')
    outs.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <filename>")
        exit(1)
    filename = sys.argv[1]
    sc = SparkContext()
    main(sc, filename)
    
    