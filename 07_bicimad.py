from pyspark import SparkContext
import json
import sys
import matplotlib.pyplot as plt

def mapper(line):
    data = json.loads(line)
    u_t = data['user_type']
    u_c = data['user_day_code']
    start = data['idunplug_station']
    end = data['idplug_station']
    time = data['travel_time']
    u_a = data['ageRange']
    return u_c, start, end, time, u_a, u_t


def main(sc, filename):
    #carga el archivo
    rdd_base = sc.textFile(filename)
    
    #filtra los usuarios de tipo 1 y 2
    rdd = rdd_base.map(mapper)\
        .filter(lambda x: x[5] in [1, 2])\
        .map(lambda x: x[:5])
    
    #los ordena segun el rango de la edad para luego contar sus usos
    rdd_age = rdd.map(lambda x: (x[4], 1))\
                    .groupByKey()\
                    .sortByKey()\
                    .mapValues(sum)\
                    .values()
    
    age = ['desconocido','0-16','17-18','19-26','27-40', '41-65','66+']
    num_usage = rdd_age.collect()
    
    #lo guarda en una grafica de barras
    plt.figure(figsize=(8,6))
    plt.bar(age, num_usage)
    plt.ylabel('Número de usos')
    plt.xlabel('Rango de edad')
    plt.title('Uso por edades')
    
    plt.savefig('07_out_chart.jpeg')
                                 
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <filename>")
        exit(1)
    filename = sys.argv[1]
    sc = SparkContext()
    main(sc, filename)
    
    