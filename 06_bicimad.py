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
    date = data['unplug_hourTime']
    return u_c, start, end, time, date, u_t


def main(sc, filename):
    # Carga el archivo
    rdd_base = sc.textFile(filename)
    
    # Filtra los usuarios de tipo 1 y 2
    rdd = rdd_base.map(mapper)\
        .filter(lambda x: x[5] in [1, 2])\
        .map(lambda x: x[:5])
    
    # Cuenta solamente según los días de la fecha
    dict_days = rdd.map(lambda x: (x[4]['$date'][8:10], 0))\
                    .countByKey()
    days = dict_days.keys()
    num_usage = dict_days.values()
    
    # Se representa en una gráfica de barras
    plt.figure(figsize=(8,6))
    plt.bar(days, num_usage)
    plt.ylabel('Número de usos')
    plt.xlabel('Días')
    plt.title('Uso por días')
    
    plt.savefig('06_out_chart.jpeg')
                                 
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"Usage: python3 {sys.argv[0]} <filename.json>")
        exit(1)
    filename = sys.argv[1]
    sc = SparkContext()
    main(sc, filename)
    
    