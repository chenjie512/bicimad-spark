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
    
    ## Realiza los cálculos y los guarda en el archivo de salida
    
    # Extracción de tiempos de viaje
    rdd_travelTime = rdd.map(lambda x: x[3])

    # Cálculo de totales
    total_travels = rdd_travelTime.count()
    total_travelTime = rdd_travelTime.sum()
    average_travelTime = total_travelTime/total_travels
    
    out = open(f'02_out.txt', 'w')
    out.write(f'Viajes: {total_travels}\n')
    out.write('Duración total: ' + format(total_travelTime/3600, ".2f") + ' horas\n')
    out.write('Media por viaje: ' + format(average_travelTime/60, ".2f") + ' minutos\n')
    out.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"Usage: python3 {sys.argv[0]} <filename.json>")
        exit(1)
    filename = sys.argv[1]
    sc = SparkContext()
    main(sc, filename)