import schedule
import time
from datetime import datetime
import daemon
import mysql.connector
import requests
import os 
import multiprocessing
config = {
  'user': 'root',
  'password': '',
  'host': 'localhost',
  'port': '3306',
  'database': 'documentos'
}
slaves = [('http://localhost:5001/', 0), ('http://localhost:5002/', 0), ('http://localhost:5003/', 0)]


def send_request(url, url_data):
    response = requests.get('{}/scrapi/{}'.format(url,url_data))
    resultado = response.json()
    print(resultado)

def get_min_slave():
    min_slave = slaves[0]
    for slave in slaves:
        if slave[1] < min_slave[1]:
            min_slave = slave
    return min_slave, slaves.index(min_slave)

def send_load_balanced_request(url_data):
    min_slave , index_min_slave = get_min_slave()
    send_request(min_slave[0],url_data)
    slaves[slaves.index(min_slave)] = (min_slave[0], min_slave[1] + 1)
    return  index_min_slave 




def consulta():
    # Agregue aquí el código para realizar la consulta
    print("Realizando consulta...")

def consultar_base_dato(config):
    conn = mysql.connector.connect(**config)

    # Crear un cursor para ejecutar consultas SQL
    cursor = conn.cursor()

    # Ejecutar una consulta SQL
    cursor.execute("SELECT * FROM documentos")


    # Obtener los resultados de la consulta
    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    return rows 

def insertar_en_base_datos(url,id_esclavo):
    conn = mysql.connector.connect(**config)

    cursor = conn.cursor()
    consulta_select = 'SELECT * FROM documentos WHERE link = %s'
    cursor.execute(consulta_select, (url,))
    objeto = cursor.fetchone()

    ## nos da la hora... 
    now = datetime.now()
    hora_actual = now.strftime("%H:%M:%S")

    id = objeto[0]
    ruta_absoluta = os.path.abspath("{}.txt".format(url))
    
    ###actualiza la base de datos 
    consulta_update = 'UPDATE documentos SET path = %s, ultima_desc = %s, id_esclavo = %s  WHERE id = %s'
    cursor.execute(consulta_update, (ruta_absoluta, hora_actual, id_esclavo,id ))
    conn.commit()

    
    conn.close()

def peticion_esclavo(url):
    response = requests.get('{}:{}/scrapi/{}'.format("http://127.0.0.1","5001",url))
    resultado = response.json()
    print(resultado)

def main():
    schedule.every().day.at("15:54").do(consulta)  # Ejecuta la función "consulta" todos los días a las 12:00
    ##schedule.every(30).minutes.do(consultar_base_dato(config)
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == '__main__':
    ##with daemon.DaemonContext():
    ##main()
    rows = consultar_base_dato(config)
    
    
    num_processes = len(rows)
    for i in rows:
        print(i)

    # for row in rows:
    #     print(row[1])
    #     minimo = send_load_balanced_request(row[1])
    #     print(minimo)
    #     insertar_en_base_datos(row[1],minimo)
    #     ##peticion_esclavo(row[1])

    #     # with multiprocessing.Pool(num_processes) as pool:
    #     #         results = pool.map(send_load_balanced_request(row[1]), range(num_processes))