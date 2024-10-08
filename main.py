import socket
import threading
import pyodbc
import csv

# Función para cargar la configuración desde un archivo
def load_config(filename):
    config = {}
    with open(filename, 'r') as file:
        for line in file:
            name, value = line.strip().split('=')
            config[name] = value
    return config

# Función para copiar datos de DETALLES_LECTURAS a PALET_LISTOS
def copy_data_from_det_lecturas_to_palet_listos(palet):
    try:
        conn_str = f'DRIVER=SQL Server;SERVER={server_name};DATABASE={database_name};UID={user_name};PWD={psw}'
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # Insertar en PALET_LISTOS los datos desde DETALLES_LECTURAS donde el n_palet coincida
        cursor.execute('''
            INSERT INTO PALET_LISTOS (palet, cajas, codigo, estado)
            SELECT n_palet, n_cajas, codigo, 1
            FROM DETALLES_LECTURAS
            WHERE n_palet = ?
        ''', (palet,))

        conn.commit()

        # Eliminar los datos copiados de DETALLES_LECTURAS
        cursor.execute('DELETE FROM DETALLES_LECTURAS WHERE n_palet = ?', (palet,))
        conn.commit()

        print(f"Datos del palet {palet} copiados a PALET_LISTOS y eliminados de DETALLES_LECTURAS.")

    except Exception as e:
        print(f"Error al copiar datos del palet {palet} a PALET_LISTOS:", e)

    finally:
        conn.close()

# Función para probar la conexión a la base de datos
def test_database_connection(server_name, database_name, user_name, psw):
    try:
        conn_str = f'DRIVER=SQL Server;SERVER={server_name};DATABASE={database_name};UID={user_name};PWD={psw}'
        conn = pyodbc.connect(conn_str)
        conn.close()
        print("Conexión a la base de datos exitosa.")
    except Exception as e:
        print("Error al conectar a la base de datos:", e)
        exit(1)  # Salir del programa si la conexión falla


# Función para manejar las conexiones en el puerto 8000
def handle_connection_port_8000():
    try:
        while True:
            client_socket, _ = server_socket1.accept()
            print("Conexión entrante en el puerto 8000:", client_socket.getpeername())

            file_data = b''
            while True:
                chunk = client_socket.recv(4096)
                if not chunk:
                    break
                file_data += chunk

            with open("archivo_recibido.csv", "wb") as file:
                file.write(file_data)
            print("Archivo CSV recibido y guardado correctamente.")

            with open("archivo_recibido.csv", newline='') as csvfile:
                csvreader = csv.reader(csvfile)
                for row in csvreader:
                    try:
                        palet = row[0]
                        conn_str = f'DRIVER=SQL Server;SERVER={server_name};DATABASE={database_name};UID={user_name};PWD={psw}'
                        conn = pyodbc.connect(conn_str)
                        cursor = conn.cursor()
                        cursor.execute('INSERT INTO DETALLES_LECTURAS (n_palet,n_cajas, codigo) VALUES (?, ?, ?)',
                                       (row[0], row[1], row[2]))
                        conn.commit()
                        print("Datos insertados correctamente en la base de datos del palet.", palet)
                    except Exception as e:
                        print("Error al insertar datos en la base de datos:", e)

            client_socket.close()

    except Exception as e:
        print("Error en la conexión del puerto 8000:", e)

# Función para manejar las conexiones en el puerto 9000
def handle_connection_port_9000():
    try:
        while True:
            client_socket, _ = server_socket2.accept()
            print("Conexión entrante en el puerto 9000:", client_socket.getpeername())

            data = client_socket.recv(1024).decode('utf-8').strip()
            if not data:
                break

            message, palet = data.split(',')

            conn_str = f'DRIVER=SQL Server;SERVER={server_name};DATABASE={database_name};UID={user_name};PWD={psw}'
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            print("Conexión exitosa con la base de datos SQL Server.")

            if message == '1':
                print("Se recibió un 1 en el puerto 9000. Realiza un Guardado.")
                copy_data_from_det_lecturas_to_palet_listos(palet)

            elif message == '2':
                print("Se recibió un 2 en el puerto 9000. Realiza una Eliminacion.")
                cursor.execute("DELETE FROM DETALLES_LECTURAS WHERE n_palet = ?", (palet,))
                conn.commit()
                cursor.execute("DELETE FROM CABECERA_PALET WHERE n_pallet = ?", (palet,))
                conn.commit()

            elif message == '3':
                print("Se recibió un 3 en el puerto 9000. Realiza un Pendiente.")

            elif message == 'verificar':
                cursor.execute("SELECT COUNT(*) FROM PALET_LISTOS WHERE palet = ?", (palet,))
                result = cursor.fetchone()[0]
                cursor.execute("SELECT COUNT(*) FROM CABECERA_PALET WHERE n_pallet = ?", (palet,))
                result2 = cursor.fetchone()[0]

                if result > 0 or result2 > 0:
                    response = "existe"
                else:
                    response = "no_existe"
                    cursor.execute("INSERT INTO CABECERA_PALET (n_pallet, estado) VALUES (?, ?)", (palet, "P"))
                    print("El numero de palet {" + palet + "} Ha sido ingresado")
                    conn.commit()

                client_socket.sendall(response.encode('utf-8'))

            else:
                print("Valor recibido en el puerto 9000 no reconocido:", message)

            client_socket.close()

    except Exception as e:
        print("Error en la conexión del puerto 9000:", e)

# Cargar la configuración del servidor desde el archivo config_server.txt
config = load_config('config_server.txt')

server_name = config['server']
database_name = config['database']
user_name = config['user']
psw = config['password']

# Probar la conexión a la base de datos
test_database_connection(server_name, database_name, user_name, psw)

# Configuración del primer puerto (8000)
server_address1 = ('', 8000)
server_socket1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket1.bind(server_address1)
server_socket1.listen(5)

# Configuración del segundo puerto (9000)
server_address2 = ('', 9000)
server_socket2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket2.bind(server_address2)
server_socket2.listen(5)

print("Esperando conexiones entrantes en los puertos 8000 y 9000...")

# Iniciar hilos para manejar las conexiones en ambos puertos
threading.Thread(target=handle_connection_port_8000).start()
threading.Thread(target=handle_connection_port_9000).start()

