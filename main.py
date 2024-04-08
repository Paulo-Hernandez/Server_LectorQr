import socket
import threading
import pyodbc
import csv
import os


def copy_data_from_det_lecturas_to_palet_listos(palet):
    conn = None

    try:
        # Conectar a la base de datos SQL Server
        conn_str = f'DRIVER=SQL Server;SERVER={server_name};DATABASE=Db_Pimpihue;UID=sa;PWD=12345678'
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # Verificar si el palet ya existe en la tabla PALET_LISTOS
        cursor.execute("SELECT COUNT(*) FROM PALET_LISTOS WHERE palet = ?", (palet,))
        if cursor.fetchone()[0] > 0:
            print(f"El palet {palet} ya existe en la tabla PALET_LISTOS. No se realizará la copia de datos.")
        else:
            # Copiar los datos de DETALLES_LECTURAS a PALET_LISTOS
            cursor.execute('''
                INSERT INTO PALET_LISTOS (palet, cajas, codigo, estado)
                SELECT ?, MAX(CAST(n_cajas AS INT)), codigo, 1
                FROM (SELECT DISTINCT codigo, n_cajas FROM DETALLES_LECTURAS WHERE n_palet = ?) AS unique_codes
                GROUP BY codigo
            ''', (palet, palet))

            # Confirmar la transacción
            conn.commit()

            # Eliminar los datos de DETALLES_LECTURAS para el palet especificado
            cursor.execute("DELETE FROM DETALLES_LECTURAS WHERE n_palet = ?", (palet,))
            conn.commit()

            print(f"Datos para el palet {palet} copiados correctamente a la tabla PALET_LISTOS.")

    except Exception as e:
        print("Error al copiar los datos:", e)

    finally:
        # Cerrar la conexión solo si está inicializada
        if conn:
            conn.close()


# Función para manejar las conexiones en el puerto 8000
def handle_connection_port_8000():
    try:
        while True:
            # Aceptar la conexión del cliente
            client_socket, _ = server_socket1.accept()
            print("Conexión entrante en el puerto 8000:", client_socket.getpeername())

            # Recibir el archivo del cliente
            file_data = b''
            while True:
                chunk = client_socket.recv(4096)
                if not chunk:
                    break
                file_data += chunk

            # Guardar el archivo recibido
            with open("archivo_recibido.csv", "wb") as file:
                file.write(file_data)
            print("Archivo CSV recibido y guardado correctamente.")

            # Procesar el archivo CSV y agregar los datos a la base de datos
            with open("archivo_recibido.csv", newline='') as csvfile:
                csvreader = csv.reader(csvfile)
                for row in csvreader:
                    # Conectar a la base de datos SQL Server y realizar la inserción
                    try:
                        palet = row [0]
                        conn_str = 'DRIVER=SQL Server;SERVER=' + server_name + ';DATABASE=Db_Pimpihue;UID=sa;PWD=12345678'
                        conn = pyodbc.connect(conn_str)
                        cursor = conn.cursor()
                        cursor.execute('INSERT INTO DETALLES_LECTURAS (n_palet, n_cajas, codigo) VALUES (?, ?, ?)',
                                       (row[0], row[1], row[2]))
                        conn.commit()
                        print("Datos insertados correctamente en la base de datos del palet. " + palet)
                    except Exception as e:
                        print("Error al insertar datos en la base de datos:", e)

            client_socket.close()

    except Exception as e:
        print("Error en la conexión del puerto 8000:", e)


# Función para manejar las conexiones en el puerto 9000
def handle_connection_port_9000():
    try:
        while True:
            # Aceptar la conexión del cliente
            client_socket, _ = server_socket2.accept()
            print("Conexión entrante en el puerto 9000:", client_socket.getpeername())

            data = client_socket.recv(1024).decode('utf-8').strip()

            if not data:
                break

            message, palet = data.split(',')

            # Conectar a la base de datos SQL Server
            conn_str = f'DRIVER=SQL Server;SERVER={server_name};DATABASE=Db_Pimpihue;UID=sa;PWD=12345678'
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()

            # Imprimir mensaje de conexión exitosa
            print("Conexión exitosa con la base de datos SQL Server.")

            # Verificar el valor recibido y tomar acciones correspondientes
            # Guardar
            if message == '1':
                print("Se recibió un 1 en el puerto 9000. Realiza un Guardado.")
                copy_data_from_det_lecturas_to_palet_listos(palet)

            # Eliminar
            elif message == '2':
                print("Se recibió un 2 en el puerto 9000. Realiza una Eliminacion.")
                cursor.execute("DELETE FROM DETALLES_LECTURAS WHERE n_palet = ?", (palet,))
                conn.commit()
                print(f"Filas con el número de palet {palet} eliminadas correctamente.")
                cursor.execute("DELETE FROM CABECERA_PALET WHERE N_PALLET = ?", (palet,))
                conn.commit()
                print(f"Filas con el número de palet {palet} eliminadas de los verificados.")

            # Pendiente
            elif message == '3':
                print("Se recibió un 3 en el puerto 9000. Realiza un Pendiente.")
                # Realizar la acción correspondiente al recibir un 3


            elif message == 'verificar':

                # Realizar una consulta SQL para verificar si el palet existe

                cursor.execute("SELECT COUNT(*) FROM PALET_LISTOS WHERE palet = ?", (palet,))
                result = cursor.fetchone()[0]
                cursor.execute("SELECT COUNT(*) FROM CABECERA_PALET WHERE N_PALLET = ?", (palet,))
                result2 = cursor.fetchone()[0]

                # Si el resultado es mayor que cero, significa que el palet existe

                if result > 0 or result2 > 0:
                    response = "existe"

                else:
                    response = "no_existe"
                    cursor.execute("INSERT INTO CABECERA_PALET (N_PALLET, ESTADO) VALUES (?, ?)", (palet,"P"))
                    conn.commit()

                # Enviar la respuesta al cliente

                client_socket.sendall(response.encode('utf-8'))

                print(f"Respuesta '{response}' enviada al cliente.")

            else:
                print("Valor recibido en el puerto 9000 no reconocido:", message)

            client_socket.close()

    except Exception as e:
        print("Error en la conexión del puerto 9000:", e)

# Solicitar al usuario el número de servidor
server_name = "DESKTOP-7QOMSTL\SQLEXPRESS"

# DESKTOP-7QOMSTL\SQLEXPRESS
# DESKTOP-MEOAI1O\SQLEXPRESS


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








