# Librerias
import pyodbc
import socket
import threading
#--------------------------------------------------------------------------------------------------------------------------------
# Conexión con SQL Server y envio de informacion

# define the server and the database
def sql(sensor):
    server = 'localhost' 
    database = 'SistemaElectrico'
    try:
        conexion = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server}; \
        SERVER='+ server +'; \
        DATABASE='+ database +';\
        Trusted_Connection=yes;'
    )
        # OK! conexión exitosa
    except Exception as e:
        # Atrapar error
        print("Ocurrió un error al conectar a SQL Server: ", e)

    #Corregir string de nombre
    string = sensor[1]
    text = string.split(' ')
    new_string = ''
    for i in range(len(text)):
        if i == len(text)-1:
            new_string = new_string + text[i]
            break
        new_string = new_string + text[i] + '_'

    try:
        with conexion.cursor() as cursor:
            cursor.execute("select top 8 * from Indices."+new_string+" order by num desc;")
            # Con fetchall traemos todas las filas
            registro = cursor.fetchall()
    except Exception as e:
        print("Ocurrió un error al consultar: ", e)

    potencia_activa = sensor[4] - sensor[6]

    # Potencia reactiva neta.
    potencia_reactiva = sensor[5] - sensor[7]

    # Potencia activa máxima registrada en las últimas 2 hrs.
    potencia = { }
    for n in range(len(registro)):
        potencia[n] = registro[n][4]

    max_potencia = potencia_activa
    for n in range(len(registro)):
        if potencia[n] > max_potencia:
            max_potencia = potencia[n]

    # Factor de potencia.
    fp = potencia_activa/pow((pow(potencia_activa,2)+pow(potencia_reactiva,2)),0.5)

    # Regulación de tensión.
    Reg = ((sensor[8] - 12000)/12000)*100
    
    # Estado de alimentador
    estado = {}
    if fp < 0.93:
        estado = 'Anormal'
    elif Reg > 8:
        estado = 'Anormal'
    else:
        estado = 'Normal'
  
    
    try:
        with conexion.cursor() as cursor:
            consulta = "INSERT INTO Medicion."+new_string+" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);"
            cursor.execute(consulta, (sensor[0], sensor[1], sensor[2], sensor[3], sensor[4], sensor[5], sensor[6], sensor[7], sensor[8]))
            consulta1 = "INSERT INTO Indices."+new_string+" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"
            cursor.execute(consulta1, (sensor[0], sensor[1], sensor[2], sensor[3], potencia_activa, potencia_reactiva, max_potencia, fp, Reg, estado))
    except Exception as e:
        print("Ocurrió un error al insertar: ", e)
    finally:
        conexion.close()

    
#--------------------------------------------------------------------------------------------------------------------------------
# Manejo de clientes

HEADER = 64
PORT = 5050
SERVER = socket.gethostbyname(socket.gethostname())
ADDR = (SERVER, PORT)
FORMAT = 'utf-8'
DISCONNECT_MESSAGE = "!DISCONNECT"
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(ADDR)
def handle_client(conn, addr):
    print(f"[NEW CONNECTION] {addr} connected.")
    connected = True
    while connected:
        sensor = list()
        msg_length = conn.recv(HEADER).decode(FORMAT)
        if msg_length:
            msg_length = int(msg_length)
            msg = conn.recv(msg_length).decode(FORMAT)
            if msg == DISCONNECT_MESSAGE:
                connected = False
            proc = msg.split(';')
            sensor.append(int(proc[0]))
            sensor.append(proc[1])
            sensor.append(proc[2])
            sensor.append(proc[3])
            sensor.append(float(proc[4]))
            sensor.append(float(proc[5]))
            sensor.append(float(proc[6]))
            sensor.append(float(proc[7]))
            sensor.append(float(proc[8]))
            sql(sensor)
            conn.send("Msg received".encode(FORMAT))     
    conn.close()
def start():
    server.listen()
    print(f"[LISTENING] Server is listening on {SERVER}")
    while True:
        conn, addr = server.accept()
        thread = threading.Thread(target=handle_client, args=(conn, addr))
        thread.start()
        print(f"[ACTIVE CONNECTIONS] {threading.activeCount() - 1}")
print("[STARTING] server is starting...")
start()
