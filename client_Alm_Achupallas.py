import socket
import csv
import time

HEADER = 64
PORT = 5050
FORMAT = 'utf-8'
DISCONNECT_MESSAGE = "DISCONNECT!"
SERVER = socket.gethostbyname(socket.gethostname())
ADDR = (SERVER, PORT)

client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client.connect(ADDR)

def send(msg):
    message = msg.encode(FORMAT)
    msg_length = len(message)
    send_length = str(msg_length).encode(FORMAT)
    send_length += b' ' * (HEADER - len(send_length))
    client.send(send_length)
    client.send(message)
    print(client.recv(2048).decode(FORMAT))

def get_csv(filename):
    with open(filename) as f:
        reader = csv.reader(f)
        next(reader)
        cont = 1
        for row in reader:
            string =  str(cont)+";"+row[0]+";"+row[1]+";"+row[2]+";"+row[3]+";"+row[4]+";"+row[5]+";"+row[6]+";"+row[7]
            cont+=1
            send(string)
            time.sleep(0.5)
input()
get_csv('Alm Achupallas.csv')
send(DISCONNECT_MESSAGE)
