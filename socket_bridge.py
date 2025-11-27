import asyncio
import websockets
import socket
import json

WS_URI = "ws://localhost:8000/ws"
TCP_HOST = "localhost"
TCP_PORT = 9999

async def forward_data():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((TCP_HOST, TCP_PORT))
    server_socket.listen(1)
    print(f"Bridge TCP en écoute sur {TCP_HOST}:{TCP_PORT}...")
    print(f"En attente de connexion Spark...")

    conn, addr = server_socket.accept()
    print(f"✅ Spark connecté: {addr}")

    async with websockets.connect(WS_URI) as websocket:
        print("✅ Connecté au WebSocket FastAPI")
        try:
            while True:
                data = await websocket.recv()
                data_dict = json.loads(data)
                
                message = json.dumps(data_dict) + "\n"
                conn.send(message.encode('utf-8'))
                print(f"Transmis: {message.strip()}")
        except Exception as e:
            print(f"Erreur: {e}")
        finally:
            conn.close()
            server_socket.close()

if __name__ == "__main__":
    asyncio.run(forward_data())