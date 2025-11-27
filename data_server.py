import asyncio
import json
import random
import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from datetime import datetime

app = FastAPI()

# Mapping pour simuler les données réelles
SHIPPING_MODES = ["Standard Class", "First Class", "Second Class", "Same Day"]
MARKETS = ["Pacific Asia", "USCA", "Africa", "Europe", "LATAM"]

def generate_order():
    """Génère une commande factice simulant le dataset DataCo"""
    return {
        "timestamp": datetime.now().isoformat(),
        "Days for shipment (scheduled)": random.randint(0, 4),
        "Category Id": random.randint(1, 76),
        "Order Item Quantity": random.randint(1, 5),
        "Shipping Mode": random.choice(SHIPPING_MODES),
        "distance": round(random.uniform(10.0, 5000.0), 2),
        "Market": random.choice(MARKETS),
        "Sales": round(random.uniform(100.0, 2000.0), 2)
    }

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            data = generate_order()
            await websocket.send_json(data)
            # Simulation de 1 commande par seconde
            await asyncio.sleep(1) 
    except WebSocketDisconnect:
        print("⚠️ Client déconnecté (Bridge arrêté)")
    except Exception as e:
        print(f"⚠️ Erreur: {e}")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)