#!/usr/bin/env python3
"""
Script de vérification de la santé de la pipeline logistique
"""
import subprocess
import requests
import pymongo
import psycopg2
import socket
import time
from typing import Dict, Tuple

def check_process_running(process_name: str) -> Tuple[bool, str]:
    """Vérifie si un processus est en cours d'exécution"""
    try:
        result = subprocess.run(
            ['pgrep', '-f', process_name],
            capture_output=True,
            text=True
        )
        if result.returncode == 0:
            pids = result.stdout.strip().split('\n')
            return True, f"✅ {process_name} en cours (PID: {', '.join(pids)})"
        else:
            return False, f"❌ {process_name} n'est pas en cours d'exécution"
    except Exception as e:
        return False, f"❌ Erreur lors de la vérification de {process_name}: {e}"

def check_docker_containers() -> Tuple[bool, str]:
    """Vérifie que les conteneurs Docker sont actifs"""
    try:
        result = subprocess.run(
            ['docker', 'ps', '--format', '{{.Names}}'],
            capture_output=True,
            text=True
        )
        containers = result.stdout.strip().split('\n')
        if len(containers) >= 2:  # PostgreSQL et MongoDB
            return True, f"✅ Conteneurs Docker actifs: {', '.join(containers)}"
        else:
            return False, f"⚠️ Nombre insuffisant de conteneurs Docker: {containers}"
    except Exception as e:
        return False, f"❌ Erreur Docker: {e}"

def check_fastapi_server() -> Tuple[bool, str]:
    """Vérifie que le serveur FastAPI répond"""
    try:
        response = requests.get("http://localhost:8000/docs", timeout=5)
        if response.status_code == 200:
            return True, "✅ FastAPI serveur accessible (http://localhost:8000)"
        else:
            return False, f"⚠️ FastAPI répond mais status: {response.status_code}"
    except Exception as e:
        return False, f"❌ FastAPI non accessible: {e}"

def check_tcp_port(port: int, name: str) -> Tuple[bool, str]:
    """Vérifie qu'un port TCP est ouvert"""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(2)
            result = s.connect_ex(('localhost', port))
            if result == 0:
                return True, f"✅ {name} port {port} est ouvert"
            else:
                return False, f"❌ {name} port {port} est fermé"
    except Exception as e:
        return False, f"❌ Erreur lors de la vérification du port {port}: {e}"

def check_mongodb() -> Tuple[bool, str]:
    """Vérifie la connexion MongoDB et les données"""
    try:
        client = pymongo.MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=5000)
        client.admin.command('ping')

        db = client["logistics_db"]
        collection = db["aggregated_stats"]
        count = collection.count_documents({})

        if count > 0:
            latest = collection.find_one(sort=[("batch_time", -1)])
            return True, f"✅ MongoDB OK - {count} enregistrements (dernier: {latest.get('batch_time', 'N/A')})"
        else:
            return False, "⚠️ MongoDB accessible mais aucune donnée trouvée"
    except Exception as e:
        return False, f"❌ Erreur MongoDB: {e}"

def check_postgresql() -> Tuple[bool, str]:
    """Vérifie la connexion PostgreSQL et les données"""
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="logistics_db",
            user="user",
            password="password"
        )
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM predictions_realtime")
        count = cursor.fetchone()[0]
        cursor.close()
        conn.close()

        if count > 0:
            return True, f"✅ PostgreSQL OK - {count} prédictions enregistrées"
        else:
            return False, "⚠️ PostgreSQL accessible mais aucune donnée trouvée"
    except Exception as e:
        return False, f"❌ Erreur PostgreSQL: {e}"

def check_streamlit() -> Tuple[bool, str]:
    """Vérifie que Streamlit est accessible"""
    try:
        response = requests.get("http://localhost:8501", timeout=5)
        if response.status_code == 200:
            return True, "✅ Streamlit dashboard accessible (http://localhost:8501)"
        else:
            return False, f"⚠️ Streamlit répond mais status: {response.status_code}"
    except Exception as e:
        return False, f"❌ Streamlit non accessible: {e}"

def main():
    """Exécute tous les checks"""
    print("=" * 70)
    print("🔍 VÉRIFICATION DE LA SANTÉ DE LA PIPELINE LOGISTIQUE")
    print("=" * 70)
    print()

    checks = [
        ("Infrastructure Docker", check_docker_containers),
        ("FastAPI Data Server (port 8000)", check_fastapi_server),
        ("Processus data_server.py", lambda: check_process_running("data_server.py")),
        ("Processus socket_bridge.py", lambda: check_process_running("socket_bridge.py")),
        ("Processus Spark", lambda: check_process_running("spark_streaming_job.py")),
        ("TCP Bridge (port 9999)", lambda: check_tcp_port(9999, "Socket Bridge")),
        ("MongoDB", check_mongodb),
        ("PostgreSQL", check_postgresql),
        ("Streamlit Dashboard (port 8501)", check_streamlit),
    ]

    results = []
    for name, check_func in checks:
        status, message = check_func()
        results.append((name, status))
        print(message)
        time.sleep(0.5)  # Petit délai pour la lisibilité

    print()
    print("=" * 70)
    total = len(results)
    passed = sum(1 for _, status in results if status)

    if passed == total:
        print(f"🎉 SUCCÈS: Tous les services fonctionnent ({passed}/{total})")
    elif passed >= total * 0.7:
        print(f"⚠️ ATTENTION: {passed}/{total} services fonctionnent - Vérifier les erreurs ci-dessus")
    else:
        print(f"❌ ÉCHEC: Seulement {passed}/{total} services fonctionnent")

    print("=" * 70)

    if passed < total:
        print("\n💡 CONSEILS DE DÉPANNAGE:")
        print("1. Vérifiez les logs: api.log, bridge.log, spark.log, dashboard.log")
        print("2. Assurez-vous que docker-compose up -d a été exécuté")
        print("3. Vérifiez que le modèle 'delivery_risk_model' existe")
        print("4. Relancez le DAG Airflow si nécessaire")

    return passed == total

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
