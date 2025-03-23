import json
import random
from datetime import datetime, timezone
import sys
import io
import os
from dotenv import load_dotenv
from azure.eventhub import EventHubProducerClient, EventData

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
load_dotenv()

# Configurações
NUM_REGISTROS = 100
TIPOS_SENSORES = ["temperatura", "umidade", "pressao"]
UNIDADES = {"temperatura": "C", "umidade": "%", "pressao": "Pa"}
PLATAFORMAS = ["Plataforma A", "Plataforma B", "Plataforma C"]
EQUIPAMENTOS = ["SensorBox 1", "SensorBox 2", "SensorBox 3"]

# Azure Event Hub
CONNECTION_STR = os.getenv("EVENT_HUB_CONNECTION_STRING")
EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME")

if not CONNECTION_STR or not EVENT_HUB_NAME:
    raise ValueError("As variáveis EVENT_HUB_CONNECTION_STRING ou EVENT_HUB_NAME não estão definidas.")

def gerar_registro():
    sensores = []
    agora = datetime.now(timezone.utc).isoformat()

    for tipo in TIPOS_SENSORES:
        sensores.append({
            "tipo": tipo,
            "valor": round(random.uniform(10, 100), 2),
            "unidade": UNIDADES[tipo],
            "timestamp": agora
        })

    return {
        "plataforma": random.choice(PLATAFORMAS),
        "equipamento": random.choice(EQUIPAMENTOS),
        "sensores": sensores
    }

def enviar_ao_eventhub(dados):
    producer = EventHubProducerClient.from_connection_string(
        conn_str=CONNECTION_STR,
        eventhub_name=EVENT_HUB_NAME
    )

    with producer:
        event_data_batch = producer.create_batch()
        for doc in dados:
            event_data_batch.add(EventData(json.dumps(doc, ensure_ascii=False)))
        producer.send_batch(event_data_batch)

    print("[OK] Dados enviados ao Azure Event Hub.")

if __name__ == "__main__":
    dados = [gerar_registro() for _ in range(NUM_REGISTROS)]
    enviar_ao_eventhub(dados)
