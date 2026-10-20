
from confluent_kafka import Producer
from datetime import datetime
import random
import json
import time
import uuid

KAFKA_SERVER = "localhost:9092"
TOPIC = "compras-black-friday"

producer = Producer({
    "bootstrap.servers": KAFKA_SERVER,
    "acks": "all"
})

precios = {
    "Leche": 4500,
    "Pan": 3000,
    "Huevos": 12000,
    "Arroz": 5200,
    "Cafe": 18000,
    "Laptop": 3200000,
    "Mouse": 45000,
    "Teclado": 120000,
    "Audifonos": 85000,
    "Celular": 1500000,
    "Cerveza": 5000,
    "Papas": 4500,
    "Gaseosa": 6000,
}

# Patrones simulados de comportamiento
patrones = [
    ["Leche", "Pan", "Huevos"],
    ["Laptop", "Mouse", "Teclado"],
    ["Cerveza", "Papas", "Gaseosa"],
    ["Arroz", "Cafe", "Leche"],
    ["Celular", "Audifonos"],
]

def generar_factura():
    patron = random.choice(patrones)

    # Seleccionar entre 1 y 3 productos del patron
    cantidad = random.randint(1, len(patron))
    productos = random.sample(patron, cantidad)

    # Algunas compras incluyen productos adicionales
    if random.random() < 0.25:
        extra = random.choice(list(precios.keys()))
        if extra not in productos:
            productos.append(extra)

    total = sum(precios[p] for p in productos)

    return {
        "factura_id": str(uuid.uuid4())[:8],
        "fecha": datetime.now().isoformat(),
        "productos": productos,
        "total": total
    }

def confirmar_envio(error, mensaje):
    if error:
        print(f"Error: {error}")
    else:
        print(
            f"Confirmado en Kafka | "
            f"offset={mensaje.offset()}"
        )

print("SIMULADOR DE COMPRAS - BLACK FRIDAY")
print("Enviando facturas a Apache Kafka...")
print("Presiona Ctrl+C para detener.\n")

try:
    while True:
        factura = generar_factura()

        producer.produce(
            TOPIC,
            key=factura["factura_id"],
            value=json.dumps(factura).encode("utf-8"),
            callback=confirmar_envio
        )

        producer.poll(0)

        print(
            f"Factura {factura['factura_id']} | "
            f"{', '.join(factura['productos'])} | "
            f"${factura['total']:,}"
        )

        time.sleep(0.5)

except KeyboardInterrupt:
    print("\nDeteniendo el productor...")

finally:
    producer.flush()
    print("Productor detenido.")
