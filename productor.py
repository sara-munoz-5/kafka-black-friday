
from confluent_kafka import Producer
import json
import random
import time
from datetime import datetime

# Conexion con Kafka
productor = Producer({
    "bootstrap.servers": "localhost:9092"
})

TOPIC = "ventas_tienda"

productos = [
    {"nombre": "Audifonos", "precio": 85000},
    {"nombre": "Teclado", "precio": 120000},
    {"nombre": "Mouse", "precio": 45000},
    {"nombre": "Monitor", "precio": 750000},
    {"nombre": "Celular", "precio": 1500000},
    {"nombre": "Laptop", "precio": 3200000},
]

def confirmar_envio(error, mensaje):
    if error:
        print(f"Error al enviar: {error}")
    else:
        print(
            f"  Evento confirmado en Kafka "
            f"(offset {mensaje.offset()})"
        )

print("BLACK FRIDAY - SIMULADOR DE VENTAS")
print("Presiona Ctrl+C para detener.\n")

try:
    while True:
        producto = random.choice(productos)
        cantidad = random.randint(1, 3)

        venta = {
            "producto": producto["nombre"],
            "precio": producto["precio"],
            "cantidad": cantidad,
            "total": producto["precio"] * cantidad,
            "fecha": datetime.now().isoformat()
        }

        productor.produce(
            TOPIC,
            value=json.dumps(venta).encode("utf-8"),
            callback=confirmar_envio
        )

        productor.poll(0)
        print(
            f"Venta generada: {venta['producto']} "
            f"x{cantidad} - ${venta['total']:,}"
        )

        time.sleep(1)

except KeyboardInterrupt:
    print("\nDeteniendo simulador...")

finally:
    productor.flush()
    print("Envios pendientes procesados.")
