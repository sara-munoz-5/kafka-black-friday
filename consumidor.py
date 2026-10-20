
from confluent_kafka import Consumer, KafkaException
from collections import Counter
import json

# Configuracion del consumidor
consumidor = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "analitica-black-friday",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False
})

consumidor.subscribe(["ventas_tienda"])

ventas = 0
ingresos = 0
unidades = Counter()

print("BLACK FRIDAY - ANALITICA EN TIEMPO REAL")
print("Esperando eventos de Kafka...\n")

try:
    while True:
        mensaje = consumidor.poll(1.0)

        if mensaje is None:
            continue

        if mensaje.error():
            raise KafkaException(mensaje.error())

        venta = json.loads(mensaje.value().decode("utf-8"))

        ventas += 1
        ingresos += venta["total"]
        unidades[venta["producto"]] += venta["cantidad"]

        producto_lider = unidades.most_common(1)[0][0]

        print("\n" + "=" * 45)
        print(f"NUEVA VENTA: {venta['producto']}")
        print(f"TOTAL DE VENTAS: {ventas}")
        print(f"INGRESOS: ${ingresos:,.0f}")
        print(f"UNIDADES VENDIDAS: {sum(unidades.values())}")
        print(f"PRODUCTO MAS VENDIDO: {producto_lider}")
        print("=" * 45)

        # Confirma la posicion del evento procesado.
        consumidor.commit(message=mensaje, asynchronous=False)

except KeyboardInterrupt:
    print("\nAnalitica detenida.")

finally:
    consumidor.close()
