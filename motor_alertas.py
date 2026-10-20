
from confluent_kafka import Consumer, Producer, KafkaException
from datetime import datetime
import json

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "alertas-black-friday-v1",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False
})

producer = Producer({
    "bootstrap.servers": "localhost:9092",
    "acks": "all"
})

consumer.subscribe(["reglas-asociacion"])

# Reglas que ya generaron una alerta en esta ejecucion
alertas_emitidas = set()

print("MOTOR DE ALERTAS COMERCIALES")
print("Escuchando reglas de asociacion...\n")

try:
    while True:
        mensaje = consumer.poll(1.0)

        if mensaje is None:
            continue

        if mensaje.error():
            raise KafkaException(mensaje.error())

        analisis = json.loads(
            mensaje.value().decode("utf-8")
        )

        for regla in analisis.get("reglas", []):
            confianza = regla["confianza"]
            lift = regla["lift"]
            soporte = regla["soporte"]

            antecedente = tuple(regla["antecedente"])
            consecuente = tuple(regla["consecuente"])

            identificador = (antecedente, consecuente)

            # Criterios demostrativos
            if (
                confianza >= 0.50
                and lift >= 1.20
                and soporte >= 0.05
                and identificador not in alertas_emitidas
            ):
                alerta = {
                    "fecha": datetime.now().isoformat(),
                    "tipo": "VENTA_CRUZADA",
                    "antecedente": list(antecedente),
                    "consecuente": list(consecuente),
                    "soporte": soporte,
                    "confianza": confianza,
                    "lift": lift,
                    "recomendacion": (
                        f"Evaluar una promocion de "
                        f"{', '.join(consecuente)} "
                        f"para clientes que compran "
                        f"{', '.join(antecedente)}"
                    )
                }

                producer.produce(
                    "alertas-comerciales",
                    value=json.dumps(alerta).encode("utf-8")
                )

                if producer.flush(10) != 0:
                    raise RuntimeError(
                        "Alerta pendiente de confirmacion"
                    )

                alertas_emitidas.add(identificador)

                print("\nALERTA COMERCIAL GENERADA")
                print(alerta["recomendacion"])
                print(f"Confianza: {confianza:.2%}")
                print(f"Lift: {lift:.2f}")
                print("-" * 45)

        consumer.commit(
            message=mensaje,
            asynchronous=False
        )

except KeyboardInterrupt:
    print("\nMotor de alertas detenido.")

finally:
    producer.flush()
    consumer.close()
