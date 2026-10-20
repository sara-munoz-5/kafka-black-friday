
from confluent_kafka import Consumer, Producer, KafkaException
from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import fpgrowth, association_rules
from collections import deque
from datetime import datetime
import pandas as pd
import json

TOPIC_ENTRADA = "compras-black-friday"
TOPIC_SALIDA = "reglas-asociacion"

VENTANA = 200
CADA_CUANTAS = 20
SOPORTE_MINIMO = 0.05
CONFIANZA_MINIMA = 0.45

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "motor-fpgrowth-v1",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False
})

producer = Producer({
    "bootstrap.servers": "localhost:9092",
    "acks": "all"
})

consumer.subscribe([TOPIC_ENTRADA])

# Conservar las ultimas 200 facturas
transacciones = deque(maxlen=VENTANA)
procesadas = 0


def analizar_canastas():
    if len(transacciones) < 20:
        return []

    # Convertir facturas en matriz binaria
    encoder = TransactionEncoder()
    matriz = encoder.fit(
        list(transacciones)
    ).transform(list(transacciones))

    df = pd.DataFrame(
        matriz,
        columns=encoder.columns_
    )

    # Descubrir conjuntos frecuentes
    frecuentes = fpgrowth(
        df,
        min_support=SOPORTE_MINIMO,
        use_colnames=True,
        max_len=2
    )

    if frecuentes.empty:
        return []

    # Generar reglas de asociacion
    reglas = association_rules(
        frecuentes,
        metric="confidence",
        min_threshold=CONFIANZA_MINIMA
    )

    if reglas.empty:
        return []

    reglas = reglas.sort_values(
        "lift", ascending=False
    ).head(10)

    resultados = []

    for _, regla in reglas.iterrows():
        resultados.append({
            "antecedente": sorted(
                list(regla["antecedents"])
            ),
            "consecuente": sorted(
                list(regla["consequents"])
            ),
            "soporte": round(
                float(regla["support"]), 4
            ),
            "confianza": round(
                float(regla["confidence"]), 4
            ),
            "lift": round(
                float(regla["lift"]), 4
            )
        })

    return resultados


print("MOTOR FP-GROWTH ACTIVADO")
print("Esperando facturas desde Kafka...\n")

try:
    while True:
        mensaje = consumer.poll(1.0)

        if mensaje is None:
            continue

        if mensaje.error():
            raise KafkaException(mensaje.error())

        factura = json.loads(
            mensaje.value().decode("utf-8")
        )

        productos = factura["productos"]

        if not isinstance(productos, list):
            raise ValueError("Productos debe ser una lista")

        transacciones.append(
            sorted(set(productos))
        )
        procesadas += 1

        if procesadas % CADA_CUANTAS == 0:
            reglas = analizar_canastas()

            resultado = {
                "fecha_analisis": datetime.now().isoformat(),
                "facturas_analizadas": len(transacciones),
                "facturas_procesadas": procesadas,
                "total_reglas": len(reglas),
                "reglas": reglas
            }

            producer.produce(
                TOPIC_SALIDA,
                value=json.dumps(
                    resultado
                ).encode("utf-8")
            )

            pendientes = producer.flush(10)

            if pendientes:
                raise RuntimeError(
                    "No se confirmo la publicacion"
                )

            print("\n" + "=" * 55)
            print(f"FACTURAS PROCESADAS: {procesadas}")
            print(f"REGLAS DESCUBIERTAS: {len(reglas)}")

            for regla in reglas[:5]:
                antecedente = ", ".join(
                    regla["antecedente"]
                )
                consecuente = ", ".join(
                    regla["consecuente"]
                )

                print(
                    f"{antecedente} -> {consecuente} | "
                    f"Confianza: {regla['confianza']:.2f} | "
                    f"Lift: {regla['lift']:.2f}"
                )

            print("Analisis publicado en Kafka")
            print("=" * 55)

        consumer.commit(
            message=mensaje,
            asynchronous=False
        )

except KeyboardInterrupt:
    print("\nMotor detenido.")

finally:
    producer.flush()
    consumer.close()
