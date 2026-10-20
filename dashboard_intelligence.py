
import json
import uuid
from collections import Counter

import pandas as pd
import streamlit as st
from confluent_kafka import Consumer, KafkaException

st.set_page_config(
    page_title="Black Friday Intelligence",
    page_icon="🛍️",
    layout="wide"
)

TOPICS = [
    "compras-black-friday",
    "reglas-asociacion",
    "alertas-comerciales"
]



if "kafka_consumer" not in st.session_state:
    consumer = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": f"intelligence-{uuid.uuid4()}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })
    consumer.subscribe(TOPICS)

    st.session_state.kafka_consumer = consumer
    st.session_state.compras = {}
    st.session_state.analisis = {}
    st.session_state.alertas = {}
    st.session_state.contadores = Counter()

st.title("🛍️ BLACK FRIDAY INTELLIGENCE")
st.caption(
    "Apache Kafka | FP-Growth | "
    "Market Basket Analysis | Event Streaming"
)

st.markdown(
    "**Centro de inteligencia comercial en tiempo real**"
)

@st.fragment(run_every="2s")
def monitor():
    consumer = st.session_state.kafka_consumer

    # Leer mensajes disponibles de los tres topics
    for _ in range(500):
        mensaje = consumer.poll(0.005)

        if mensaje is None:
            break

        if mensaje.error():
            st.error(str(mensaje.error()))
            break

        try:
            dato = json.loads(
                mensaje.value().decode("utf-8")
            )
        except (ValueError, UnicodeDecodeError):
            continue

        topic = mensaje.topic()
        identificador = (
            topic, mensaje.partition(), mensaje.offset()
        )

        st.session_state.contadores[topic] += 1

        if topic == "compras-black-friday":
            st.session_state.compras[identificador] = dato

        elif topic == "reglas-asociacion":
            st.session_state.analisis[identificador] = dato

        elif topic == "alertas-comerciales":
            st.session_state.alertas[identificador] = dato

    compras = list(st.session_state.compras.values())
    analisis = list(st.session_state.analisis.values())
    alertas = list(st.session_state.alertas.values())

    ingresos = sum(x.get("total", 0) for x in compras)
    articulos = Counter(
        p
        for compra in compras
        for p in compra.get("productos", [])
    )

    ultimo = analisis[-1] if analisis else {}
    reglas = ultimo.get("reglas", [])

    tab1, tab2, tab3, tab4 = st.tabs([
        "📊 Ventas",
        "🧠 Reglas de asociación",
        "🔔 Alertas",
        "⚡ Eventos Kafka"
    ])

    
    with tab1:
        a, b, c, d = st.columns(4)

        a.metric("Facturas", len(compras))

        b.metric(
            "Ingresos simulados",
            f"${ingresos / 1_000_000:,.1f} M"
        )

        c.metric("Unidades", sum(articulos.values()))
        d.metric("Productos diferentes", len(articulos))

        st.subheader("Productos más comprados")


        if articulos:
            df_productos = pd.DataFrame(
                articulos.items(),
                columns=["Producto", "Frecuencia"]
            ).sort_values("Frecuencia", ascending=False)

            st.bar_chart(
                df_productos.set_index("Producto"),
                horizontal=True
            )

        st.subheader("Últimas compras")
        if compras:
            st.dataframe(
                pd.DataFrame(compras[-15:][::-1]),
                use_container_width=True,
                hide_index=True
            )

    with tab2:
        st.subheader("Market Basket Analysis")

        a, b, c = st.columns(3)
        a.metric("Reglas actuales", len(reglas))
        b.metric(
            "Facturas en ventana",
            ultimo.get("facturas_analizadas", 0)
        )
        c.metric(
            "Análisis publicados",
            len(analisis)
        )

        if reglas:
            filas = []
            for regla in reglas:
                filas.append({
                    "Antecedente": ", ".join(
                        regla["antecedente"]
                    ),
                    "Consecuente": ", ".join(
                        regla["consecuente"]
                    ),
                    "Soporte": regla["soporte"],
                    "Confianza": regla["confianza"],
                    "Lift": regla["lift"]
                })

            df_reglas = pd.DataFrame(filas)

            st.dataframe(
                df_reglas,
                use_container_width=True,
                hide_index=True
            )

            st.subheader("Lift por regla")
            df_reglas["Regla"] = (
                df_reglas["Antecedente"]
                + " → "
                + df_reglas["Consecuente"]
            )

            st.bar_chart(
                df_reglas.set_index("Regla")["Lift"],
                horizontal=True
            )
        else:
            st.info("Esperando reglas de asociación.")

    with tab3:
        st.subheader("Recomendaciones comerciales")

        st.metric("Alertas registradas", len(alertas))

        if alertas:
            for alerta in alertas[-10:][::-1]:
                with st.container(border=True):
                    st.markdown(
                        "**" + alerta["recomendacion"] + "**"
                    )
                    st.write(
                        f"Confianza: "
                        f"{alerta['confianza']:.1%}"
                        f" | Lift: {alerta['lift']:.2f}"
                    )
        else:
            st.info("No hay alertas registradas.")

    with tab4:
        st.subheader("Eventos observados por el dashboard")

        st.caption(
            "Conteos locales de mensajes leídos en esta "
            "sesión; no representan el consumer lag del broker."
        )

        df_eventos = pd.DataFrame([
            {
                "Topic": topic,
                "Mensajes leídos": sum(
                    1 for key in (
                        st.session_state.compras
                        if topic == TOPICS[0]
                        else st.session_state.analisis
                        if topic == TOPICS[1]
                        else st.session_state.alertas
                    ) if key[0] == topic
                )
            }
            for topic in TOPICS
        ])

        st.dataframe(
            df_eventos,
            hide_index=True,
            use_container_width=True
        )

        st.info(
            "Para comprobar los offsets confirmados "
            "y el consumer lag real, utilizaremos Kafbat."
        )

monitor()
