
import streamlit as st
import pandas as pd
import json
import random
from datetime import datetime
from collections import Counter
from confluent_kafka import Consumer, Producer

st.set_page_config(
    page_title="Black Friday | Kafka",
    page_icon="🛒",
    layout="wide"
)

TOPIC = "ventas_tienda"

PRODUCTOS = [
    ("Audifonos", 85000),
    ("Teclado", 120000),
    ("Mouse", 45000),
    ("Monitor", 750000),
    ("Celular", 1500000),
    ("Laptop", 3200000),
]

# Cada sesion del dashboard usa su propio consumidor.
if "consumer" not in st.session_state:
    consumer = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": f"dashboard-{random.getrandbits(64):016x}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })
    consumer.subscribe([TOPIC])
    st.session_state.consumer = consumer
    st.session_state.ventas = []

@st.cache_resource
def crear_productor():
    return Producer({"bootstrap.servers": "localhost:9092"})

def simular_pico():
    producer = crear_productor()

    for _ in range(50):
        nombre, precio = random.choice(PRODUCTOS)
        cantidad = random.randint(1, 4)
        venta = {
            "producto": nombre,
            "precio": precio,
            "cantidad": cantidad,
            "total": precio * cantidad,
            "fecha": datetime.now().isoformat()
        }
        producer.produce(
            TOPIC,
            value=json.dumps(venta).encode("utf-8")
        )
        producer.poll(0)

    pendientes = producer.flush(10)
    if pendientes:
        st.warning(f"{pendientes} mensajes siguen pendientes.")
    else:
        st.toast("50 ventas enviadas a Kafka!")

st.title("🛒 BLACK FRIDAY | LIVE ANALYTICS")
st.caption("Apache Kafka + Python + Streamlit")
st.markdown("Monitoreo de ventas en tiempo real")

if st.button("🚀 SIMULAR PICO DE 50 VENTAS", type="primary"):
    simular_pico()

st.divider()

# Fragmento que se actualiza cada 2 segundos
@st.fragment(run_every="2s")
def actualizar_dashboard():
    consumer = st.session_state.consumer

    # Leer hasta 500 eventos en cada actualizacion.
    for _ in range(500):
        mensaje = consumer.poll(0.01)

        if mensaje is None:
            break

        if mensaje.error():
            st.error(f"Kafka: {mensaje.error()}")
            break

        try:
            venta = json.loads(
                mensaje.value().decode("utf-8")
            )
            st.session_state.ventas.append(venta)
        except (ValueError, KeyError) as error:
            st.warning(f"Evento invalido: {error}")

    ventas = st.session_state.ventas

    if not ventas:
        st.info("Esperando ventas desde Kafka...")
        return

    df = pd.DataFrame(ventas)
    ingresos = int(df["total"].sum())
    unidades = int(df["cantidad"].sum())

    conteo = Counter()
    for venta in ventas:
        conteo[venta["producto"]] += venta["cantidad"]

    lider = conteo.most_common(1)[0][0]

    col1, col2, col3, col4 = st.columns(4)

    col1.metric("Ventas registradas", len(ventas))
    col2.metric("Ingresos", f"${ingresos:,.0f}")
    col3.metric("Unidades vendidas", unidades)
    col4.metric("Producto lider", lider)

    st.divider()

    izquierda, derecha = st.columns(2)

    with izquierda:
        st.subheader("Productos mas vendidos")
        resumen = (
            df.groupby("producto")["cantidad"]
            .sum()
            .sort_values(ascending=False)
        )
        st.bar_chart(resumen)

    with derecha:
        st.subheader("Ingresos por producto")
        ingresos_producto = (
            df.groupby("producto")["total"]
            .sum()
            .sort_values(ascending=False)
        )
        st.bar_chart(ingresos_producto)

    st.subheader("Actividad de ventas")
    df["fecha"] = pd.to_datetime(df["fecha"])
    actividad = (
        df.set_index("fecha")
        .resample("10s")
        .size()
        .rename("Ventas")
    )
    st.line_chart(actividad)

    st.subheader("Ultimas transacciones")
    st.dataframe(
        df.tail(10).iloc[::-1],
        use_container_width=True,
        hide_index=True
    )

    st.caption(
        "Los indicadores se calculan sobre eventos "
        "consumidos por este dashboard desde Kafka."
    )

actualizar_dashboard()
