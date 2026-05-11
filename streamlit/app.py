# dashboard/app.py
import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
import os

st.set_page_config(page_title="SmartParking", layout="wide")

# =============================================
# Подключение к БД
# =============================================
DB_HOST = os.getenv("WAREHOUSE_DB_HOST", "warehouse")
DB_PORT = os.getenv("WAREHOUSE_DB_PORT", "5432")
DB_NAME = os.getenv("WAREHOUSE_DB_NAME", "warehouse")
DB_USER = os.getenv("WAREHOUSE_DB_USER", "postgres")
DB_PASSWORD = os.getenv("WAREHOUSE_DB_PASSWORD", "postgres")

DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

@st.cache_data(ttl=3600)
def load_predictions():
    engine = create_engine(DB_URL)
    query = """
        SELECT 
            p.forecast_ts,
            p.predicted_occupancy_pct,
            pz.address,
            pz.district,
            pz.max_places,
            pz.is_paid
        FROM staging.fct_predictions p
        JOIN public.parking_zones pz ON p.pz_id = pz.pz_id
        ORDER BY p.predicted_occupancy_pct ASC
    """
    return pd.read_sql(query, engine)

@st.cache_data(ttl=3600)
def load_metrics():
    try:
        with open("/app/ml/model/metrics/metrics.json", "r") as f:
            import yaml
            return yaml.safe_load(f)
    except FileNotFoundError:
        return {"mae": "—", "mape": "—", "trained_at": "—"}

def color_row(val):
    if val <= 50:
        return "background-color: green"
    elif val <= 80:
        return "background-color: yellow"
    else:
        return "background-color: red"

# =============================================
# Загрузка данных
# =============================================
try:
    df = load_predictions()
    metrics = load_metrics()
except Exception as e:
    st.error(f"Ошибка подключения к БД: {e}")
    st.stop()

# =============================================
# Боковая панель
# =============================================
st.sidebar.title("SmartParking 🅿️")
mode = st.sidebar.radio("Режим:", ["🚗 Водитель", "🔧 Администратор"])

# =============================================
# РЕЖИМ ВОДИТЕЛЯ
# =============================================
if mode == "🚗 Водитель":
    st.title("Прогноз загруженности парковок")
    st.caption("Выберите час — получите список парковок от свободных к занятым")

    # Выбор часа
    hours = sorted(df["forecast_ts"].unique())
    if len(hours) == 0:
        st.warning("Прогнозов пока нет. Дождитесь выполнения DAG.")
        st.stop()

    selected_hour = st.selectbox(
        "На какое время?",
        hours,
        format_func=lambda x: x.strftime("%d.%m.%Y, %H:%M")
    )

    # Фильтр по выбранному часу
    df_hour = df[df["forecast_ts"] == selected_hour].copy()
    df_hour["Заполненность, %"] = df_hour["predicted_occupancy_pct"].round(0).astype(int)

    # Цветовая подсветка
    styled = df_hour.style.map(
        color_row,
        subset=["Заполненность, %"]
    ).format({"Заполненность, %": "{}%"})

    st.subheader(f"Парковки на {selected_hour.strftime('%d.%m.%Y, %H:%M')}")
    st.dataframe(
        styled,
        column_config={
            "forecast_ts": None,
            "predicted_occupancy_pct": None,
            "address": "Адрес",
            "district": "Район",
            "max_places": "Всего мест",
            "is_paid": "Платная"
        },
        use_container_width=True,
        hide_index=True
    )

    # Детальный график по выбранной парковке
    st.subheader("Детальный прогноз по парковке")
    selected_address = st.selectbox(
        "Выберите парковку:",
        df_hour["address"].unique()
    )

    df_zone = df[df["address"] == selected_address].sort_values("forecast_ts")

    fig = px.bar(
        df_zone,
        x="forecast_ts",
        y="predicted_occupancy_pct",
        title=f"{selected_address} — прогноз",
        labels={"forecast_ts": "Время", "predicted_occupancy_pct": "Заполненность, %"}
    )
    fig.add_hline(y=80, line_dash="dash", line_color="red", annotation_text="80%")
    fig.update_layout(yaxis_range=[0, 100])
    st.plotly_chart(fig, use_container_width=True)

# =============================================
# РЕЖИМ АДМИНИСТРАТОРА
# =============================================
else:
    st.title("Панель администратора")

    col1, col2, col3 = st.columns(3)
    col1.metric("MAE", f"{metrics['mae']} %")
    col2.metric("MAPE", f"{metrics['mape']} %")
    col3.metric("Обучена", str(metrics.get("trained_at", "—"))[:19])

    st.caption("Метрики обновляются после каждого выполнения DAG")

    if st.button("📄 Скачать PDF-отчёт"):
        try:
            from report_generator import generate_pdf
            path = generate_pdf()
            with open(path, "rb") as f:
                st.download_button(
                    "Сохранить отчёт",
                    f,
                    file_name="smartparking_report.pdf",
                    mime="application/pdf"
                )
        except ImportError:
            st.warning("Генератор PDF ещё не настроен")