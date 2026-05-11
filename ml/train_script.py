"""
Скрипт обучения модели LightGBM и генерации прогноза.
Читает витрину fct_forecast_features, обучает модель,
сохраняет артефакт и метрики, записывает прогноз в таблицу predictions.
"""

import pandas as pd
import numpy as np
import lightgbm as lgb
import pickle
import logging
import yaml
import json
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from sklearn.metrics import mean_absolute_error
import os
from pathlib import Path

# =============================================
# Логирование
# =============================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)

# =============================================
# Загрузка конфигурации
# =============================================
log.info("[config] Loading configuration...")
with open("/opt/airflow/ml/config/ml_config.yml", "r") as f:
    config = yaml.safe_load(f)

db_cfg = config["database"]
model_cfg = config["model"]

DB_URL = f"postgresql://{db_cfg['user']}:{db_cfg['password']}@{db_cfg['host']}:{db_cfg['port']}/{db_cfg['dbname']}"

FEATURES = model_cfg["features"]
TARGET = model_cfg["target"]
TEST_SPLIT_PCT = model_cfg["test_split_pct"]
MODEL_PATH = model_cfg["model_path"]
METRICS_PATH = model_cfg["metrics_path"].replace(".yml", ".json")
FORECAST_HORIZON_HOURS = model_cfg["forecast_horizon_hours"]

# Создание необходимых директорий
for path in [MODEL_PATH, METRICS_PATH]:
    dir_path = os.path.dirname(path)
    if dir_path:
        Path(dir_path).mkdir(parents=True, exist_ok=True)
        log.info(f"[setup] Directory ready: {dir_path}")

# =============================================
# Чтение витрины из PostgreSQL
# =============================================
log.info("[db] Connecting to database...")
engine = create_engine(DB_URL)

log.info("[db] Reading fct_forecast_features...")
df = pd.read_sql("SELECT * FROM staging.fct_forecast_features", engine)
log.info(f"[db] Loaded {len(df)} rows, {df['pz_id'].nunique()} zones")

df = df.sort_values("hour_ts").reset_index(drop=True)

# =============================================
# Time-based split
# =============================================
split_idx = int(len(df) * (1 - TEST_SPLIT_PCT))
train_df = df.iloc[:split_idx]
test_df = df.iloc[split_idx:]

log.info(f"[split] Train: {len(train_df)} rows, Test: {len(test_df)} rows")

X_train = train_df[FEATURES]
y_train = train_df[TARGET]
X_test = test_df[FEATURES]
y_test = test_df[TARGET]

# =============================================
# Обучение LightGBM
# =============================================
log.info("[model] Training LightGBM...")

model = lgb.LGBMRegressor(
    n_estimators=100,
    max_depth=10,
    learning_rate=0.05,
    num_leaves=31,
    random_state=42,
    n_jobs=-1,
    verbosity=-1
)

model.fit(X_train, y_train)
log.info("[model] Training completed")

# =============================================
# Оценка качества
# =============================================
y_pred = model.predict(X_test)
mae = mean_absolute_error(y_test, y_pred)
mape = np.mean(np.abs((y_test - y_pred) / np.where(y_test > 0, y_test, 1))) * 100

log.info(f"[metrics] MAE: {mae:.2f} %")
log.info(f"[metrics] MAPE: {mape:.2f} %")

# Сохранение метрик в JSON (без numpy-типов)
metrics = {
    "mae": round(float(mae), 2),
    "mape": round(float(mape), 2),
    "trained_at": datetime.now().isoformat(),
    "train_rows": int(len(train_df)),
    "test_rows": int(len(test_df))
}

with open(METRICS_PATH, "w") as f:
    json.dump(metrics, f, ensure_ascii=False, indent=2)
log.info(f"[metrics] Saved to {METRICS_PATH}")

# =============================================
# Сохранение модели
# =============================================
with open(MODEL_PATH, "wb") as f:
    pickle.dump(model, f)
log.info(f"[model] Saved to {MODEL_PATH}")

# =============================================
# Генерация прогноза на FORECAST_HORIZON_HOURS часов
# =============================================
log.info(f"[predict] Generating forecast for next {FORECAST_HORIZON_HOURS} hours...")

last_hour = df["hour_ts"].max()
log.info(f"[predict] Last known hour: {last_hour}")

zones = df["pz_id"].unique()
predictions = []

for pz_id in zones:
    zone_data = df[df["pz_id"] == pz_id].iloc[-1]
    
    current_features = {
        "lag_1h": float(zone_data["occupancy_pct"]),
        "lag_24h": float(zone_data["lag_24h"]),
        "lag_168h": float(zone_data["lag_168h"]),
        "rolling_avg_7d": float(zone_data["rolling_avg_7d"]),
        "hour": int(zone_data["hour"]),
        "day_of_week": int(zone_data["day_of_week"]),
        "is_weekend": int(zone_data["is_weekend"]),
        "month": int(zone_data["month"])
    }

    for h in range(1, FORECAST_HORIZON_HOURS + 1):
        forecast_ts = last_hour + timedelta(hours=h)
        
        current_features["hour"] = forecast_ts.hour
        current_features["day_of_week"] = forecast_ts.weekday()
        current_features["is_weekend"] = 1 if forecast_ts.weekday() >= 5 else 0
        current_features["month"] = forecast_ts.month
        
        X_input = pd.DataFrame([current_features])
        pred = float(model.predict(X_input)[0])
        pred = max(0.0, min(100.0, pred))
        
        predictions.append({
            "pz_id": int(pz_id),
            "forecast_ts": forecast_ts,
            "predicted_occupancy_pct": round(pred, 2)
        })
        
        current_features["lag_168h"] = current_features["lag_24h"]
        current_features["lag_24h"] = current_features["lag_1h"]
        current_features["lag_1h"] = pred

# =============================================
# Запись прогноза в БД
# =============================================
log.info(f"[db] Inserting {len(predictions)} predictions...")

pred_df = pd.DataFrame(predictions)
pred_df.to_sql(
    "fct_predictions",
    engine,
    schema="staging",
    if_exists="append",
    index=False,
    method="multi"
)

log.info("[db] Predictions inserted successfully")
log.info(f"\nDone. Model trained, {len(predictions)} predictions generated")