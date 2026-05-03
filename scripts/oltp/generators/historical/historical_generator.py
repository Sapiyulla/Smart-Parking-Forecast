import random
import yaml
import psycopg2
from datetime import datetime, timedelta

# =============================================
# Загрузка конфигурации
# =============================================
with open("generation.config.yml", "r") as f:
    config = yaml.safe_load(f)

db_cfg = config["database"]
gen_cfg = config["generation"]
dirt_cfg = config["dirt"]
lp_cfg = config["load_pattern"]

print(f"[config] Configuration loaded.")

# =============================================
# Подключение к БД, получение parking_zones
# =============================================
print(f"[postgres] Connecting to OLTP database...")
conn = psycopg2.connect(**db_cfg)
cur = conn.cursor()
cur.execute("SELECT pz_id, max_places, storeys_count, is_paid FROM parking_zones")
zones = {row[0]: {"max_places": row[1], "storeys": row[2], "is_paid": row[3]} for row in cur.fetchall()}

pz_ids = list(zones.keys())
print(f"[postgres] Connected.")

# =============================================
# Параметры периода
# =============================================
end_date = datetime.now().replace(hour=23, minute=59, second=59)
start_date = end_date - timedelta(days=gen_cfg["years_back"] * 365)
total_records = gen_cfg["records_count"]

# =============================================
# Счётчики занятых мест по каждой зоне (текущее состояние)
# =============================================
occupied = {pz_id: int(zones[pz_id]["max_places"] * gen_cfg["initial_occupancy_pct"] / 100)
            for pz_id in pz_ids}

# Храним timestamp последнего въезда для каждой (pz_id, storey) — чтобы чистить зависшие
last_entrance = {}

# =============================================
# Вспомогательные функции
# =============================================
def get_hour_factor(hour):
    if hour in lp_cfg["night_hours"]:
        return 0.1
    elif hour in lp_cfg["morning_peak"] or hour in lp_cfg["evening_peak"]:
        return 0.9
    else:
        return 0.5

def get_day_factor(weekday):
    if weekday in lp_cfg["high_demand_days"]:
        return 1.2
    elif weekday in lp_cfg["low_demand_days"]:
        return 0.6
    return 1.0

def get_seasonal_factor(month):
    return lp_cfg["seasonal_factors"].get(month, 1.0)

def probability_of_event(pz_id, ts):
    """Вероятность въезда в момент ts"""
    max_p = zones[pz_id]["max_places"]
    free = max_p - occupied[pz_id]
    if free <= 0:
        return 0.0
    base = free / max_p
    hour_f = get_hour_factor(ts.hour)
    day_f = get_day_factor(ts.weekday())
    season_f = get_seasonal_factor(ts.month)
    return base * hour_f * day_f * season_f

# =============================================
# Генерация списка событий
# =============================================
events = []
ts_current = start_date
delta = (end_date - start_date) / total_records

while ts_current < end_date:
    pz_id = random.choice(pz_ids)
    storey = random.randint(1, zones[pz_id]["storeys"])
    zone = zones[pz_id]
    is_paid = zone["is_paid"]

    # Въезд или выезд?
    if occupied[pz_id] == 0:
        action = "entrance"
    elif occupied[pz_id] >= zone["max_places"]:
        action = "exit"
    else:
        # Смещаем баланс: больше въездов в пики, больше выездов ночью
        prob_entrance = probability_of_event(pz_id, ts_current)
        if random.random() < prob_entrance:
            action = "entrance"
        else:
            action = "exit"

    rate = round(random.uniform(50, 300), 2) if is_paid and action == "entrance" else 0.0

    events.append((pz_id, storey, action, rate, ts_current))

    # Обновляем счётчики
    if action == "entrance":
        occupied[pz_id] += 1
        last_entrance[(pz_id, storey)] = ts_current
    else:
        if occupied[pz_id] > 0:
            occupied[pz_id] -= 1

    ts_current += delta

# =============================================
# Внесение грязи
# =============================================
n = len(events)

# 1. Orphan exit (exit без entrance) — вставим лишние exit
for _ in range(int(n * dirt_cfg["orphan_exit_pct"] / 100)):
    idx = random.randint(0, n - 1)
    pz_id, storey, _, rate, ts = events[idx]
    events.append((pz_id, storey, "exit", 0.0, ts))

# 2. Duplicates — дублируем записи подряд
for _ in range(int(n * dirt_cfg["duplicate_pct"] / 100)):
    idx = random.randint(0, n - 1)
    events.insert(idx, events[idx])

# 3. Negative duration — меняем порядок timestamp для пары entrance/exit
for _ in range(int(n * dirt_cfg["negative_duration_pct"] / 100)):
    i = random.randint(0, n - 2)
    j = i + 1
    if events[i][2] == "entrance" and events[j][2] == "exit":
        # Меняем местами ts
        e1, e2 = list(events[i]), list(events[j])
        e1[4], e2[4] = e2[4], e1[4]
        events[i], events[j] = tuple(e1), tuple(e2)

# 4. Missing days — удалим все события за выбранные дни
missing_days_list = []
for _ in range(dirt_cfg["missing_days"]):
    day = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
    missing_days_list.append(day.date())

events = [e for e in events if e[4].date() not in missing_days_list]

# =============================================
# Сортировка по времени
# =============================================
events.sort(key=lambda x: x[4])

# =============================================
# Вставка в БД
# =============================================
insert_sql = """
    INSERT INTO parkings (pz_id, storey, action, rate, ts)
    VALUES (%s, %s, %s, %s, %s)
"""

print("[postgres] Starting batched insert...")
batch_size = 1000
for i in range(0, len(events), batch_size):
    batch = events[i:i + batch_size]
    cur.executemany(insert_sql, batch)
    conn.commit()

print("[postgres] Inserting done.")
# =============================================
# Финал
# =============================================
cur.close()
conn.close()

print(f"\nGeneration done. Total records inserted: {len(events)}")
print(f"Missing days: {missing_days_list}")