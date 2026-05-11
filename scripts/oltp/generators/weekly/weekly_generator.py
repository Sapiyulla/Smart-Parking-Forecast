import random
import yaml
import psycopg2
from datetime import datetime, timedelta
from collections import defaultdict
import logging

# =============================================
# Логирование
# =============================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)

# =============================================
# Загрузка конфигурации
# =============================================
log.info("[config] Loading weekly generation configuration...")
with open("weekly_generation.config.yml", "r") as f:
    config = yaml.safe_load(f)

db_cfg = config["database"]
gen_cfg = config["generation"]
dirt_cfg = config["dirt"]
lp_cfg = config["load_pattern"]

# =============================================
# Определение периода: последняя запись в БД → текущий час
# =============================================
log.info("[postgres] Connecting to OLTP database...")
conn = psycopg2.connect(**db_cfg)
cur = conn.cursor()

# Находим последнюю временную метку в parkings
cur.execute("SELECT MAX(ts) FROM parkings")
last_ts = cur.fetchone()[0]

if last_ts is None:
    # Если таблица пуста — начинаем с трёх лет назад (историческая генерация ещё не запускалась)
    log.warning("[postgres] Table 'parkings' is empty. Fallback to 3 years ago.")
    last_ts = datetime.now() - timedelta(days=3 * 365)

# Округляем до начала следующего часа
start_ts = last_ts.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)

# Конец периода — начало текущего часа
end_ts = datetime.now().replace(minute=0, second=0, microsecond=0)

# Если после последней записи прошло меньше часа — генерировать нечего
if start_ts >= end_ts:
    log.info(f"[config] No new data to generate. Last ts: {last_ts}, current hour: {end_ts}")
    cur.close()
    conn.close()
    exit(0)

log.info(f"[config] Generation period: {last_ts} → {end_ts} (start: {start_ts})")

# =============================================
# Получение parking_zones
# =============================================
cur.execute("SELECT pz_id, max_places, storeys_count, is_paid FROM parking_zones")
zones = {row[0]: {"max_places": row[1], "storeys": row[2], "is_paid": row[3]} for row in cur.fetchall()}
pz_ids = list(zones.keys())
log.info("[postgres] Parking zones loaded.")

# =============================================
# Вычисление количества записей
# =============================================
hours_diff = (end_ts - start_ts).total_seconds() / 3600
# Пропорционально количеству часов: чем больше разрыв, тем больше записей
base_records = gen_cfg["min_records"] + random.randint(0, gen_cfg["records_range"])
total_records = max(100, int(base_records * (hours_diff / 168)))  # 168 часов в неделе
log.info(f"[config] Hours since last record: {hours_diff:.1f}h, target records: {total_records}")

# =============================================
# Определение начальной занятости
# =============================================
occupied = {}
for pz_id in pz_ids:
    cur.execute("""
        SELECT COUNT(*) FILTER (WHERE action = 'entrance') - COUNT(*) FILTER (WHERE action = 'exit')
        FROM parkings
        WHERE pz_id = %s AND ts <= %s
    """, (pz_id, last_ts))
    balance = cur.fetchone()[0] or 0
    occupied[pz_id] = max(0, min(balance, zones[pz_id]["max_places"]))

log.info("[postgres] Initial occupancy calculated.")

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
# Генерация событий
# =============================================
events = []
ts_current = start_ts
delta = (end_ts - start_ts) / total_records if total_records > 0 else timedelta(hours=1)

log.info("[generator] Generating events...")
while ts_current < end_ts:
    pz_id = random.choice(pz_ids)
    storey = random.randint(1, zones[pz_id]["storeys"])
    zone = zones[pz_id]
    is_paid = zone["is_paid"]

    if occupied[pz_id] == 0:
        action = "entrance"
    elif occupied[pz_id] >= zone["max_places"]:
        action = "exit"
    else:
        prob_entrance = probability_of_event(pz_id, ts_current)
        action = "entrance" if random.random() < prob_entrance else "exit"

    rate = round(random.uniform(50, 300), 2) if is_paid and action == "entrance" else 0.0

    events.append((pz_id, storey, action, rate, ts_current))

    if action == "entrance":
        occupied[pz_id] += 1
    else:
        if occupied[pz_id] > 0:
            occupied[pz_id] -= 1

    ts_current += delta

log.info(f"[generator] Base events generated: {len(events)}")

# =============================================
# Внесение грязи
# =============================================
n = len(events)
if n == 0:
    log.info("[generator] No events to dirty. Exiting.")
    cur.close()
    conn.close()
    exit(0)

# 1. Orphan exits
orphan_count = int(n * dirt_cfg["orphan_exit_pct"] / 100)
for _ in range(orphan_count):
    idx = random.randint(0, n - 1)
    pz_id, storey, _, rate, ts = events[idx]
    events.append((pz_id, storey, "exit", 0.0, ts))
log.info(f"[dirt] Orphan exits added: {orphan_count}")

# 2. Duplicates
dup_count = int(n * dirt_cfg["duplicate_pct"] / 100)
for _ in range(dup_count):
    idx = random.randint(0, len(events) - 1)
    events.insert(idx, events[idx])
log.info(f"[dirt] Duplicates added: {dup_count}")

# 3. Negative duration
neg_count = int(n * dirt_cfg["negative_duration_pct"] / 100)
for _ in range(neg_count):
    i = random.randint(0, len(events) - 2)
    j = i + 1
    if events[i][2] == "entrance" and events[j][2] == "exit":
        e1, e2 = list(events[i]), list(events[j])
        e1[4], e2[4] = e2[4], e1[4]
        events[i], events[j] = tuple(e1), tuple(e2)
log.info(f"[dirt] Negative durations added: {neg_count}")

# 4. Missing hours
missing_hours = []
for _ in range(dirt_cfg["missing_hours"]):
    hour_start = start_ts + timedelta(hours=random.randint(0, max(0, int(hours_diff) - 1)))
    missing_hours.append(hour_start.replace(minute=0, second=0, microsecond=0))

events = [e for e in events 
          if e[4].replace(minute=0, second=0, microsecond=0) not in missing_hours]
log.info(f"[dirt] Missing hours: {[h.strftime('%Y-%m-%d %H:00') for h in missing_hours]}")

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

log.info("[postgres] Starting batched insert...")
batch_size = 1000
for i in range(0, len(events), batch_size):
    batch = events[i:i + batch_size]
    cur.executemany(insert_sql, batch)
    conn.commit()

cur.close()
conn.close()

log.info("[postgres] Inserting done.")
log.info(f"\nGeneration done. Total records inserted: {len(events)}")
log.info(f"Period: {start_ts} → {end_ts}")