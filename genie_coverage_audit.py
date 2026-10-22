# Databricks notebook source
# MAGIC %md
# MAGIC # Genie time-coverage audit
# MAGIC
# MAGIC **Hypothesis:** when the data behind a Genie space lags, answers to time-based questions cover a different or partial period than the user asked about, and the answer doesn't say so.
# MAGIC
# MAGIC **Design:** one synthetic `orders` dataset, cut off at four points (fresh, stalled, monthly batch, archived). Same questions on every table, each in a new conversation. Experiment 2 repeats a subset on a stalled space that has a "state your dates" instruction.
# MAGIC
# MAGIC **Steps**
# MAGIC 1. Run cells 1–3 (install SDK, build tables, record ground truth).
# MAGIC 2. In the UI, create 5 Genie spaces (see instructions in cell 4) and paste their IDs into cell 4.
# MAGIC 3. Run the rest. Results are appended to `workspace.default.genie_audit_results` after every question, so you can re-run safely if anything stops.
# MAGIC 4. In the last cell, download the results table as CSV.

# COMMAND ----------

# MAGIC %pip install --upgrade databricks-sdk --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# Cell 2: build the four tables from ONE base dataset (identical data, different cutoffs)
import datetime as dt
import numpy as np
import pandas as pd

CATALOG, SCHEMA = "workspace", "default"
today = dt.date.today()
yesterday = today - dt.timedelta(days=1)

cutoffs = {
    "orders_fresh":    yesterday,
    "orders_stalled":  today - dt.timedelta(days=10),
    "orders_monthly":  today.replace(day=1) - dt.timedelta(days=1),   # last day of previous month
    "orders_archived": today - dt.timedelta(days=366),
}
WINDOW_DAYS = 180

base_start = min(cutoffs.values()) - dt.timedelta(days=WINDOW_DAYS)
days = pd.date_range(base_start, yesterday, freq="D").date

rng = np.random.default_rng(42)
regions = ["North", "South", "East", "West"]
categories = {"Electronics": 120.0, "Home": 65.0, "Apparel": 40.0, "Grocery": 18.0}
rows = []
for i, d in enumerate(days):
    weekday_factor = 1.15 if d.weekday() >= 5 else 1.0
    trend = 1 + 0.0004 * i
    for r in regions:
        for c, price in categories.items():
            orders = max(1, int(rng.normal(60, 8) * weekday_factor * trend))
            revenue = round(orders * price * rng.uniform(0.95, 1.05), 2)
            rows.append((d, r, c, orders, revenue))
base = pd.DataFrame(rows, columns=["order_date", "region", "category", "orders", "revenue"])

for name, end in cutoffs.items():
    start = end - dt.timedelta(days=WINDOW_DAYS - 1)
    part = base[(base.order_date >= start) & (base.order_date <= end)].copy()
    (spark.createDataFrame(part)
          .selectExpr("CAST(order_date AS DATE) AS order_date", "region", "category",
                      "CAST(orders AS INT) AS orders", "CAST(revenue AS DOUBLE) AS revenue")
          .write.mode("overwrite").option("overwriteSchema", "true")
          .saveAsTable(f"{CATALOG}.{SCHEMA}.{name}"))
    print(f"{name}: {start} to {end}  ({len(part):,} rows)")

# COMMAND ----------

# Cell 3: record ground truth (what each table actually covers) for scoring later
gt = []
for name in cutoffs:
    r = spark.sql(f"SELECT MIN(order_date) mn, MAX(order_date) mx, COUNT(*) n FROM {CATALOG}.{SCHEMA}.{name}").first()
    gt.append((name, str(r.mn), str(r.mx), int(r.n), str(today), (today - r.mx).days))
gt_df = spark.createDataFrame(gt, "table STRING, min_date STRING, max_date STRING, row_count INT, run_date STRING, lag_days INT")
gt_df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.genie_audit_ground_truth")
display(gt_df)

# COMMAND ----------

# Cell 4: paste your Genie space IDs here.
#
# How to create each space (UI, ~2 min each):
#   Sidebar > Genie Agents > New > select ONLY the one table listed below > create.
#   Do NOT add instructions, except for "stalled_instructed".
#   Copy the space ID from the URL: .../genie/rooms/<THIS PART>/...
#
# For "stalled_instructed": same table as stalled (orders_stalled), then open Configure > Instructions and paste:
#   Always state the exact date range your answer covers. If the data ends before today, say so and give the latest date available.

SPACES = {
    "fresh":              "PASTE_ID",   # orders_fresh
    "stalled":            "PASTE_ID",   # orders_stalled
    "monthly":            "PASTE_ID",   # orders_monthly
    "archived":           "PASTE_ID",   # orders_archived
    "stalled_instructed": "PASTE_ID",   # orders_stalled + instruction
}
assert all(v != "PASTE_ID" for v in SPACES.values()), "Paste all five space IDs first"

# COMMAND ----------

# Cell 5: the test plan (fixed before running)
QUESTIONS = [
    ("Q01", "day",               "What was total revenue yesterday?"),
    ("Q02", "rolling days",      "What was total revenue in the last 7 days?"),
    ("Q03", "calendar week",     "What was total revenue last week?"),
    ("Q04", "week to date",      "How much revenue have we made this week so far?"),
    ("Q05", "calendar month",    "What was total revenue last month?"),
    ("Q06", "month to date",     "What is month-to-date revenue?"),
    ("Q07", "rolling + breakdown","Show revenue for the last 30 days by region"),
    ("Q08", "comparison",        "How did revenue this month compare to last month?"),
    ("Q09", "calendar quarter",  "What was revenue last quarter?"),
    ("Q10", "year to date",      "What is year-to-date revenue?"),
    ("Q11", "vague recency",     "What are our most recent sales numbers?"),
    ("Q12", "explicit (control)",f"What was total revenue in {(today.replace(day=1) - dt.timedelta(days=1)).strftime('%B %Y')}?"),
]
REPEAT_IDS = ["Q03", "Q05", "Q08", "Q11"]            # asked a second time on each main space
INSTRUCTED_IDS = ["Q01", "Q03", "Q06", "Q07", "Q08", "Q10"]

plan = []
for key in ["fresh", "stalled", "monthly", "archived"]:
    for qid, grain, q in QUESTIONS:
        plan.append((key, qid, grain, q, 1))
    for qid, grain, q in QUESTIONS:
        if qid in REPEAT_IDS:
            plan.append((key, qid, grain, q, 2))
for qid, grain, q in QUESTIONS:
    if qid in INSTRUCTED_IDS:
        plan.append(("stalled_instructed", qid, grain, q, 1))
print(len(plan), "runs planned")

# COMMAND ----------

# Cell 6: runner (official Genie Conversation API, authenticated as you via the notebook)
import json, time, traceback
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
RESULTS = f"{CATALOG}.{SCHEMA}.genie_audit_results"
MIN_GAP_S = 15          # stay under Genie API rate limits
TERMINAL = {"COMPLETED", "FAILED", "CANCELLED", "QUERY_RESULT_EXPIRED"}

def call(method, path, body=None, tries=6):
    delay = 5
    for t in range(tries):
        try:
            return w.api_client.do(method, path, body=body)
        except Exception as e:
            msg = str(e)
            if t < tries - 1 and any(s in msg for s in ["429", "RESOURCE_EXHAUSTED", "rate", "503", "TEMPORARILY"]):
                time.sleep(delay); delay = min(delay * 2, 60); continue
            raise

def ask(space_id, question):
    start = call("POST", f"/api/2.0/genie/spaces/{space_id}/start-conversation", {"content": question})
    cid = start.get("conversation_id") or start.get("conversation", {}).get("id")
    mid = start.get("message_id") or start.get("message", {}).get("id")
    t0, delay = time.time(), 2
    while True:
        msg = call("GET", f"/api/2.0/genie/spaces/{space_id}/conversations/{cid}/messages/{mid}")
        if msg.get("status") in TERMINAL or time.time() - t0 > 600:
            break
        time.sleep(delay); delay = min(delay * 1.5, 30)
    return cid, mid, msg

def query_result(space_id, cid, mid, aid):
    for path in [f"/api/2.0/genie/spaces/{space_id}/conversations/{cid}/messages/{mid}/attachments/{aid}/query-result",
                 f"/api/2.0/genie/spaces/{space_id}/conversations/{cid}/messages/{mid}/query-result/{aid}"]:
        try:
            r = call("GET", path)
            sr = r.get("statement_response", r)
            cols = [c.get("name") for c in sr.get("manifest", {}).get("schema", {}).get("columns", [])]
            data = sr.get("result", {}).get("data_array", []) or []
            return json.dumps({"columns": cols, "rows": data[:20], "row_count": len(data)})
        except Exception:
            continue
    return None

spark.sql(f"""CREATE TABLE IF NOT EXISTS {RESULTS} (
  run_key STRING, space STRING, qid STRING, grain STRING, question STRING, attempt INT,
  status STRING, answer_text STRING, sql STRING, query_description STRING, result_preview STRING,
  conversation_id STRING, message_id STRING, asked_at STRING, seconds DOUBLE, error STRING, raw_json STRING)""")
done = {r.run_key for r in spark.table(RESULTS).select("run_key").collect()}

last = 0
for space, qid, grain, q, attempt in plan:
    run_key = f"{space}|{qid}|{attempt}"
    if run_key in done:
        continue
    wait = MIN_GAP_S - (time.time() - last)
    if wait > 0: time.sleep(wait)
    last, t0 = time.time(), time.time()
    rec = dict(run_key=run_key, space=space, qid=qid, grain=grain, question=q, attempt=attempt,
               status=None, answer_text=None, sql=None, query_description=None, result_preview=None,
               conversation_id=None, message_id=None, asked_at=dt.datetime.now().isoformat(),
               seconds=None, error=None, raw_json=None)
    try:
        cid, mid, msg = ask(SPACES[space], q)
        rec.update(conversation_id=cid, message_id=mid, status=msg.get("status"), raw_json=json.dumps(msg)[:60000])
        texts, sqls, descs, previews = [], [], [], []
        for a in msg.get("attachments", []) or []:
            if a.get("text"):
                texts.append(a["text"].get("content", ""))
            if a.get("query"):
                sqls.append(a["query"].get("query", ""))
                descs.append(a["query"].get("description", "") or "")
                aid = a.get("attachment_id") or a.get("id")
                if aid:
                    previews.append(query_result(SPACES[space], cid, mid, aid) or "")
        rec.update(answer_text="\n".join(texts) or msg.get("content"), sql="\n---\n".join(sqls),
                   query_description="\n".join(descs), result_preview="\n".join(previews))
    except Exception as e:
        rec["error"] = (str(e) + "\n" + traceback.format_exc())[:5000]
    rec["seconds"] = round(time.time() - t0, 1)
    spark.createDataFrame([rec], spark.table(RESULTS).schema).write.mode("append").saveAsTable(RESULTS)
    print(f"{run_key:<32} {rec['status'] or 'ERROR':<10} {rec['seconds']}s  {(rec['answer_text'] or rec['error'] or '')[:90]!r}")

# COMMAND ----------

# Cell 7: check progress / errors, then download CSV from the table below (Download icon > CSV)
res = spark.table(RESULTS)
display(res.groupBy("space", "status").count().orderBy("space"))
display(res.orderBy("space", "qid", "attempt").drop("raw_json"))

# COMMAND ----------

# Cell 8 (optional): also download the raw JSON version if I ask for it
display(spark.table(RESULTS).select("run_key", "raw_json"))
