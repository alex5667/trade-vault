import os
import redis
import json

def main():
    redis_url = os.environ.get("REDIS_URL")
    r = redis.from_url(redis_url, decode_responses=True)
    text = """✅ <b>ML Scorer V3 (Binary) — АВТО-ПРИНЯТА</b>

📊 <b>OOF Metrics (4 folds)</b>
  • ROC-AUC: <code>0.8178</code>
  • LogLoss: <code>0.5723</code>
  • Brier:   <code>0.2065</code>
  • Top5%:   <code>2.94%</code>

🏆 <b>Сравнение с чемпионом</b>
  • Первая модель — чемпиона не было

💡 Причина: <code>no_champion_yet</code>
  • Samples: <code>27297</code>"""
    
    fields = {
        "type": "report",
        "text": text,
        "parse_mode": "HTML",
        "source": "ml_scorer_v3"
    }
    r.xadd("notify:telegram", fields, maxlen=50000)
    print("Message sent to notify:telegram")

if __name__ == "__main__":
    main()
