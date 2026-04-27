import redis
import json
import time

def send_test_report():
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    
    text = "🧪 <b>Fix Verification: Manual ML Report</b>\n\nChecking if 'Approve challenger' button is now visible.\n\nRecommend challenger: LR ver=test_v1"
    
    buttons = [[
        {"text": "👀 Preview diff", "callback": "recs:preview2:test_bundle:sig123"},
        {"text": "✅ Approve challenger", "callback": "recs:confirm:test_bundle:sig123"},
        {"text": "❌ Reject", "callback": "recs:reject:test_bundle:sig123"},
    ]]
    
    fields = {
        "type": "report",
        "text": text,
        "ts": str(int(time.time() * 1000)),
        "buttons": json.dumps(buttons)
    }
    
    r.xadd("notify:telegram", fields)
    print("✅ Pushed test report to notify:telegram stream")

if __name__ == "__main__":
    send_test_report()
