
import json
import unittest


# Mocking the worker's extraction logic
def extract_text(message_data):
    payload_str = message_data.get("payload")
    payload = {}

    if payload_str:
        try:
            payload = json.loads(payload_str) if isinstance(payload_str, str) else payload_str
        except json.JSONDecodeError:
            payload = {"message": str(payload_str)}

    text = payload.get("message", "")
    if not text:
        # Fallback as implemented in the fix
        text = (
            message_data.get("message", "") or
            message_data.get("text", "") or
            message_data.get("caption", "") or
            payload.get("text", "") or
            payload.get("caption", "")
        )
    return text

class TestTelegramPayload(unittest.TestCase):
    def test_standard_payload(self):
        msg = {"payload": json.dumps({"message": "hello world"})}
        self.assertEqual(extract_text(msg), "hello world")

    def test_direct_text(self):
        msg = {"text": "hello direct"}
        self.assertEqual(extract_text(msg), "hello direct")

    def test_caption_fallback(self):
        # The case that was failing in TelegramReporterExt
        msg = {"caption": "my photo caption"}
        self.assertEqual(extract_text(msg), "my photo caption")

    def test_nested_text(self):
        msg = {"payload": json.dumps({"text": "nested text"})}
        self.assertEqual(extract_text(msg), "nested text")

    def test_nested_caption(self):
        msg = {"payload": json.dumps({"caption": "nested caption"})}
        self.assertEqual(extract_text(msg), "nested caption")

if __name__ == "__main__":
    unittest.main()
