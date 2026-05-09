import json
import logging
import os
import re
import time
import urllib.error
import urllib.request

logger = logging.getLogger(__name__)

# ENV config
ENABLED = os.getenv("TELEGRAM_LLM_ANALYSIS_ENABLED", "1").strip().lower() in ("1", "true", "yes")
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://ollama:11434").rstrip("/")
MODEL = os.getenv("TELEGRAM_LLM_MODEL", "deepseek-r1:7b")
TIMEOUT = float(os.getenv("TELEGRAM_LLM_TIMEOUT_SEC", "300.0"))
MIN_TEXT_LEN = int(os.getenv("TELEGRAM_LLM_MIN_LEN", "30"))
MAX_SOURCE_LEN = int(os.getenv("TELEGRAM_LLM_MAX_SOURCE_LEN", "16000"))

class TelegramMessageAnalyzer:
    """Utility to analyze Telegram messages using a local LLM before sending."""

    # Unicode ranges to strip from LLM output:
    # CJK Unified Ideographs, Hiragana, Katakana, Hangul, Arabic, Hebrew, Thai, etc.
    _EXOTIC_RE = re.compile(
        r"[\u2600-\u27BF"    # Misc symbols, Dingbats (includes ✅ ✓ ✗ etc.)
        r"\u4E00-\u9FFF"     # CJK Unified Ideographs
        r"\u3000-\u303F"     # CJK Symbols and Punctuation
        r"\u3040-\u30FF"     # Hiragana, Katakana
        r"\uAC00-\uD7AF"     # Hangul Syllables
        r"\u0600-\u06FF"     # Arabic
        r"\u0590-\u05FF"     # Hebrew
        r"\u0E00-\u0E7F"     # Thai
        r"\uF900-\uFAFF"     # CJK Compatibility Ideographs
        r"\U0001F300-\U0001F9FF"  # Emoji (Misc Symbols, Transport, People, etc.)
        r"\U00010000-\U0001FFFF"  # Other supplementary planes
        r"]"
    )

    @staticmethod
    def _clean_llm_text(text: str) -> str:
        """Strip exotic unicode, CJK chars, and LLM-generated emoji from text."""
        if not text:
            return text
        cleaned = TelegramMessageAnalyzer._EXOTIC_RE.sub("", text)
        # Collapse multiple spaces left after removal
        cleaned = re.sub(r" {2,}", " ", cleaned).strip()
        return cleaned


    @staticmethod
    def is_enabled() -> bool:
        return ENABLED

    @staticmethod
    def analyze_and_reply(text: str, chat_id: str, reply_to_message_id: int, bot_token: str) -> None:
        if not ENABLED:
            return

        if not text or len(text.strip()) < MIN_TEXT_LEN:
            return

        if len(text) > MAX_SOURCE_LEN:
            logger.info(f"Skipping LLM analysis: source text too long ({len(text)} chars)")
            return

        # Skip automated reports
        if any(marker in text for marker in ["🤖 РАПОРТ", "🤖 <b>ML RCA", "🤖", "AIOPS"]):
            return

        # Give it a luxurious 240s timeout in the background for deeper context
        analysis = TelegramMessageAnalyzer._get_llm_analysis(text, timeout_sec=240.0)
        if not analysis:
            return

        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        # Since LLMs can output arbitrary chars, omit parse_mode to prevent HTML/MD breakage
        payload = {
            "chat_id": chat_id,
            "text": f"--- DeepSeek Analysis ---\n{analysis}",
            "reply_to_message_id": reply_to_message_id
        }

        try:
            data = json.dumps(payload).encode("utf-8")
            req = urllib.request.Request(
                url,
                data=data,
                headers={"Content-Type": "application/json"}
            )
            with urllib.request.urlopen(req, timeout=10) as response:
                logger.info(f"Successfully posted LLM reply for msg {reply_to_message_id}")
        except Exception as e:
            logger.warning(f"Failed to post LLM reply: {e}")

    @staticmethod
    def analyze(text: str, payload: dict = None, timeout_sec: float = None) -> str:
        """
        Takes original message text, gets analysis from LLM, 
        and returns text with analysis appended.
        """
        if not ENABLED:
            return text

        if not text or len(text.strip()) < MIN_TEXT_LEN:
            return text

        if len(text) > MAX_SOURCE_LEN:
            logger.info(f"Skipping LLM analysis: source text too long ({len(text)} chars)")
            return text

        if "--- DeepSeek Analysis ---" in text:
            return text

        # Skip automated reports
        if any(marker in text for marker in ["🤖 РАПОРТ", "🤖 <b>ML RCA", "🤖", "AIOPS"]):
            return text

        payload_dict = payload or {}
        analysis = TelegramMessageAnalyzer._get_llm_analysis(text, payload_dict, timeout_sec=timeout_sec)
        if not analysis:
            return text

        # Formatting: ensure a couple of newlines before analysis
        divider = "\n\n--- DeepSeek Analysis ---\n"
        return f"{text}{divider}{analysis}"

    @staticmethod
    def _language_guard(parsed: dict) -> bool:
        """Return True if parsed JSON looks like valid Russian output.

        Detection strategy:
        1. Reject if Ukrainian-specific letters (і ї є ґ — absent in Russian) exceed 5% of
           all Cyrillic chars. These letters are U+0456, U+0457, U+0454, U+0491.
        2. Reject if CJK/exotic non-Latin/non-Cyrillic alphabetic chars exceed 3% of all alpha.
        3. Reject if Cyrillic ratio < 30% of all alpha (output is mostly English).
        4. Pass-through if not enough text to judge.
        """
        # Ukrainian-specific Cyrillic codepoints absent from Russian
        _UKR_SPECIFIC = frozenset("іїєґІЇЄҐ")  # U+0456,0457,0454,0491 + uppercase

        text_parts: list[str] = []
        for key in ("summary_1line", "facts", "risks", "assumptions", "steps_now", "steps_later", "root_cause_hypotheses"):
            val = parsed.get(key)
            if isinstance(val, str):
                text_parts.append(val)
            elif isinstance(val, list):
                text_parts.extend(str(x) for x in val)
        # Also check nested operator_action
        action = parsed.get("operator_action", {})
        if isinstance(action, dict):
            for key in ("steps_now", "steps_later"):
                val = action.get(key)
                if isinstance(val, list):
                    text_parts.extend(str(x) for x in val)

        combined = " ".join(text_parts)
        if not combined.strip():
            return True  # no text to check — pass through

        alpha_chars = [c for c in combined if c.isalpha()]
        if len(alpha_chars) < 10:
            return True  # too short to judge

        cyrillic_chars = [c for c in alpha_chars if "\u0400" <= c <= "\u04FF"]
        ukr_count = sum(1 for c in combined if c in _UKR_SPECIFIC)
        # CJK Unified Ideographs and other non-Latin/non-Cyrillic alpha
        exotic_count = sum(
            1 for c in alpha_chars
            if not ("\u0400" <= c <= "\u04FF") and not ("A" <= c <= "z")
        )

        # Check 1: Ukrainian-specific chars
        if cyrillic_chars and ukr_count / len(cyrillic_chars) > 0.05:
            logger.warning(
                f"Language guard: Ukrainian-specific chars ratio="
                f"{ukr_count/len(cyrillic_chars):.2f} (sample={combined[:100]!r})"
            )
            return False

        # Check 2: exotic chars
        if exotic_count / len(alpha_chars) > 0.03:
            logger.warning(
                f"Language guard: exotic chars ratio={exotic_count/len(alpha_chars):.2f} "
                f"(sample={combined[:100]!r})"
            )
            return False

        # Check 3: minimum Cyrillic ratio (reject mostly-English output)
        _min_cyr = float(os.getenv("LANG_GUARD_MIN_CYRILLIC_RATIO", "0.10"))
        cyrillic_ratio = len(cyrillic_chars) / len(alpha_chars)
        if cyrillic_ratio < _min_cyr:
            logger.warning(
                f"Language guard: low Cyrillic ratio={cyrillic_ratio:.2f} "
                f"(cyrillic={len(cyrillic_chars)}/{len(alpha_chars)}) "
                f"(sample={combined[:100]!r})"
            )
            return False

        return True


    @staticmethod
    def _get_llm_analysis(text: str, payload: dict = None, timeout_sec: float = None) -> str | None:
        """Calls local Ollama instance using the notification LLM registry."""

        try:
            from utils.notification_llm_registry import build_analysis_envelope, validate_llm_response
        except ImportError as e:
            logger.error(f"Cannot import notification_llm_registry: {e}")
            return None

        # Build req from registry (which also sanitizes the payload)
        # We ensure 'text' is accessible heuristic routing later even if missing from payload
        payload = dict(payload) if payload else {}
        if "text" not in payload and "message" not in payload:
            payload["text"] = text

        source_service = (payload.get("source_service", payload.get("source", "unknown_source")))

        envelope = build_analysis_envelope(
            source_service=source_service,
            payload=payload,
            model=MODEL,
        )
        llm_request_payload = envelope["llm_request"]

        # Extract routed type for diagnostics
        routed_type = envelope.get("notification_type", "unknown")
        logger.info(f"LLM analysis: routed_type={routed_type}, source={source_service}")

        # Use temperature and max_tokens from the registry request struct
        prof_temperature = llm_request_payload.get("temperature", 0.15)
        prof_max_tokens = llm_request_payload.get("max_tokens", 600)

        # Ollama /api/chat payload
        endpoint = f"{OLLAMA_BASE_URL}/api/chat"
        # Extract messages and options from the registry request struct
        messages = llm_request_payload.get("messages", [])

        request_body = {
            "model": MODEL,
            "messages": messages,
            "stream": False,
            "format": "json",  # force JSON mode in Ollama
            "options": {
                "temperature": prof_temperature,
                "num_predict": min(prof_max_tokens, 600),
                "num_ctx": 8192,
                "repeat_penalty": 1.05,
            }
        }

        actual_timeout = timeout_sec if timeout_sec is not None else TIMEOUT
        response_text = None
        t0 = time.monotonic()

        try:
            max_retries = 10
            retry_delay = 5.0

            for attempt in range(max_retries):
                try:
                    data = json.dumps(request_body).encode("utf-8")
                    req = urllib.request.Request(
                        endpoint,
                        data=data,
                        headers={"Content-Type": "application/json"}
                    )
                    with urllib.request.urlopen(req, timeout=actual_timeout) as resp:
                        result = json.loads(resp.read().decode("utf-8"))

                    message_obj = result.get("message", {})
                    response_text = message_obj.get("content", "").strip()
                    if response_text:
                        break # Success

                except (urllib.error.URLError, ConnectionError) as e:
                    is_last = attempt == max_retries - 1
                    if is_last:
                        logger.warning(f"Telegram LLM analysis: failed to connect to Ollama after {max_retries} attempts: {e}")
                        return None
                    logger.warning(f"Telegram LLM analysis: Ollama not ready ({e}). Retrying in {retry_delay}s (attempt {attempt+1}/{max_retries})...")
                    time.sleep(retry_delay)
                except Exception as e:
                    logger.warning(f"Telegram LLM analysis: unexpected error during request: {e}")
                    return None

            if not response_text:
                return None

            # Post-processing: remove <think> tags if present
            response_text = re.sub(r'<think>.*?</think>', '', response_text, flags=re.DOTALL).strip()

            # Clean markdown JSON wraps
            response_text = re.sub(r'^```(?:json)?', '', response_text, flags=re.IGNORECASE | re.MULTILINE)
            response_text = re.sub(r'```$', '', response_text, flags=re.MULTILINE).strip()

            # Attempt to parse json
            start_idx = response_text.find('{')
            end_idx = response_text.rfind('}')
            if start_idx != -1 and end_idx != -1:
                json_str = response_text[start_idx:end_idx+1]
            else:
                json_str = response_text

            try:
                parsed = json.loads(json_str)

                # Language guard: reject non-Russian output
                if not TelegramMessageAnalyzer._language_guard(parsed):
                    logger.warning("LLM response failed language guard — discarding to avoid gibberish")
                    return None

                # Validate and correct reason code if needed
                validate_llm_response(parsed, routed_type)

                # Render nicely
                reason_code = parsed.get("reason_code", "unknown")
                severity = parsed.get("severity", "info")
                emoji = "🚨" if severity == "critical" else "⚠️" if severity == "warning" else "ℹ️"

                parts = []
                parts.append(f"{emoji} [{severity.upper()}] {reason_code}")

                if parsed.get("summary_1line"):
                    parts.append(TelegramMessageAnalyzer._clean_llm_text(parsed["summary_1line"]))

                # Single most urgent action only — no lists of facts/risks/assumptions
                action = parsed.get("operator_action", {})
                if action.get("needed"):
                    urgency = action.get("urgency", "low").upper()
                    owner = action.get("owner", "SRE").upper()
                    steps = action.get("steps_now", [])
                    first_step = steps[0] if steps else action.get("steps_later", [""])[0]
                    if first_step:
                        first_step = TelegramMessageAnalyzer._clean_llm_text(first_step)
                        parts.append(f"Действие ({urgency} → {owner}): {first_step}")

                final_text = " ".join(parts)

            except json.JSONDecodeError as e:
                logger.warning(f"Telegram LLM analysis: failed to decode JSON: {e} — falling back to raw LLM output")
                return response_text

            elapsed = time.monotonic() - t0
            logger.info(f"Telegram analysis generated in {elapsed:.2f}s using {MODEL}")
            return final_text

        except Exception as e:
            logger.warning(f"Telegram LLM analysis failed: {e}")
            return None
