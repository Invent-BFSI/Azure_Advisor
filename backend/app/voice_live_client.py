from __future__ import annotations

import asyncio
import base64
import datetime as dt
import json
import logging
import os
import uuid
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Optional, Set

import websockets  # type: ignore[import]
from azure.identity import DefaultAzureCredential
from websockets import WebSocketClientProtocol  # type: ignore[import]

try:
    from websockets.protocol import State as WebSocketState  # type: ignore[import]
except ImportError:  # pragma: no cover - older websockets versions
    WebSocketState = None  # type: ignore[assignment]

from .audio_utils import float_frame_base64_to_pcm16_base64
from .tools import AVAILABLE_FUNCTIONS, TOOLS_LIST
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# Ensure .env from backend root is loaded when module is imported
load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=False)

SYSTEM_INSTRUCTIONS = """
You are Aria, a warm and professional AI investment advisor assistant. You ONLY discuss investment and personal finance.
You ALWAYS respond in English. You NEVER discuss unrelated topics.

## STRICT CONVERSATION RULE — MOST IMPORTANT:
Ask EXACTLY ONE question per turn. Never combine two questions in one message.
Acknowledge the user's previous answer briefly (one sentence), then ask the next single question.
Do NOT list upcoming questions. Do NOT number questions. Keep replies to 2-3 sentences max.
Use the user's first name occasionally — roughly once every 3-4 turns, not more.

## CONVERSATION FLOW — Follow this exact sequence:

### PRE-CONVERSATION — Disclaimer
Start EVERY new session with this exact message:
"Hey! Before we get started, just a quick heads-up — I'm an AI, not a licensed financial advisor. Everything I share is meant to be educational and help you think things through, but it doesn't replace advice from a real professional. Ready to get started?"

- YES / Sure / Ready → Give Introduction, then proceed to Phase 0.
- Asks what it means → Briefly clarify, then ask to continue.
- NO → "No problem at all. If you ever change your mind, I'll be right here." End gracefully.

Introduction (say once after disclaimer accepted):
"Great! I'm Aria, your investment advisor assistant. My goal is to help you build a clear, personalized picture of your finances. I'll ask a few questions — no right or wrong answers, just honest ones. Let's dig in."

---

### PHASE 0 — Identity & Context

Q01 — Full Name:
"To get us started, what's your full name?"
LOGIC: Record full name. Extract and use first name going forward.

Q02 — Age:
"And how old are you right now?"
LOGIC: Record age.

Q03 — Country / Region:
"Which country or region are you currently living in?"
LOGIC: Silently resolve canonical country, ISO currency code, and symbol (e.g. India → INR → ₹). Apply symbol in all future monetary references. Do NOT ask the user about currency. If ambiguous, ask one clarifying follow-up.

---

### PHASE 1 — Knowledge Calibration

Q04 — Knowledge Level:
"Quick one before we get into the details — how would you describe your current knowledge of investing? Are you pretty new to all of this, or do you have some experience with things like stocks, bonds, or index funds?"

- Beginner / New → Tag Beginner. Define all terms. Use analogies. Avoid acronyms.
- Some experience → Tag Intermediate. Use standard terms; confirm understanding when needed.
- Experienced / Advanced → Tag Advanced. Use precise language freely. Skip basics.

LOGIC: This tag governs all explanations for the rest of the session.

---

### PHASE 2 — Life Situation & Capacity

Q05 — Dependents:
"Do you have anyone who depends on your income financially — like children, a partner who doesn't work, or aging parents?"

- No → Proceed to Q07. Note higher risk flexibility.
- Yes → Proceed to Q06.

Q06 — Life Insurance (CONDITIONAL: only if Q05 = Yes):
"Since others are counting on your income, it's worth asking — do you have life insurance coverage that would replace your income if something happened to you?"

- Yes, adequate → Note and proceed.
- No / employer-only / unsure → Acknowledge: "That's worth looking into — it's usually the first thing to sort out before building a portfolio, since it protects everything else." Flag HIGH PRIORITY. Proceed.

---

### PHASE 3 — Financial Foundations

Q07 — High-Interest Debt:
"Alright, let's look at the foundations. Do you currently carry any high-interest debt — like credit card balances or payday loans?"

- No → Record debt-free. Proceed to Q09.
- Yes → Proceed to Q08.
- Unsure → Clarify: "A good rule of thumb — anything above roughly 8% interest is worth tackling before investing." Record response.

Q08 — Debt Details (CONDITIONAL: only if Q07 = Yes):
"Can you give me a rough sense of the total balance — even a ballpark — and the interest rate if you know it?"

Q09 — Emergency Fund Presence:
"Do you have money set aside in a separate, accessible account specifically for emergencies — like a job loss or an unexpected bill?"

- No → Note gap. Flag as foundational gap. Proceed to Phase 4.
- Yes → Proceed to Q10.

Q10 — Emergency Fund Size (CONDITIONAL: only if Q09 = Yes):
"Roughly how many months of living expenses would that fund cover?"
LOGIC: Target is 3-6 months. Below 3 = partial gap flag.

---

### PHASE 4 — Income & Budget

Q11 — Monthly Take-Home Income:
"Roughly what's your monthly take-home pay after taxes? A ballpark is totally fine."
LOGIC: Record as Monthly Income. Used to calculate Surplus and to personalise Q15 (10x and 6.5x figures).

Q12 — Monthly Essential Expenses:
"And about how much do you spend each month on the essentials — housing, utilities, groceries, transportation?"
LOGIC: Calculate Surplus = Monthly Income minus Monthly Expenses.

Q13 — Investable Monthly Amount:
"Based on what you've shared, it sounds like you have roughly [Surplus] left over each month. Does that feel about right? And of that, what portion would you feel comfortable putting toward investing?"
LOGIC: Use the actual Surplus figure with the correct currency symbol as an anchor.

---

### PHASE 5 — Financial Goals

Q14 — Primary Goals & Timelines:
"What's the main reason you want to start investing? Whether it's retirement, buying a home, a child's education, or building wealth — and roughly how many years away are those goals?"

- Single goal with timeline → Record and proceed.
- Multiple goals → Record all.
- Vague / no timeline → Follow up: "Do you have a rough timeframe in mind? Even 'within 10 years' or 'long-term' helps a lot."

---

### PHASE 6 — Risk Tolerance & Behavior

Q15 — Large Loss Scenario:
"Imagine [10x Monthly Income] is sitting in your investment account, and over the next year a market downturn causes it to drop to [6.5x Monthly Income]. What's your gut reaction in that moment?"
(Use actual calculated figures with the correct currency symbol.)

- Sell / Panic → Emotional Risk = low
- Hold / Wait → Emotional Risk = moderate
- Buy more / Great opportunity → Emotional Risk = high

Q16 — Recovery Time / Financial Capacity:
"And if it took 3 to 5 years for that portfolio to recover — would that cause any real financial hardship, or would it mostly just be emotionally uncomfortable?"

- Real hardship → Financial Capacity = low
- Uncomfortable but manageable → Financial Capacity = moderate
- Fine / no real impact → Financial Capacity = high

LOGIC: Derive risk_appetite from Q15 x Q16:
- Low Emotional + Low Capacity → conservative
- Low Emotional + High Capacity → moderate
- High Emotional + Low Capacity → moderate
- High Emotional + High Capacity → aggressive
- All moderate combos → moderate

---

### PHASE 7 — Investment Preferences

Q17 — Asset Class Interests / Exclusions:
"Are there any specific types of investments you're particularly interested in — like real estate, gold, or crypto — or anything you'd want to avoid entirely?"

- Interest → record as asset_interests
- Exclusion → record as avoid_asset_classes
- No preference → record as none

Q18 — Involvement Preference:
"Last one — once your finances are set up, would you prefer something fully automated that runs in the background, or do you like reviewing things and making decisions yourself? Or somewhere in between?"

- Fully automated → hands-off
- Somewhere in between → occasional
- Active / want control → active or diy

---

### WRAP-UP — Profile Summary & Save

After Q18, say:
"That's everything — you've been really thoughtful with your answers. Let me put together a summary of your financial profile."

Then call saveUserProfile with ALL collected fields. The profile_summary field should contain a warm, conversational 4-6 sentence summary covering:
1. Who they are and their situation (name, age, country, dependents)
2. Their financial foundations (emergency fund, debt situation)
3. Their goals and investment horizon
4. Their risk profile and involvement preference
5. Any important flags or priorities

Do NOT mention or suggest any specific investments, funds, ETFs, asset classes, percentages, or portfolio structures.

When the tool responds successfully, read back the profile_summary warmly to the user and end with:
"I've saved your profile. If you ever want to revisit or update any of this, I'm here."

## Tone: Warm, concise, knowledge-calibrated. Always English only.

## LANGUAGE RULE — CRITICAL:
You MUST respond ONLY in English, regardless of what language the user speaks in.
If the user speaks Spanish, French, Hindi, or any other language, still reply in English only.
Never switch languages. Never greet or respond in the user's language. English only, always.
"""


class VoiceLiveSession:
    """Manage a single Voice Live realtime session and broadcast events to subscribers."""

    def __init__(self, session_id: str):
        self.session_id = session_id
        self.ws: Optional[WebSocketClientProtocol] = None
        self._listeners: Set[asyncio.Queue] = set()
        self._lock = asyncio.Lock()
        self._receive_task: Optional[asyncio.Task] = None
        self._avatar_future: Optional[asyncio.Future] = None
        self._connected_event = asyncio.Event()

        endpoint = os.getenv("AZURE_VOICE_LIVE_ENDPOINT")
        model = os.getenv("VOICE_LIVE_MODEL")
        if not endpoint or not model:
            raise RuntimeError("AZURE_VOICE_LIVE_ENDPOINT and VOICE_LIVE_MODEL must be set")
        self._endpoint = endpoint
        self._model = model
        self._api_version = os.getenv("AZURE_VOICE_LIVE_API_VERSION", "2025-05-01-preview")
        self._api_key = os.getenv("AZURE_OPENAI_API_KEY")
        self._use_api_key = bool(self._api_key)

        self._session_config = {
            "modalities": ["text", "audio", "avatar", "animation"],
            "input_audio_sampling_rate": 24000,
            "instructions": SYSTEM_INSTRUCTIONS,
            "turn_detection": {
                "type": "server_vad",
                "threshold": 0.5,
                "prefix_padding_ms": 300,
                "silence_duration_ms": 500,
            },
            "tools": TOOLS_LIST,
            "tool_choice": "auto",
            "input_audio_noise_reduction": {"type": "azure_deep_noise_suppression"},
            "input_audio_echo_cancellation": {"type": "server_echo_cancellation"},
            "voice": {
                "name": os.getenv("AZURE_TTS_VOICE", "en-IN-AartiIndicNeural"),
                "type": "azure-standard",
                "temperature": 0.8,
            },
            "input_audio_transcription": {"model": "whisper-1"},
            "avatar": self._build_avatar_config(),
            "animation": {"model_name": "default", "outputs": ["blendshapes", "viseme_id"]},
        }
        self._response_config = {
            "modalities": ["text", "audio"],
        }

    def _ws_is_open(self) -> bool:
        ws = self.ws
        if ws is None:
            return False
        state = getattr(ws, "state", None)
        if state is not None:
            if WebSocketState is not None:
                try:
                    if state == WebSocketState.OPEN:
                        return True
                    if state in {WebSocketState.CLOSING, WebSocketState.CLOSED}:
                        return False
                except TypeError:
                    pass
            state_name = getattr(state, "name", None)
            if isinstance(state_name, str):
                if state_name.upper() == "OPEN":
                    return True
                if state_name.upper() in {"CLOSING", "CLOSED"}:
                    return False
        open_attr = getattr(ws, "open", None)
        if isinstance(open_attr, bool):
            return open_attr
        if callable(open_attr):
            try:
                return bool(open_attr())
            except TypeError:
                pass
        closed_attr = getattr(ws, "closed", None)
        if isinstance(closed_attr, bool):
            return not closed_attr
        if callable(closed_attr):
            try:
                return not bool(closed_attr())
            except TypeError:
                pass
        close_code = getattr(ws, "close_code", None)
        return close_code is None

    async def _ensure_connection(self) -> None:
        if self._ws_is_open():
            return
        await self.connect()
        if not self._ws_is_open():
            raise RuntimeError("Session websocket is not connected")
            return False
        open_attr = getattr(self.ws, "open", None)
        if isinstance(open_attr, bool):
            return open_attr
        closed_attr = getattr(self.ws, "closed", None)
        if isinstance(closed_attr, bool):
            return not closed_attr
        if callable(closed_attr):  # type: ignore[call-overload]
            try:
                return not closed_attr()
            except TypeError:
                pass
        close_code = getattr(self.ws, "close_code", None)
        return close_code is None

    def _build_avatar_config(self) -> Dict[str, Any]:
        character = os.getenv("AZURE_VOICE_AVATAR_CHARACTER", "lisa")
        style = os.getenv("AZURE_VOICE_AVATAR_STYLE")
        video_width = int(os.getenv("AZURE_VOICE_AVATAR_WIDTH", "1280"))
        video_height = int(os.getenv("AZURE_VOICE_AVATAR_HEIGHT", "720"))
        bitrate = int(os.getenv("AZURE_VOICE_AVATAR_BITRATE", "2000000"))
        config: Dict[str, Any] = {
            "character": character,
            "customized": False,
            "video": {"resolution": {"width": video_width, "height": video_height}, "bitrate": bitrate},
        }
        if style:
            config["style"] = style
        ice_urls = os.getenv("AZURE_VOICE_AVATAR_ICE_URLS")
        if ice_urls:
            config["ice_servers"] = [
                {"urls": [url.strip() for url in ice_urls.split(",") if url.strip()]}
            ]
        return config

    async def connect(self) -> None:
        async with self._lock:
            if self._ws_is_open():
                return
            headers = {"x-ms-client-request-id": str(uuid.uuid4())}
            if self._use_api_key:
                ws_url = self._build_ws_url()
                headers["api-key"] = self._api_key  # Azure OpenAI key
            else:
                token = await self._get_token()
                ws_url = self._build_ws_url(token)
                headers["Authorization"] = f"Bearer {token}"
            self.ws = await websockets.connect(ws_url, additional_headers=headers)
            logger.info("[%s] Connected to Azure Voice Live", self.session_id)
            self._receive_task = asyncio.create_task(self._receive_loop())
            await self._send("session.update", {"session": self._session_config}, allow_reconnect=False)
            self._connected_event.set()

    async def disconnect(self) -> None:
        async with self._lock:
            if self._ws_is_open():
                await self.ws.close()
            if self._receive_task:
                self._receive_task.cancel()
            self.ws = None
            self._connected_event.clear()
            logger.info("[%s] Disconnected session", self.session_id)

    async def _get_token(self) -> str:
        credential = DefaultAzureCredential()
        scope = "https://ai.azure.com/.default"
        token = await asyncio.get_event_loop().run_in_executor(None, credential.get_token, scope)
        return token.token

    def _build_ws_url(self, agent_token: Optional[str] = None) -> str:
        azure_ws_endpoint = self._endpoint.rstrip("/").replace("https://", "wss://")
        base = f"{azure_ws_endpoint}/voice-live/realtime?api-version={self._api_version}&model={self._model}"
        if agent_token:
            return f"{base}&agent-access-token={agent_token}"
        return base

    async def _send(
        self,
        event_type: str,
        data: Optional[Dict[str, Any]] = None,
        *,
        allow_reconnect: bool = True,
    ) -> None:
        if not self._ws_is_open():
            if allow_reconnect:
                await self.connect()
            if not self._ws_is_open():
                raise RuntimeError("Session websocket is not connected")
        if not self.ws:
            raise RuntimeError("Session websocket is not connected")
        payload = {"event_id": self._generate_id("evt_"), "type": event_type}
        if data:
            payload.update(data)
        await self.ws.send(json.dumps(payload))

    @staticmethod
    def _generate_id(prefix: str) -> str:
        return f"{prefix}{int(dt.datetime.utcnow().timestamp() * 1000)}"

    @staticmethod
    def _encode_client_sdp(client_sdp: str) -> str:
        payload = json.dumps({"type": "offer", "sdp": client_sdp})
        return base64.b64encode(payload.encode("utf-8")).decode("ascii")

    @staticmethod
    def _decode_server_sdp(server_sdp_raw: Optional[str]) -> Optional[str]:
        if not server_sdp_raw:
            return None
        if server_sdp_raw.startswith("v=0"):
            return server_sdp_raw
        try:
            decoded_bytes = base64.b64decode(server_sdp_raw)
        except Exception:
            return server_sdp_raw
        try:
            decoded_text = decoded_bytes.decode("utf-8")
        except Exception:
            return server_sdp_raw
        try:
            payload = json.loads(decoded_text)
        except json.JSONDecodeError:
            return decoded_text
        if isinstance(payload, dict):
            sdp_value = payload.get("sdp")
            if isinstance(sdp_value, str) and sdp_value:
                return sdp_value
        return decoded_text

    def create_event_queue(self) -> asyncio.Queue:
        queue: asyncio.Queue = asyncio.Queue(maxsize=200)
        self._listeners.add(queue)
        return queue

    def remove_event_queue(self, queue: asyncio.Queue) -> None:
        self._listeners.discard(queue)

    async def _broadcast(self, event: Dict[str, Any]) -> None:
        if not self._listeners:
            return
        for queue in list(self._listeners):
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                logger.warning("[%s] Dropping event %s due to slow consumer", self.session_id, event.get("type"))

    async def send_user_message(self, text: str) -> None:
        await self._connected_event.wait()
        await self._ensure_connection()
        await self._send(
            "conversation.item.create",
            {
                "item": {
                    "type": "message",
                    "role": "user",
                    "content": [{"type": "input_text", "text": text}],
                }
            },
        )
        await self._send("response.create", {"response": self._response_config})

    async def send_audio_chunk(self, audio_b64: str, encoding: str = "float32") -> None:
        await self._connected_event.wait()
        await self._ensure_connection()
        if encoding == "float32":
            pcm_b64 = float_frame_base64_to_pcm16_base64(audio_b64)
        else:
            pcm_b64 = audio_b64
        await self._send("input_audio_buffer.append", {"audio": pcm_b64})

    async def commit_audio(self) -> None:
        await self._connected_event.wait()
        await self._ensure_connection()
        await self._send("input_audio_buffer.commit")

    async def clear_audio(self) -> None:
        await self._connected_event.wait()
        await self._ensure_connection()
        await self._send("input_audio_buffer.clear")

    async def request_response(self) -> None:
        await self._connected_event.wait()
        await self._ensure_connection()
        await self._send("response.create", {"response": self._response_config})

    async def connect_avatar(self, client_sdp: str) -> str:
        await self._connected_event.wait()
        await self._ensure_connection()
        future: asyncio.Future = asyncio.get_event_loop().create_future()
        self._avatar_future = future
        encoded_sdp = self._encode_client_sdp(client_sdp)
        payload = {
            "client_sdp": encoded_sdp,
            "rtc_configuration": {"bundle_policy": "max-bundle"},
        }
        logger.info("[%s] Sending session.avatar.connect ...", self.session_id)
        await self._send("session.avatar.connect", payload)
        try:
            server_sdp = await asyncio.wait_for(future, timeout=30)
            logger.info("[%s] Avatar SDP received successfully", self.session_id)
            return server_sdp
        finally:
            self._avatar_future = None

    async def _receive_loop(self) -> None:
        ws = self.ws
        if ws is None:
            return
        try:
            async for message in ws:
                try:
                    event = json.loads(message)
                except json.JSONDecodeError:
                    logger.warning("[%s] Failed to decode message", self.session_id)
                    continue
                event_type = event.get("type")
                logger.debug("[%s] Received event: %s", self.session_id, event_type)
                if event_type == "error":
                    logger.error("[%s] Error event: %s", self.session_id, event.get("error", event))
                    await self._broadcast({"type": "error", "payload": event})
                elif event_type == "response.audio.delta":
                    await self._broadcast({"type": "assistant_audio_delta", "delta": event.get("delta")})
                elif event_type == "response.audio.done":
                    await self._broadcast({"type": "assistant_audio_done", "payload": event})
                elif event_type == "response.audio_transcript.delta":
                    await self._broadcast(
                        {
                            "type": "assistant_transcript_delta",
                            "delta": event.get("delta"),
                            "item_id": event.get("item_id"),
                        }
                    )
                elif event_type == "response.audio_transcript.done":
                    await self._broadcast(
                        {
                            "type": "assistant_transcript_done",
                            "transcript": event.get("transcript"),
                            "item_id": event.get("item_id"),
                        }
                    )
                elif event_type == "conversation.item.input_audio_transcription.completed":
                    await self._broadcast(
                        {
                            "type": "user_transcript_completed",
                            "transcript": event.get("transcript"),
                            "item_id": event.get("item_id"),
                        }
                    )
                elif event_type == "input_audio_buffer.speech_started":
                    await self._broadcast({"type": "speech_started"})
                elif event_type == "input_audio_buffer.speech_stopped":
                    await self._broadcast({"type": "speech_stopped"})
                elif event_type == "input_audio_buffer.committed":
                    await self._broadcast({"type": "input_audio_committed"})
                elif event_type == "session.avatar.connecting":
                    server_sdp = event.get("server_sdp")
                    decoded_sdp = self._decode_server_sdp(server_sdp)
                    if self._avatar_future and not self._avatar_future.done():
                        if decoded_sdp is None:
                            self._avatar_future.set_exception(RuntimeError("Empty server SDP"))
                        else:
                            self._avatar_future.set_result(decoded_sdp)
                    await self._broadcast({"type": "avatar_connecting"})
                elif event_type == "response.done":
                    await self._handle_response_done(event)
                else:
                    await self._broadcast({"type": "event", "payload": event})
        except Exception as exc:  # pylint: disable=broad-except
            logger.exception("[%s] Azure Voice Live websocket receive loop ended with error", self.session_id)
            await self._broadcast({"type": "error", "payload": {"message": str(exc)}})
        finally:
            if self.ws is ws:
                self.ws = None
            logger.info("[%s] Azure Voice Live websocket closed", self.session_id)

    async def _handle_response_done(self, event: Dict[str, Any]) -> None:
        response = event.get("response", {})
        status = response.get("status")
        if status != "completed":
            await self._broadcast({"type": "response_status", "status": status})
            return
        output_items = response.get("output", [])
        if not output_items:
            return
        first_item = output_items[0]
        if first_item.get("type") != "function_call":
            return
        function_name = first_item.get("name")
        arguments = json.loads(first_item.get("arguments", "{}"))
        call_id = first_item.get("call_id")
        logger.info("[%s] Function call requested: %s", self.session_id, function_name)
        func = AVAILABLE_FUNCTIONS.get(function_name)
        if not func:
            logger.error("Function %s is not registered", function_name)
            return
        try:
            # Inject session_id for tools that need it (e.g. saveUserProfile)
            if function_name == "saveUserProfile":
                arguments["session_id"] = self.session_id
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, lambda: func(**arguments))
        except Exception as exc:  # pylint: disable=broad-except
            logger.exception("Function %s failed", function_name)
            result = json.dumps({"error": str(exc)})
        if not isinstance(result, str):
            result_payload = json.dumps(result)
        else:
            result_payload = result
        await self._send(
            "conversation.item.create",
            {
                "item": {
                    "type": "function_call_output",
                    "call_id": call_id,
                    "output": result_payload,
                }
            },
        )
        await self._send("response.create", {"response": self._response_config})
        await self._broadcast({"type": "function_call_completed", "name": function_name})
