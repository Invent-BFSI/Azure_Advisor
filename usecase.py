"""
Investment Advisor Chatbot — Aria (ConversationMap Edition)
===========================================================
FastAPI + WebSockets backend using AWS Bedrock Nova-2-Sonic.
Follows ConversationMap_final.md (18-question flow) exactly.
After all questions are answered, gives a profile summary and saves to CSV.
No portfolio or asset allocation logic.
"""

import asyncio
import base64
import csv
import datetime
import json
import logging
import os
import uuid
import warnings

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

import uvicorn
from aws_sdk_bedrock_runtime.client import (
    BedrockRuntimeClient,
    InvokeModelWithBidirectionalStreamOperationInput,
)
from aws_sdk_bedrock_runtime.config import Config
from aws_sdk_bedrock_runtime.models import (
    BidirectionalInputPayloadPart,
    InvokeModelWithBidirectionalStreamInputChunk,
)
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from smithy_aws_core.identity.environment import EnvironmentCredentialsResolver

warnings.filterwarnings("ignore")

CSV_FILE = "customer_details.csv"

# ═══════════════════════════════════════════════════════════════
#  CSV COLUMNS
# ═══════════════════════════════════════════════════════════════

CSV_COLUMNS = [
    "session_id",
    "timestamp",
    # Phase 0 — Identity
    "full_name",
    "age",
    "region_stated",
    "canonical_country",
    "currency_code",
    "currency_symbol",
    # Phase 1 — Knowledge
    "knowledge_level",
    # Phase 2 — Life Situation
    "has_dependents",
    "has_life_insurance",
    # Phase 3 — Financial Foundations
    "high_interest_debt",
    "debt_balance",
    "debt_rate_pct",
    "has_emergency_fund",
    "emergency_fund_months",
    # Phase 4 — Income & Budget
    "monthly_inflow",
    "monthly_outflow",
    "monthly_surplus",
    "investment_amount",
    # Phase 5 — Goals
    "investment_goals",
    "investment_period_years",
    # Phase 6 — Risk
    "risk_tolerance_emotional",
    "risk_capacity_financial",
    "risk_appetite",
    # Phase 7 — Preferences
    "asset_interests",
    "avoid_asset_classes",
    "involvement_level",
    # Summary
    "flags",
    "profile_summary",
]

# ═══════════════════════════════════════════════════════════════
#  SYSTEM PROMPT  (ConversationMap_final.md — 18 questions)
# ═══════════════════════════════════════════════════════════════

SYSTEM_PROMPT = """
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

# ═══════════════════════════════════════════════════════════════
#  TOOL SCHEMA
# ═══════════════════════════════════════════════════════════════

_SAVE_PROFILE_SCHEMA = json.dumps(
    {
        "type": "object",
        "properties": {
            "full_name": {"type": "string"},
            "age": {"type": "number"},
            "region_stated": {"type": "string"},
            "canonical_country": {"type": "string"},
            "currency_code": {"type": "string"},
            "currency_symbol": {"type": "string"},
            "knowledge_level": {
                "type": "string",
                "enum": ["beginner", "intermediate", "advanced"],
            },
            "has_dependents": {"type": "boolean"},
            "has_life_insurance": {"type": "boolean"},
            "high_interest_debt": {"type": "boolean"},
            "debt_balance": {"type": "number"},
            "debt_rate_pct": {"type": "number"},
            "has_emergency_fund": {"type": "boolean"},
            "emergency_fund_months": {"type": "number"},
            "monthly_inflow": {"type": "number"},
            "monthly_outflow": {"type": "number"},
            "investment_amount": {"type": "number"},
            "investment_goals": {"type": "array", "items": {"type": "string"}},
            "investment_period_years": {"type": "number"},
            "risk_tolerance_emotional": {
                "type": "string",
                "enum": ["low", "moderate", "high"],
            },
            "risk_capacity_financial": {
                "type": "string",
                "enum": ["low", "moderate", "high"],
            },
            "risk_appetite": {
                "type": "string",
                "enum": ["conservative", "moderate", "aggressive"],
            },
            "asset_interests": {"type": "array", "items": {"type": "string"}},
            "avoid_asset_classes": {"type": "array", "items": {"type": "string"}},
            "involvement_level": {
                "type": "string",
                "enum": ["hands-off", "occasional", "active", "diy"],
            },
            "profile_summary": {
                "type": "string",
                "description": "The full conversational profile summary Aria will read back to the user.",
            },
        },
        "required": [
            "full_name",
            "age",
            "canonical_country",
            "currency_code",
            "currency_symbol",
            "monthly_inflow",
            "monthly_outflow",
            "investment_amount",
            "investment_period_years",
            "risk_appetite",
            "investment_goals",
            "profile_summary",
        ],
    }
)


# ═══════════════════════════════════════════════════════════════
#  BUSINESS LOGIC — flags + CSV save
# ═══════════════════════════════════════════════════════════════


def _build_flags(profile: dict) -> list:
    flags = []

    if profile.get("high_interest_debt"):
        rate = profile.get("debt_rate_pct", 0)
        balance = profile.get("debt_balance", 0)
        sym = profile.get("currency_symbol", "$")
        rate_str = f" at ~{rate:.0f}%" if rate else ""
        bal_str = f" ({sym}{balance:,.0f})" if balance else ""
        flags.append(f"HIGH_INTEREST_DEBT{bal_str}{rate_str}")

    has_ef = profile.get("has_emergency_fund", False)
    ef_months = float(profile.get("emergency_fund_months", 0))
    if not has_ef:
        flags.append("NO_EMERGENCY_FUND")
    elif ef_months < 3:
        flags.append(f"LOW_EMERGENCY_FUND ({ef_months:.0f} months)")

    if profile.get("has_dependents") and not profile.get("has_life_insurance"):
        flags.append("NO_LIFE_INSURANCE + DEPENDENTS")

    return flags


def save_to_csv(session_id: str, profile: dict):
    flags = _build_flags(profile)
    inflow = float(profile.get("monthly_inflow", 0))
    outflow = float(profile.get("monthly_outflow", 0))

    row = {
        "session_id": session_id,
        "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "full_name": profile.get("full_name", ""),
        "age": profile.get("age", ""),
        "region_stated": profile.get("region_stated", ""),
        "canonical_country": profile.get("canonical_country", ""),
        "currency_code": profile.get("currency_code", ""),
        "currency_symbol": profile.get("currency_symbol", ""),
        "knowledge_level": profile.get("knowledge_level", ""),
        "has_dependents": profile.get("has_dependents", False),
        "has_life_insurance": profile.get("has_life_insurance", False),
        "high_interest_debt": profile.get("high_interest_debt", False),
        "debt_balance": profile.get("debt_balance", 0),
        "debt_rate_pct": profile.get("debt_rate_pct", 0),
        "has_emergency_fund": profile.get("has_emergency_fund", False),
        "emergency_fund_months": profile.get("emergency_fund_months", 0),
        "monthly_inflow": inflow,
        "monthly_outflow": outflow,
        "monthly_surplus": round(inflow - outflow, 2),
        "investment_amount": profile.get("investment_amount", 0),
        "investment_goals": "; ".join(profile.get("investment_goals", [])),
        "investment_period_years": profile.get("investment_period_years", ""),
        "risk_tolerance_emotional": profile.get("risk_tolerance_emotional", ""),
        "risk_capacity_financial": profile.get("risk_capacity_financial", ""),
        "risk_appetite": profile.get("risk_appetite", ""),
        "asset_interests": "; ".join(profile.get("asset_interests", [])),
        "avoid_asset_classes": "; ".join(profile.get("avoid_asset_classes", [])),
        "involvement_level": profile.get("involvement_level", ""),
        "flags": " | ".join(flags) if flags else "",
        "profile_summary": profile.get("profile_summary", ""),
    }

    file_exists = os.path.isfile(CSV_FILE)
    with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)
    logger.info(f"Profile saved to CSV for session {session_id}.")


# ═══════════════════════════════════════════════════════════════
#  TOOL PROCESSOR
# ═══════════════════════════════════════════════════════════════


class ToolProcessor:
    def __init__(self, session_id: str):
        self.session_id = session_id
        self.tasks = {}

    async def process_tool_async(self, tool_name: str, tool_content: dict):
        task_id = str(uuid.uuid4())
        task = asyncio.create_task(self._run_tool(tool_name, tool_content))
        self.tasks[task_id] = task
        try:
            return await task
        finally:
            self.tasks.pop(task_id, None)

    async def _run_tool(self, tool_name: str, tool_content: dict):
        logger.info(f"Running tool: {tool_name}")

        raw = tool_content.get("content", "{}")
        try:
            payload = json.loads(raw) if isinstance(raw, str) else (raw or {})
        except json.JSONDecodeError as e:
            logger.error(f"JSONDecodeError in tool {tool_name}: {e}")
            payload = {}

        if tool_name.lower() == "saveuserprofile":
            try:
                required = [
                    "full_name",
                    "monthly_inflow",
                    "monthly_outflow",
                    "investment_amount",
                    "investment_period_years",
                    "risk_appetite",
                    "investment_goals",
                    "profile_summary",
                ]
                missing = [k for k in required if not payload.get(k)]
                if missing:
                    err = f"Missing required fields: {missing}. Please collect all answers first."
                    logger.error(err)
                    return {"error": err}

                save_to_csv(self.session_id, payload)
                logger.info(f"Profile saved successfully for session {self.session_id}")
                return {
                    "status": "saved",
                    "profile_summary": payload.get("profile_summary", ""),
                }

            except Exception as e:
                import traceback

                logger.error(f"Save profile failed: {e}\n{traceback.format_exc()}")
                return {"error": f"Save failed: {e}"}

        logger.warning(f"Unknown tool: {tool_name}")
        return {"error": f"Unknown tool: {tool_name}"}


# ═══════════════════════════════════════════════════════════════
#  BEDROCK STREAM MANAGER
# ═══════════════════════════════════════════════════════════════


class BedrockStreamManager:

    START_SESSION_EVENT = json.dumps(
        {
            "event": {
                "sessionStart": {
                    "inferenceConfiguration": {
                        "maxTokens": 1024,
                        "topP": 0.9,
                        "temperature": 0.7,
                    }
                }
            }
        }
    )

    CONTENT_START_EVENT = """{
        "event": {
            "contentStart": {
                "promptName": "%s", "contentName": "%s",
                "type": "AUDIO", "interactive": true, "role": "USER",
                "audioInputConfiguration": {
                    "mediaType": "audio/lpcm", "sampleRateHertz": 16000,
                    "sampleSizeBits": 16, "channelCount": 1,
                    "audioType": "SPEECH", "encoding": "base64",
                    "languageCode": "en-US"
                }
            }
        }
    }"""

    AUDIO_EVENT_TEMPLATE = """{
        "event": {
            "audioInput": {
                "promptName": "%s", "contentName": "%s", "content": "%s"
            }
        }
    }"""

    TEXT_CONTENT_START_EVENT = """{
        "event": {
            "contentStart": {
                "promptName": "%s", "contentName": "%s",
                "type": "TEXT", "role": "%s", "interactive": false,
                "textInputConfiguration": {"mediaType": "text/plain"}
            }
        }
    }"""

    TEXT_INPUT_EVENT = """{
        "event": {
            "textInput": {
                "promptName": "%s", "contentName": "%s", "content": "%s"
            }
        }
    }"""

    TOOL_CONTENT_START_EVENT = """{
        "event": {
            "contentStart": {
                "promptName": "%s", "contentName": "%s",
                "interactive": false, "type": "TOOL", "role": "TOOL",
                "toolResultInputConfiguration": {
                    "toolUseId": "%s", "type": "TEXT",
                    "textInputConfiguration": {"mediaType": "text/plain"}
                }
            }
        }
    }"""

    CONTENT_END_EVENT = """{
        "event": {
            "contentEnd": {"promptName": "%s", "contentName": "%s"}
        }
    }"""

    PROMPT_END_EVENT = """{
        "event": {"promptEnd": {"promptName": "%s"}}
    }"""

    SESSION_END_EVENT = '{"event": {"sessionEnd": {}}}'

    def start_prompt(self):
        return json.dumps(
            {
                "event": {
                    "promptStart": {
                        "promptName": self.prompt_name,
                        "textOutputConfiguration": {"mediaType": "text/plain"},
                        "audioOutputConfiguration": {
                            "mediaType": "audio/lpcm",
                            "sampleRateHertz": 24000,
                            "sampleSizeBits": 16,
                            "channelCount": 1,
                            "voiceId": "amy",
                            "encoding": "base64",
                            "audioType": "SPEECH",
                        },
                        "toolUseOutputConfiguration": {"mediaType": "application/json"},
                        "toolConfiguration": {
                            "tools": [
                                {
                                    "toolSpec": {
                                        "name": "saveUserProfile",
                                        "description": (
                                            "Saves the completed user profile to the database after all 18 questions "
                                            "have been answered. YOU must populate canonical_country, currency_code, "
                                            "and currency_symbol from the user's stated region — do NOT ask the user. "
                                            "YOU must derive risk_appetite from the Q15/Q16 two-dimensional matrix. "
                                            "The profile_summary field must contain the full 4-6 sentence conversational "
                                            "summary you will read back to the user. Call ONLY after ALL questions are done."
                                        ),
                                        "inputSchema": {"json": _SAVE_PROFILE_SCHEMA},
                                    }
                                }
                            ]
                        },
                    }
                }
            }
        )

    def __init__(
        self,
        ws_queue: asyncio.Queue,
        model_id="amazon.nova-2-sonic-v1:0",
        region="us-east-1",
    ):
        self.model_id = model_id
        self.region = region
        self.session_id = str(uuid.uuid4())
        self.prompt_name = str(uuid.uuid4())
        self.content_name = str(uuid.uuid4())
        self.ws_queue = ws_queue

        self.is_active = False
        self.toolName = None
        self.toolUseId = None
        self.toolUseContent = None

        self.pending_tool_tasks = {}
        self.tool_processor = ToolProcessor(self.session_id)
        self._audio_chunk_queue = asyncio.Queue()

        self.stream_response = None
        self.response_task = None
        self.send_task = None

    async def initialize_stream(self):
        import traceback as tb

        logger.info(f"Initializing Bedrock stream for session {self.session_id}")
        try:
            config = Config(
                endpoint_uri=f"https://bedrock-runtime.{self.region}.amazonaws.com",
                aws_credentials_identity_resolver=EnvironmentCredentialsResolver(),
                region=self.region,
            )
            self.client = BedrockRuntimeClient(config=config)
            self.stream_response = (
                await self.client.invoke_model_with_bidirectional_stream(
                    InvokeModelWithBidirectionalStreamOperationInput(
                        model_id=self.model_id
                    )
                )
            )
        except Exception:
            logger.error(
                f"Failed to open Bedrock bidirectional stream:\n{tb.format_exc()}"
            )
            raise

        self.is_active = True
        self.send_task = asyncio.create_task(self._send_events_loop())

        await self.send_raw_event(self.START_SESSION_EVENT)
        await self.send_raw_event(self.start_prompt())

        sys_cn = str(uuid.uuid4())
        await self.send_raw_event(
            self.TEXT_CONTENT_START_EVENT % (self.prompt_name, sys_cn, "SYSTEM")
        )
        await self.send_raw_event(
            self.TEXT_INPUT_EVENT
            % (self.prompt_name, sys_cn, json.dumps(SYSTEM_PROMPT)[1:-1])
        )
        await self.send_raw_event(self.CONTENT_END_EVENT % (self.prompt_name, sys_cn))

        self.content_name = str(uuid.uuid4())
        await self.send_raw_event(
            self.CONTENT_START_EVENT % (self.prompt_name, self.content_name)
        )

        self.response_task = asyncio.create_task(self._process_responses())
        logger.info("Bedrock stream initialized successfully.")

    async def _send_events_loop(self):
        import traceback as tb

        while self.is_active:
            try:
                event_str = await asyncio.wait_for(
                    self._audio_chunk_queue.get(), timeout=0.1
                )
                chunk = InvokeModelWithBidirectionalStreamInputChunk(
                    value=BidirectionalInputPayloadPart(
                        bytes_=event_str.encode("utf-8")
                    )
                )
                await self.stream_response.input_stream.send(chunk)
            except asyncio.TimeoutError:
                continue
            except Exception:
                logger.error(f"Send events loop error:\n{tb.format_exc()}")
                break
        logger.info("Send events loop exited.")

    async def send_raw_event(self, event_str: str):
        if self.is_active:
            await self._audio_chunk_queue.put(event_str)
        else:
            logger.warning("Attempted to send event on inactive stream.")

    def add_audio_chunk(self, audio_bytes: bytes):
        # Called from the async websocket handler — put directly, no cross-thread call
        if not self.is_active:
            return
        encoded = base64.b64encode(audio_bytes).decode("utf-8")
        event = self.AUDIO_EVENT_TEMPLATE % (
            self.prompt_name,
            self.content_name,
            encoded,
        )
        try:
            self._audio_chunk_queue.put_nowait(event)
        except asyncio.QueueFull:
            logger.warning("Audio chunk queue full — dropping chunk.")

    async def _process_responses(self):
        import traceback as tb

        try:
            while self.is_active:
                output = await self.stream_response.await_output()
                result = await output[1].receive()
                if result.value and result.value.bytes_:
                    json_data = json.loads(result.value.bytes_.decode("utf-8"))
                    logger.debug(f"Received event from Bedrock: {json_data}")
                    await self._handle_event(json_data)
        except Exception:
            logger.error(f"Bedrock response processing error:\n{tb.format_exc()}")
        finally:
            logger.info("Response processing loop exited — marking stream inactive.")
            self.is_active = False

    async def _handle_event(self, json_data: dict):
        event = json_data.get("event", {})

        if "textOutput" in event:
            text = event["textOutput"]["content"]
            role = event["textOutput"].get("role", "ASSISTANT")
            logger.info(f"Received text from Bedrock [{role}]: {text}")
            if '{ "interrupted" : true }' in text:
                await self.ws_queue.put({"type": "interrupted"})
            else:
                await self.ws_queue.put({"type": "text", "text": text, "role": role})

        elif "audioOutput" in event:
            await self.ws_queue.put(
                {"type": "audio", "data": event["audioOutput"]["content"]}
            )

        elif "contentEnd" in event:
            if event.get("contentEnd", {}).get("type") == "TOOL":
                logger.info(f"Tool request ended for: {self.toolName}")
                self.handle_tool_request(
                    self.toolName, self.toolUseContent, self.toolUseId
                )
            else:
                await self.ws_queue.put({"type": "audio_end"})

        elif "toolUse" in event:
            self.toolUseContent = event["toolUse"]
            self.toolName = event["toolUse"]["toolName"]
            self.toolUseId = event["toolUse"]["toolUseId"]
            logger.info(f"Received tool use: {self.toolName} id: {self.toolUseId}")

    def handle_tool_request(self, tool_name, tool_content, tool_use_id):
        cn = str(uuid.uuid4())
        task = asyncio.create_task(
            self._execute_tool_and_send_result(tool_name, tool_content, tool_use_id, cn)
        )
        self.pending_tool_tasks[cn] = task

    async def _execute_tool_and_send_result(
        self, tool_name, tool_content, tool_use_id, cn
    ):
        try:
            result = await self.tool_processor.process_tool_async(
                tool_name, tool_content
            )
            await self.send_raw_event(
                self.TOOL_CONTENT_START_EVENT % (self.prompt_name, cn, tool_use_id)
            )

            result_json = json.dumps(result)
            tool_res_event = json.dumps(
                {
                    "event": {
                        "toolResult": {
                            "promptName": self.prompt_name,
                            "contentName": cn,
                            "content": result_json,
                        }
                    }
                }
            )
            await self.send_raw_event(tool_res_event)
            await self.send_raw_event(self.CONTENT_END_EVENT % (self.prompt_name, cn))
            logger.info(f"Tool result sent for {tool_name}")
        except Exception as e:
            logger.error(f"Failed to send tool result for {tool_name}: {e}")

    async def close(self):
        logger.info(f"Closing Bedrock stream for session {self.session_id}")
        if not self.is_active:
            return
        self.is_active = False
        await self.send_raw_event(
            self.CONTENT_END_EVENT % (self.prompt_name, self.content_name)
        )
        await self.send_raw_event(self.PROMPT_END_EVENT % self.prompt_name)
        await self.send_raw_event(self.SESSION_END_EVENT)

        await asyncio.sleep(0.5)
        if self.send_task:
            self.send_task.cancel()
            try:
                await self.send_task
            except asyncio.CancelledError:
                pass
        logger.info("Bedrock stream closed.")


# ═══════════════════════════════════════════════════════════════
#  FASTAPI APP & ROUTING
# ═══════════════════════════════════════════════════════════════

app = FastAPI()


@app.on_event("startup")
async def startup_event():
    logger.info("Aria starting up...")


@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Application shutting down...")


app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
async def root():
    return FileResponse("static/index.html")


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    logger.info("WebSocket connection accepted.")
    ws_queue = asyncio.Queue()
    manager = BedrockStreamManager(ws_queue=ws_queue)

    try:
        await manager.initialize_stream()
    except Exception as e:
        logger.error(f"WebSocket initialization error: {e}")
        await websocket.send_json(
            {"type": "system", "text": f"Initialization error: {e}"}
        )
        await websocket.close()
        return

    async def sender_task():
        # Use a timeout so the loop re-checks is_active even when the queue is idle
        while True:
            try:
                msg = await asyncio.wait_for(ws_queue.get(), timeout=0.5)
                if msg["type"] == "audio":
                    await websocket.send_bytes(base64.b64decode(msg["data"]))
                else:
                    await websocket.send_json(msg)
            except asyncio.TimeoutError:
                # No message — check if stream died
                if not manager.is_active:
                    break
                continue
            except Exception:
                import traceback as tb

                logger.error(f"Sender task error:\n{tb.format_exc()}")
                break
        logger.info("Sender task exited.")

    send_task = asyncio.create_task(sender_task())

    try:
        while True:
            data = await websocket.receive()
            if "bytes" in data:
                manager.add_audio_chunk(data["bytes"])
            elif "text" in data:
                try:
                    msg = json.loads(data["text"])
                    if msg.get("type") == "ping":
                        pass  # Silently ignore keepalive pings
                    else:
                        logger.info(f"Received text from client: {data['text']}")
                except json.JSONDecodeError:
                    logger.info(f"Received non-JSON text: {data['text']}")
    except WebSocketDisconnect:
        logger.info("WebSocket client disconnected.")
    except Exception as e:
        logger.error(f"WebSocket listen loop error: {e}")
    finally:
        logger.info("Closing WebSocket connection.")
        await manager.close()
        send_task.cancel()


if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)