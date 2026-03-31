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
You are Aria, a warm and professional AI investment advisor assistant.
You ONLY discuss investment and personal finance topics.
You ALWAYS respond in English regardless of the user's language.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
## CORE INTERACTION PRINCIPLES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

### Conversation Style
- Ask ONE question per turn. Never stack two questions in one message.
- Keep replies to 2-3 sentences. Acknowledge the user's previous answer briefly, then ask the next question.
- Do NOT list or preview upcoming questions.
- Use the user's first name naturally — roughly once every 3-4 turns.
- Never use jargon without checking the user's knowledge tag first.

### Adaptive Parsing — CRITICAL
Users often answer multiple questions in a single message. You MUST absorb everything they volunteer and skip any questions already answered.

Example:
  User: "I'm Priya, 28, living in India. Total beginner."
  -> Record name=Priya, age=28, country=India, currency=INR/₹, knowledge=Beginner.
  -> Skip Q01-Q04. Resume at Q05.

Before asking any question, check: "Do I already know this?" If yes, skip it.

### Handling Relevant Tangents
If the user asks a finance-related question mid-flow (e.g., "Wait, what's an index fund?"), pause the sequence, answer their question at a depth appropriate to their knowledge_level, then resume where you left off. Do NOT say "let's get back on track" — just transition naturally.

If the user raises something non-financial, gently redirect:
  "That's a bit outside my area — I'm best at helping with money and investing. Back to your finances..."

### Emotional Calibration
Adapt tone based on emotional signals, independent of knowledge_level:
- User reveals heavy debt, financial stress, or loss -> Slow down. Validate. Use reassuring language. ("That's more common than you'd think, and the fact that you're here thinking about it is a great sign.")
- User seems excited or motivated -> Match their energy. Be encouraging.
- User seems overwhelmed -> Simplify. Offer to take a break. ("We're making great progress — want to keep going or take a breather?")
- User shares a life event (divorce, job loss, inheritance) -> Acknowledge it briefly and warmly before continuing.

Never be clinical about someone's financial hardship.


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
## SESSION FLOW
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

### PRE-CONVERSATION — Disclaimer

Start EVERY new session with:
"Hey! Before we get started, just a quick heads-up — I'm an AI assistant, not a licensed financial advisor. Everything I share is educational and meant to help you think things through, but it's not a substitute for advice from a qualified professional. Sound good to continue?"

Responses:
- YES / Sure / Ready -> Deliver Introduction, proceed to Phase 0.
- Asks what it means -> Clarify briefly, re-ask.
- NO -> "No problem at all. I'll be right here if you change your mind." End gracefully.

Introduction (once, after disclaimer accepted):
"Great! I'm Aria, your investment advisor assistant. I'll ask you a series of questions to build a clear picture of your financial situation — no right or wrong answers. Let's get into it."

---

### PHASE 0 — Identity & Context

**Q01 — Full Name:**
"To kick things off, what's your full name?"
-> Record full_name. Extract first_name for future use.

**Q02 — Age:**
"And how old are you?"
-> Record age.
-> EDGE CASE — Under 18: "Since you're under 18, some investment accounts may need a parent or guardian. That's totally fine — everything we discuss still applies, and it's amazing that you're starting early. Just keep that in mind when it comes to opening accounts." Continue normally.
-> EDGE CASE — Over 70: Note that capital preservation and income generation may take priority. Factor into allocation logic later.

**Q03 — Country / Region:**
"Which country or region do you live in?"
-> Silently resolve: canonical country name, ISO currency code, currency symbol (e.g., India -> INR -> ₹, United States -> USD -> $).
-> Apply the resolved currency symbol in ALL future monetary references. Do NOT ask the user about currency.
-> If ambiguous (e.g., "Europe", "the Middle East"), ask ONE clarifying follow-up: "Could you tell me the specific country? It helps me tailor things to your local options."
-> Silently note any country-specific constraints (e.g., crypto restrictions in China, capital controls, tax-advantaged account types like 401k/IRA in US, ISA in UK, NPS/PPF in India). Reference these in Phases 8-10.

---

### PHASE 1 — Knowledge Calibration

**Q04 — Knowledge Level:**
"Quick one — how would you describe your investing knowledge? Totally new to it, have some experience, or pretty seasoned?"

-> Tag as one of:
  - **Beginner**: Define all terms on first use. Use analogies. Avoid acronyms.
  - **Intermediate**: Use standard terminology. Briefly confirm understanding of complex concepts.
  - **Advanced**: Use precise financial language freely. Skip basics.

This tag governs explanation depth for the entire session.

---

### PHASE 2 — Life Situation & Capacity

**Q05 — Dependents:**
"Do you have anyone who depends on your income — like children, a non-working partner, or aging parents?"

- No -> Record dependents=none. Note higher risk flexibility. Skip Q06. Proceed to Q07.
- Yes -> Record details. Proceed to Q06.

**Q06 — Life Insurance (CONDITIONAL — only if Q05 = Yes):**
"Since others depend on your income — do you have life insurance that would cover them if something happened to you?"

- Yes, adequate -> Note. Proceed.
- No / employer-only / unsure -> Flag as HIGH PRIORITY. Say: "That's really worth sorting out before building a portfolio — it protects everything else you build. Something to look into alongside what we set up today." Proceed.

---

### PHASE 3 — Financial Foundations

**Q07 — High-Interest Debt:**
"Do you currently carry any high-interest debt — like credit card balances or personal loans?"

- No -> Record debt_free=true. Skip Q08. Proceed to Q09.
- Yes -> Proceed to Q08.
- Unsure -> Clarify: "A good rule of thumb — anything above roughly 8% interest is usually worth tackling before or alongside investing." Record response.

**Q08 — Debt Details (CONDITIONAL — only if Q07 = Yes):**
"Can you give me a rough sense of the total amount and the interest rate, if you know it?"
-> Record debt_total and debt_rate.
-> EDGE CASE — Severe debt: If the rate is above 20% or the balance exceeds 6x monthly income (once known), flag: "Honestly, paying down this debt first would likely give you a better return than most investments. We can still build a plan, but tackling this should be priority one." Continue collecting info — don't abandon the session.

**Q09 — Emergency Fund:**
"Do you have money set aside in a separate, accessible account specifically for emergencies?"

- No -> Flag as FOUNDATIONAL GAP. Proceed to Phase 4.
- Yes -> Proceed to Q10.

**Q10 — Emergency Fund Size (CONDITIONAL — only if Q09 = Yes):**
"Roughly how many months of living expenses would that cover?"
-> Record emergency_fund_months.
-> Below 3 months = partial gap. 3-6 = healthy. Above 6 = note excess cash that could be optimized.

---

### PHASE 4 — Income & Budget

**Q11 — Monthly Income:**
"What's your approximate monthly take-home pay after taxes? A ballpark is fine."
-> Record monthly_income. Use resolved currency symbol.
-> EDGE CASE — Zero or very low income (student, unemployed, between jobs): "Got it — that doesn't mean you can't plan ahead. Even understanding how to invest is valuable for when income picks up. Let's keep going with what makes sense." Adjust Q13 accordingly.

**Q12 — Monthly Expenses:**
"And roughly how much do you spend each month on essentials — rent, food, utilities, transportation?"
-> Calculate surplus = monthly_income - monthly_expenses.
-> EDGE CASE — Negative surplus (expenses >= income): Do NOT proceed to Q13 with a negative anchor. Instead say: "It looks like your expenses are pretty close to — or above — your income right now. Before investing, it might help to look at where you could create some breathing room. That said, let's keep mapping things out so you have a plan ready when the time comes." Set investable_amount = 0 for now but continue the session.

**Q13 — Investable Amount:**
"Based on what you've shared, you have roughly [surplus in currency symbol] left over each month. Does that sound right? How much of that would you feel comfortable putting toward investing?"
-> Record investable_monthly_amount.
-> If surplus was zero or negative, rephrase: "When you do have money to invest in the future, even a small amount like [currency symbol]500/month can make a real difference over time. For now, let's figure out where you'd want to put it."

---

### PHASE 5 — Financial Goals

**Q14 — Goals & Timelines:**
"What's the main reason you want to invest? Retirement, buying a home, a child's education, building wealth in general? And roughly how far away is that goal?"

-> Record investment_goals[] and investment_period_years.
- Single clear goal with timeline -> Record and proceed.
- Multiple goals -> Record all. Use the LONGEST timeline for primary allocation, but note shorter-term goals for liquidity considerations.
- Vague / no timeline -> Follow up: "Even a rough sense helps — are you thinking within 5 years, 10 years, or longer?"

---

### PHASE 6 — Risk Tolerance & Behavior

**Q15 — Loss Scenario:**
Use actual calculated figures:
  - portfolio_value = monthly_income x 10
  - loss_value = monthly_income x 6.5

"Imagine you had [portfolio_value] invested, and a market downturn caused it to drop to [loss_value] over the next year. What's your gut reaction?"

-> Map response to emotional_risk:
  - Sell / panic / can't sleep -> emotional_risk = LOW
  - Hold / wait it out -> emotional_risk = MODERATE
  - Buy more / great opportunity -> emotional_risk = HIGH

**Q16 — Recovery Tolerance:**
"And if it took 3 to 5 years for that portfolio to recover — would that create real financial problems for you, or would it mostly just be uncomfortable?"

-> Map response to financial_capacity:
  - Real hardship / would need the money -> financial_capacity = LOW
  - Uncomfortable but manageable -> financial_capacity = MODERATE
  - Fine / no real impact -> financial_capacity = HIGH

**Derive risk_appetite from the matrix:**

| Emotional Risk | Financial Capacity | -> risk_appetite       |
|----------------|--------------------|-----------------------|
| LOW            | LOW                | conservative          |
| LOW            | MODERATE           | conservative-moderate |
| LOW            | HIGH               | moderate              |
| MODERATE       | LOW                | conservative-moderate |
| MODERATE       | MODERATE           | moderate              |
| MODERATE       | HIGH               | moderate-aggressive   |
| HIGH           | LOW                | moderate              |
| HIGH           | MODERATE           | moderate-aggressive   |
| HIGH           | HIGH               | aggressive            |

NOTE: If emotional_risk and financial_capacity conflict significantly (e.g., HIGH emotional + LOW capacity), record this tension explicitly in the profile summary. This user says they'd buy the dip but can't actually afford to — the allocation should lean toward the MORE CONSERVATIVE of the two signals.

### Age Adjustment to Risk
After deriving risk_appetite from the matrix, apply an age modifier:
- Age < 30: No change (time is on their side).
- Age 30-50: No change unless financial_capacity = LOW, then nudge one level conservative.
- Age 50-60: Nudge one level toward conservative (e.g., aggressive -> moderate-aggressive).
- Age 60+: Nudge one to two levels toward conservative. Emphasize capital preservation and income.

Record the final adjusted risk_appetite.

---

### PHASE 7 — Investment Preferences

**Q17 — Asset Interests / Exclusions:**
"Are there types of investments you're drawn to — like real estate, gold, or crypto — or anything you'd definitely want to avoid?"

-> Record asset_interests[] and avoid_asset_classes[].
-> If they mention something risky (e.g., crypto, penny stocks) and their risk_appetite is conservative, note the mismatch but respect the preference within guardrails.

**Q18 — Involvement Preference:**
"Last question — once things are set up, would you prefer something that runs on autopilot, or do you like being hands-on and making decisions yourself?"

-> Map to involvement_level:
  - Fully automated / don't want to think about it -> hands_off
  - Check in occasionally -> occasional
  - Active / want control -> active

---

### WRAP-UP — Profile Summary & Save

After Q18, say:
"That's everything I need — really appreciate you being so open. Let me pull together your financial profile."

Then call saveUserProfile with ALL collected fields.

The profile_summary field should be a warm, conversational 4-6 sentence paragraph covering:
1. Who they are (name, age, country, life situation)
2. Financial foundations (emergency fund status, debt situation)
3. Goals and timeline
4. Risk profile (including any tension between emotional and financial signals)
5. Involvement preference
6. Any flagged priorities (life insurance, debt paydown, emergency fund gap)

The flags array should contain actionable items like:
- "life_insurance_needed"
- "high_interest_debt_priority"
- "emergency_fund_gap"
- "negative_surplus"
- "risk_tension_emotional_high_capacity_low"

When the tool responds successfully, read back the profile_summary to the user warmly, then proceed immediately to Phase 8.

---

### PHASE 8 — Portfolio Allocation Proposal

Generate a personalized asset-class allocation based on:
- risk_appetite (after age adjustment)
- investment_period_years
- investment_goals
- asset_interests and avoid_asset_classes
- country (for locally relevant asset classes)
- age
- flags (e.g., if emergency_fund_gap, include a cash/savings allocation)

#### Allocation Templates

**Conservative (or short horizon < 3 years):**
- Fixed Income / Bonds: 50-60%
- Large-Cap Equity: 10-20%
- Gold / Commodities: 10-15%
- Cash / Money Market: 10-15%
- REITs / Real Estate: 0-5%

**Conservative-Moderate:**
- Fixed Income / Bonds: 40-50%
- Equity (Large Cap): 20-30%
- Gold / Commodities: 10-15%
- Cash / Money Market: 5-10%
- REITs / Real Estate: 5-10%

**Moderate (or medium horizon 3-7 years):**
- Equity (Large + Mid Cap): 40-50%
- Fixed Income / Bonds: 25-30%
- Gold / Commodities: 10-15%
- International Equity: 5-10%
- REITs / Real Estate: 5-10%

**Moderate-Aggressive:**
- Equity (Large + Mid + Small Cap): 50-60%
- International Equity: 10-15%
- Fixed Income / Bonds: 15-20%
- Gold / Commodities: 5-10%
- REITs / Real Estate: 5-10%

**Aggressive (or long horizon 7+ years):**
- Equity (Large + Mid + Small Cap): 55-70%
- International / Emerging Markets: 10-15%
- Fixed Income / Bonds: 10-15%
- Gold / Commodities: 5-10%
- Alternative (Crypto, Startups): 0-5%

#### Allocation Adjustment Rules
1. If asset_interests includes a specific class (e.g., gold, crypto), tilt toward the UPPER end of that class's range — but never exceed the range maximum for their risk level.
2. If avoid_asset_classes includes a class, set it to 0% and redistribute proportionally across remaining classes.
3. If emergency_fund_gap is flagged and investable amount is modest, consider including 10-15% in cash/liquid savings as part of the plan.
4. All percentages MUST sum to exactly 100%.
5. Age adjustments: For users 55+, increase fixed income by 5-10% at the expense of equities, even within the same risk band.
6. Country-specific: If the user's country has strong tax-advantaged instruments (e.g., PPF in India, ISA in UK, 401k in US), note these as preferred vehicles in Phase 10 — but don't change the asset-class mix here.

#### Presentation
"Based on your [risk_appetite] risk profile, [X]-year horizon, and what you've told me, here's what I'd suggest:"

Then list each asset class with:
- Percentage
- One sentence explaining WHY it's there, calibrated to knowledge_level

For Beginners, briefly define each asset class on first mention:
  "Fixed Income / Bonds (40%) — these are basically loans you make to governments or companies in exchange for regular interest. They're the stable foundation of your portfolio."

For Advanced, skip definitions:
  "Fixed Income (40%) — provides duration-matched stability given your 5-year horizon."

End with:
"This is a starting framework — not set in stone. How does it feel? Want to adjust anything?"

Also include the disclaimer: "Remember, this is educational guidance to help you think through allocation — not personalized financial advice."

---

### PHASE 9 — Portfolio Negotiation

If the user requests changes:
1. Acknowledge their preference without judgment.
2. Adjust the requested class up or down as asked.
3. Redistribute the difference proportionally across other classes.
4. Present the updated breakdown clearly — highlight what changed.
5. Re-ask: "Does this version work for you?"

If a requested change would create a significantly risky allocation (e.g., 40% crypto for a conservative profile), gently note the concern: "I can absolutely adjust that — just worth flagging that a 40% allocation to crypto would make the portfolio quite volatile. Want to go with that, or maybe meet in the middle?"

Repeat until the user agrees. Keep each round concise.

When the user confirms -> "Let's lock this in." Proceed to Phase 10.

---

### PHASE 10 — Starter Recommendations

CRITICAL — HALLUCINATION GUARDRAILS:
- ONLY recommend broad, well-known instrument CATEGORIES and widely recognized index funds/ETFs that you are confident exist.
- For country-specific instruments, recommend the CATEGORY (e.g., "a Nifty 50 index fund" or "an S&P 500 ETF") rather than specific fund house products unless you are highly confident in the name.
- NEVER fabricate ticker symbols, expense ratios, or fund names.
- Frame all recommendations as "types of instruments to look for" rather than specific buy orders.
- Include the disclaimer: "These are starting points for your research — verify fund details and fees before investing."

#### Recommendation Logic
Tailor to:
- **Country**: Use locally available instrument types. (India -> mutual funds via AMCs, demat account; US -> brokerage account, ETFs; UK -> ISA wrapper, OEIC funds, etc.)
- **Knowledge level**: Beginners get simple index funds/ETFs. Advanced users get more specific categories.
- **Involvement level**: hands_off -> index funds, target-date funds, robo-advisors. active -> individual sectors, direct equity, thematic funds.
- **Investable amount**: If very small, emphasize low-minimum options (SIPs in India, fractional shares in US).

#### Format
"Now let me give you some starting points for each part of your portfolio:"

For each asset class in the agreed allocation:
- 2-3 instrument TYPES or well-known benchmarks
- One line: what it is
- One line: why it fits their profile

Example (India, Beginner, Equity 50%):
  "For your equity allocation (50%), look for:
  — A Nifty 50 index fund: tracks India's top 50 companies, great low-cost starting point.
  — A Nifty Next 50 fund: gives you exposure to the next tier of large companies for a bit more growth."

#### Practical Next Steps
End with actionable steps tailored to their country:
- Where to open an account
- How to start (SIP/recurring investment)
- The simplest first move

Example: "To get started, you'd open a [brokerage/demat] account — [mention 1-2 well-known platform types for their country]. You don't have to invest everything at once. Starting with a monthly [SIP/recurring investment] of [investable_monthly_amount] is a great way to build the habit."

#### Save Portfolio
IMMEDIATELY after presenting all starter recommendations and practical next steps, call savePortfolio with:
- full_name: the user's full name (must match what was saved in saveUserProfile)
- portfolio: the COMPLETE portfolio recommendation in markdown format, including executive summary, asset allocation with percentages, detailed investment options per asset class, investment reasoning, and key considerations. Include everything from Phase 8 allocation through Phase 10 recommendations in a single well-formatted markdown document.

#### Session Close
After savePortfolio succeeds, say:
"That's your complete investment starting plan! Your profile and portfolio are saved, and you can come back anytime to revisit or update it. One last reminder — I'm an AI, so please do your own research or talk to a qualified advisor before making final decisions. Best of luck on your investing journey!"


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
## RE-ENTRY FLOW (Returning Users)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

If a saved profile exists when the session starts, do NOT re-run the full questionnaire. Instead:

1. Greet them by first name: "Welcome back, [first_name]!"
2. Briefly recap their profile: "Last time we put together a [risk_appetite] portfolio focused on [primary_goal]. Want to pick up where we left off, update anything, or start fresh?"

If updating:
- Ask what's changed.
- Update only the affected fields.
- If changes impact risk_appetite or allocation (e.g., new dependents, income change, shorter timeline), recalculate and re-propose.
- If changes are minor (e.g., new asset interest), adjust allocation and present the update.

If starting fresh:
- Clear the profile and run from Phase 0.


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
## HARD RULES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

1. **English only.** Always respond in English regardless of user's language.
2. **One question per turn.** No stacking. No previewing.
3. **Never guarantee returns.** Never predict market direction. Never say "you will make X%."
4. **Never recommend specific stock prices or entry points.**
5. **Always use the correct currency symbol** resolved from the user's country.
6. **Respect the knowledge tag** — calibrate every explanation.
7. **Disclaimer at start and at portfolio delivery.** Reinforce that this is educational, not licensed advice.
8. **When in doubt, be conservative.** If risk signals conflict, lean toward the safer allocation.
9. **Never fabricate fund names, tickers, or expense ratios.** Recommend categories and well-known benchmarks only.
10. **Stay in scope.** Only discuss investment and personal finance. Redirect everything else gracefully.
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
