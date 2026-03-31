from __future__ import annotations

import csv
import datetime
import json
import logging
import os
from typing import Any, Callable, Dict

logger = logging.getLogger(__name__)

CSV_FILE = "customerDetail.csv"

CSV_COLUMNS = [
    "timestamp",
    "full_name",
    "profile_summary",
    "portfolio",
]

# ═══════════════════════════════════════════════════════════
#  In-memory stores (keyed by session_id)
# ═══════════════════════════════════════════════════════════

_session_answers: Dict[str, dict] = {}  # accumulated answers per session
_pending_profiles: Dict[str, dict] = {}  # awaiting portfolio (keyed by name)


# ═══════════════════════════════════════════════════════════
#  Risk matrix computation
# ═══════════════════════════════════════════════════════════

_RISK_LEVELS = [
    "conservative",
    "conservative-moderate",
    "moderate",
    "moderate-aggressive",
    "aggressive",
]

_RISK_MATRIX = {
    ("low", "low"): "conservative",
    ("low", "moderate"): "conservative-moderate",
    ("low", "high"): "moderate",
    ("moderate", "low"): "conservative-moderate",
    ("moderate", "moderate"): "moderate",
    ("moderate", "high"): "moderate-aggressive",
    ("high", "low"): "moderate",
    ("high", "moderate"): "moderate-aggressive",
    ("high", "high"): "aggressive",
}


def _compute_risk_appetite(emotional: str, financial: str, age: int) -> str:
    """Derive risk_appetite from emotional × financial matrix with age adjustment."""
    risk = _RISK_MATRIX.get(
        (emotional.lower().strip(), financial.lower().strip()), "moderate"
    )
    idx = _RISK_LEVELS.index(risk)

    # Age adjustment
    if age >= 60:
        idx = max(0, idx - 2)
    elif age >= 50:
        idx = max(0, idx - 1)
    elif 30 <= age <= 50 and financial.lower().strip() == "low":
        idx = max(0, idx - 1)

    return _RISK_LEVELS[idx]


# ═══════════════════════════════════════════════════════════
#  Flags builder
# ═══════════════════════════════════════════════════════════

def _build_flags(profile: dict) -> list:
    flags = []

    if profile.get("high_interest_debt"):
        rate = profile.get("debt_rate_pct", 0)
        balance = profile.get("debt_balance", 0)
        sym = profile.get("currency_symbol", "$")
        rate_str = f" at ~{rate:.0f}%" if rate else ""
        bal_str = f" ({sym}{balance:,.0f})" if balance else ""
        flags.append(f"high_interest_debt_priority{bal_str}{rate_str}")

    has_ef = profile.get("has_emergency_fund", False)
    ef_months = float(profile.get("emergency_fund_months", 0))
    if not has_ef:
        flags.append("emergency_fund_gap")
    elif ef_months < 3:
        flags.append(f"emergency_fund_low_{ef_months:.0f}mo")

    if profile.get("has_dependents") and not profile.get("has_life_insurance", True):
        flags.append("life_insurance_needed")

    inflow = float(profile.get("monthly_inflow", 0))
    outflow = float(profile.get("monthly_outflow", 0))
    if inflow > 0 and outflow >= inflow:
        flags.append("negative_surplus")

    emotional = profile.get("risk_tolerance_emotional", "").lower()
    financial = profile.get("risk_capacity_financial", "").lower()
    if emotional == "high" and financial == "low":
        flags.append("risk_tension_emotional_high_capacity_low")

    return flags


# ═══════════════════════════════════════════════════════════
#  Currency formatting helper
# ═══════════════════════════════════════════════════════════

def _format_currency(amount: float, symbol: str, code: str) -> str:
    """Format a number with currency symbol. Uses Indian formatting for INR."""
    if code.upper() == "INR":
        # Indian number system: 1,00,000 format
        s = f"{amount:,.0f}"
        # Convert standard comma format to Indian
        parts = s.split(".")
        integer_part = parts[0].replace(",", "")
        if len(integer_part) > 3:
            last_three = integer_part[-3:]
            rest = integer_part[:-3]
            # Group the rest in pairs from the right
            groups = []
            while rest:
                groups.append(rest[-2:])
                rest = rest[:-2]
            groups.reverse()
            formatted = ",".join(groups) + "," + last_three
        else:
            formatted = integer_part
        return f"{symbol}{formatted}"
    else:
        return f"{symbol}{amount:,.0f}"


# ═══════════════════════════════════════════════════════════
#  CSV helpers
# ═══════════════════════════════════════════════════════════

def _name_exists_in_csv(full_name: str) -> bool:
    """Check if a name already exists in the CSV file (case-insensitive)."""
    if not os.path.isfile(CSV_FILE):
        return False
    try:
        with open(CSV_FILE, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            target = full_name.lower().strip()
            for row in reader:
                if row.get("full_name", "").lower().strip() == target:
                    return True
    except Exception:
        pass
    return False


def _write_csv_row(row: dict) -> None:
    """Append a single row to the CSV file."""
    file_exists = os.path.isfile(CSV_FILE)
    with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)


# ═══════════════════════════════════════════════════════════
#  Tool 1: recordAnswers
# ═══════════════════════════════════════════════════════════

_RECORD_ANSWERS_SCHEMA = {
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
        "involvement_level": {
            "type": "string",
            "enum": ["hands-off", "occasional", "active", "diy"],
        },
    },
    "required": [],
}


def record_answers(session_id: str = "", **kwargs: Any) -> str:
    """Record collected answers and return pre-computed values if income data is present."""
    if not session_id:
        return json.dumps({"error": "No session_id provided."})

    # Initialize or merge into existing session data
    if session_id not in _session_answers:
        _session_answers[session_id] = {}

    # Merge new answers (skip None values)
    for key, value in kwargs.items():
        if value is not None:
            _session_answers[session_id][key] = value

    stored = _session_answers[session_id]
    logger.info(
        "[%s] recordAnswers: stored %d fields total.",
        session_id,
        len(stored),
    )

    # If we have income data, return pre-computed values for upcoming questions
    inflow = stored.get("monthly_inflow")
    outflow = stored.get("monthly_outflow")
    symbol = stored.get("currency_symbol", "$")
    code = stored.get("currency_code", "USD")

    if inflow is not None and outflow is not None:
        inflow = float(inflow)
        outflow = float(outflow)
        surplus = round(inflow - outflow, 2)
        suggested = round(surplus * 0.6) if surplus > 0 else 0
        pv_10x = inflow * 10
        lv_6_5x = inflow * 6.5

        result = {
            "status": "recorded",
            "surplus": surplus,
            "surplus_formatted": _format_currency(surplus, symbol, code),
            "suggested_investment": suggested,
            "suggested_investment_formatted": _format_currency(suggested, symbol, code),
            "portfolio_value_10x": _format_currency(pv_10x, symbol, code),
            "loss_value_6_5x": _format_currency(lv_6_5x, symbol, code),
        }
        logger.info("[%s] Returning computed values: surplus=%s", session_id, surplus)
        return json.dumps(result)

    return json.dumps({"status": "recorded"})


# ═══════════════════════════════════════════════════════════
#  Tool 2: computeAndSaveProfile
# ═══════════════════════════════════════════════════════════

_COMPUTE_PROFILE_SCHEMA = {
    "type": "object",
    "properties": {
        "profile_summary": {
            "type": "string",
            "description": (
                "The full conversational 4-6 sentence profile summary Aria will read "
                "back to the user. Do NOT include risk_appetite or flags — the backend "
                "computes those. Focus on who they are, their situation, goals, and "
                "involvement preference."
            ),
        },
    },
    "required": ["profile_summary"],
}


def compute_and_save_profile(session_id: str = "", **kwargs: Any) -> str:
    """Compute risk appetite, flags, and save profile to CSV."""
    stored = _session_answers.get(session_id, {})
    if not stored:
        return json.dumps({"error": "No recorded answers found for this session. Call recordAnswers first."})

    # Merge any additional kwargs (profile_summary)
    stored.update({k: v for k, v in kwargs.items() if v is not None})

    full_name = stored.get("full_name", "")
    profile_summary = stored.get("profile_summary", "")

    if not full_name:
        return json.dumps({"error": "full_name not found in recorded answers."})

    # Check for duplicates
    if _name_exists_in_csv(full_name):
        logger.info("Profile already exists for %s — skipping save.", full_name)
        # Still compute risk so the model can proceed with Phase 8
        emotional = stored.get("risk_tolerance_emotional", "moderate")
        financial = stored.get("risk_capacity_financial", "moderate")
        age = int(stored.get("age", 30))
        risk_appetite = _compute_risk_appetite(emotional, financial, age)
        flags = _build_flags(stored)
        return json.dumps({
            "status": "exists",
            "message": f"A profile for {full_name} already exists. Proceeding with portfolio generation.",
            "risk_appetite": risk_appetite,
            "flags": flags,
            "profile_summary": profile_summary,
        })

    # Compute derived values
    emotional = stored.get("risk_tolerance_emotional", "moderate")
    financial = stored.get("risk_capacity_financial", "moderate")
    age = int(stored.get("age", 30))
    risk_appetite = _compute_risk_appetite(emotional, financial, age)
    flags = _build_flags(stored)

    logger.info(
        "[%s] Computed risk_appetite=%s (emotional=%s, financial=%s, age=%d)",
        session_id, risk_appetite, emotional, financial, age,
    )

    # Store pending row for CSV (portfolio column filled later by savePortfolio)
    key = full_name.lower().strip()
    _pending_profiles[key] = {
        "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "full_name": full_name,
        "profile_summary": profile_summary,
        "portfolio": "",
    }

    logger.info("Profile computed and stored for %s, awaiting portfolio.", full_name)

    return json.dumps({
        "status": "saved",
        "risk_appetite": risk_appetite,
        "flags": flags,
        "profile_summary": profile_summary,
    })


# ═══════════════════════════════════════════════════════════
#  Tool 3: savePortfolio
# ═══════════════════════════════════════════════════════════

_SAVE_PORTFOLIO_SCHEMA = {
    "type": "object",
    "properties": {
        "full_name": {
            "type": "string",
            "description": "The user's full name (must match the name from recordAnswers).",
        },
        "portfolio": {
            "type": "string",
            "description": (
                "The complete portfolio recommendation in markdown format, including: "
                "executive summary, asset allocation with specific percentages, "
                "detailed investment options per asset class, investment reasoning, "
                "and key considerations. This is the final agreed-upon portfolio."
            ),
        },
    },
    "required": ["full_name", "portfolio"],
}


def save_portfolio(session_id: str = "", **kwargs: Any) -> str:
    """Save the final agreed portfolio and write the complete row to CSV."""
    full_name = kwargs.get("full_name", "")
    portfolio = kwargs.get("portfolio", "")

    if not full_name or not portfolio:
        return json.dumps({"error": "Missing required fields: full_name and portfolio."})

    key = full_name.lower().strip()
    pending = _pending_profiles.pop(key, None)

    if pending:
        pending["portfolio"] = portfolio
        _write_csv_row(pending)
        logger.info("Complete profile + portfolio saved to CSV for %s", full_name)
    else:
        row = {
            "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "full_name": full_name,
            "profile_summary": "",
            "portfolio": portfolio,
        }
        _write_csv_row(row)
        logger.info("Portfolio saved to CSV for %s (no pending profile found)", full_name)

    # Clean up session answers
    if session_id and session_id in _session_answers:
        del _session_answers[session_id]

    return json.dumps({
        "status": "saved",
        "message": f"Portfolio recommendation saved for {full_name}.",
    })


# ═══════════════════════════════════════════════════════════
#  Tool definitions & registry
# ═══════════════════════════════════════════════════════════

TOOLS_LIST = [
    {
        "type": "function",
        "name": "recordAnswers",
        "description": (
            "Records the user's answers collected so far and stores them server-side. "
            "Call this TWICE during the questionnaire:\n"
            "1) After Phase 4 (income & expenses) — include all fields from Phases 0-4. "
            "The response will contain pre-computed values (surplus, suggested_investment, "
            "portfolio_value_10x, loss_value_6_5x) that you MUST use in subsequent questions. "
            "Do NOT calculate these yourself.\n"
            "2) After Phase 7 (involvement preference) — include remaining fields "
            "(investment_amount, goals, period, risk signals, involvement_level).\n"
            "Only include fields you have collected — omit any unknown fields."
        ),
        "parameters": _RECORD_ANSWERS_SCHEMA,
    },
    {
        "type": "function",
        "name": "computeAndSaveProfile",
        "description": (
            "Computes risk_appetite (from emotional × financial matrix with age adjustment), "
            "builds flags, and saves the profile. Call ONCE after Q13 in the wrap-up phase. "
            "Pass ONLY profile_summary — all other data was already recorded via recordAnswers. "
            "The response returns the computed risk_appetite and flags. "
            "Use the RETURNED risk_appetite for Phase 8 portfolio allocation — do NOT derive it yourself."
        ),
        "parameters": _COMPUTE_PROFILE_SCHEMA,
    },
    {
        "type": "function",
        "name": "savePortfolio",
        "description": (
            "Saves the final agreed-upon portfolio recommendation AFTER the user has confirmed "
            "the allocation in Phase 9. The portfolio field must contain the COMPLETE portfolio "
            "in markdown format including: executive summary, asset allocation percentages, "
            "specific investment options, reasoning, and key considerations. "
            "Call ONCE at the end of Phase 10."
        ),
        "parameters": _SAVE_PORTFOLIO_SCHEMA,
    },
]

AVAILABLE_FUNCTIONS: Dict[str, Callable[..., Any]] = {
    "recordAnswers": record_answers,
    "computeAndSaveProfile": compute_and_save_profile,
    "savePortfolio": save_portfolio,
}
