from __future__ import annotations

import csv
import datetime
import json
import logging
import os
from typing import Any, Callable, Dict

logger = logging.getLogger(__name__)

CSV_FILE = "customer_details.csv"

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

_SAVE_PROFILE_SCHEMA = {
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


def _save_to_csv(session_id: str, profile: dict) -> None:
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
    logger.info("Profile saved to CSV for session %s", session_id)


def save_user_profile(session_id: str = "", **kwargs: Any) -> str:
    """Save the completed user investment profile to CSV after all 18 questions."""
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
    missing = [k for k in required if not kwargs.get(k)]
    if missing:
        err = f"Missing required fields: {missing}. Please collect all answers first."
        logger.error(err)
        return json.dumps({"error": err})

    _save_to_csv(session_id, kwargs)
    logger.info("Profile saved successfully for session %s", session_id)
    return json.dumps({
        "status": "saved",
        "profile_summary": kwargs.get("profile_summary", ""),
    })


TOOLS_LIST = [
    {
        "type": "function",
        "name": "saveUserProfile",
        "description": (
            "Saves the completed user profile to the database after all 18 questions "
            "have been answered. YOU must populate canonical_country, currency_code, "
            "and currency_symbol from the user's stated region — do NOT ask the user. "
            "YOU must derive risk_appetite from the Q15/Q16 two-dimensional matrix. "
            "The profile_summary field must contain the full 4-6 sentence conversational "
            "summary you will read back to the user. Call ONLY after ALL questions are done."
        ),
        "parameters": _SAVE_PROFILE_SCHEMA,
    },
]

AVAILABLE_FUNCTIONS: Dict[str, Callable[..., Any]] = {
    "saveUserProfile": save_user_profile,
}
