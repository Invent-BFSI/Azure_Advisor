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
            "enum": ["conservative", "conservative-moderate", "moderate", "moderate-aggressive", "aggressive"],
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

_SAVE_PORTFOLIO_SCHEMA = {
    "type": "object",
    "properties": {
        "full_name": {
            "type": "string",
            "description": "The user's full name (must match the name saved in profile).",
        },
        "portfolio": {
            "type": "string",
            "description": (
                "The complete portfolio recommendation in markdown format, including: "
                "executive summary, asset allocation with specific percentages, "
                "detailed investment options per asset class, investment reasoning, "
                "and key considerations. This is the final agreed-upon portfolio after negotiation."
            ),
        },
    },
    "required": ["full_name", "portfolio"],
}


# ── In-memory store for profile data awaiting portfolio ────────
# Maps full_name (lowered) -> row dict so savePortfolio can find it
_pending_profiles: Dict[str, dict] = {}


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


def save_user_profile(session_id: str = "", **kwargs: Any) -> str:
    """Save the completed user investment profile. Stores in memory until portfolio is saved."""
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

    full_name = kwargs.get("full_name", "")
    profile_summary = kwargs.get("profile_summary", "")

    if _name_exists_in_csv(full_name):
        logger.info("Profile already exists for %s — skipping save.", full_name)
        return json.dumps({
            "status": "exists",
            "message": f"A profile for {full_name} already exists. Would you like to update it or continue with the existing profile?",
            "profile_summary": profile_summary,
        })

    # Store pending row — will be written to CSV when portfolio arrives
    _pending_profiles[full_name.lower().strip()] = {
        "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "full_name": full_name,
        "profile_summary": profile_summary,
        "portfolio": "",
    }

    logger.info("Profile saved in memory for %s (session %s), awaiting portfolio.", full_name, session_id)
    return json.dumps({
        "status": "saved",
        "profile_summary": profile_summary,
    })


def save_portfolio(session_id: str = "", **kwargs: Any) -> str:
    """Save the final agreed portfolio recommendation and write the complete row to CSV."""
    full_name = kwargs.get("full_name", "")
    portfolio = kwargs.get("portfolio", "")

    if not full_name or not portfolio:
        err = "Missing required fields: full_name and portfolio are required."
        logger.error(err)
        return json.dumps({"error": err})

    key = full_name.lower().strip()
    pending = _pending_profiles.pop(key, None)

    if pending:
        pending["portfolio"] = portfolio
        _write_csv_row(pending)
        logger.info("Complete profile + portfolio saved to CSV for %s", full_name)
    else:
        # No pending profile — write what we have
        row = {
            "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "full_name": full_name,
            "profile_summary": "",
            "portfolio": portfolio,
        }
        _write_csv_row(row)
        logger.info("Portfolio saved to CSV for %s (no pending profile found)", full_name)

    return json.dumps({
        "status": "saved",
        "message": f"Portfolio recommendation saved for {full_name}.",
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
    {
        "type": "function",
        "name": "savePortfolio",
        "description": (
            "Saves the final agreed-upon portfolio recommendation AFTER the user has confirmed "
            "they are happy with the allocation in Phase 9. The portfolio field must contain "
            "the COMPLETE portfolio recommendation in markdown format including: executive summary, "
            "asset allocation percentages, specific investment options for each asset class, "
            "investment reasoning, and key considerations. Call this ONCE at the end of Phase 10 "
            "after presenting all starter recommendations and practical next steps."
        ),
        "parameters": _SAVE_PORTFOLIO_SCHEMA,
    },
]

AVAILABLE_FUNCTIONS: Dict[str, Callable[..., Any]] = {
    "saveUserProfile": save_user_profile,
    "savePortfolio": save_portfolio,
}
