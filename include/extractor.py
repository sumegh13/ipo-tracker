# extractor.py — full updated version

import re
from typing import Optional
from dataclasses import dataclass
from bs4 import BeautifulSoup
import anthropic

@dataclass
class OfferingTerms:
    shares_offered: Optional[int] = None
    price_low: Optional[float] = None
    price_high: Optional[float] = None
    offering_type: str = "unknown"   # "ipo", "spac", "follow_on", "unknown"
    extraction_method: str = "regex"


def normalize(text: str) -> str:
    return re.sub(r"\s+", " ", text)


def classify_offering(cover: str) -> str:
    t = cover.lower()

    spac_signals = [
        "trust account", "per public share",
        "initial business combination", "blank check company", "founder shares",
    ]
    if sum(1 for s in spac_signals if s in t) >= 2:
        return "spac"

    follow_on_signals = [
        "our common stock is listed", "our shares are listed",
        "our common stock is traded", "currently listed on",
        "currently traded on", "shares are currently listed",
        "our ordinary shares are listed",              # foreign follow-ons
        "traded on the nyse", "traded on nasdaq",
        "listed on the nasdaq", "listed on the nyse",
        "supplement to the prospectus",               # shelf takedowns
        "is a direct offering",
    ]
    if any(s in t for s in follow_on_signals):
        return "follow_on"

    ipo_signals = [
        "this is an initial public offering",
        "no public market for our common stock",
        "no established public trading market",
        "prior to this offering, there has been no",
        "no public market has existed",
        "no prior public market",
        "no public trading market currently exists",
    ]
    if any(s in t for s in ipo_signals):
        return "ipo"

    # Fallback: if it looks priced like an IPO and has underwriting table
    if re.search(r"underwriting discounts? and commissions?", t):
        if re.search(r"no .{0,30} public market", t):
            return "ipo"
        return "follow_on"   # has underwriter but existing company

    return "unknown"


def extract_price_range(cover: str) -> tuple[Optional[float], Optional[float]]:
    text = normalize(cover)

    # --- Price range (S-1/A pre-IPO) ---
    range_patterns = [
        r"\$\s*(\d+\.?\d{0,2})\s+(?:to|and|-)\s+\$\s*(\d+\.?\d{0,2})\s+per\s+share",
        r"between\s+\$\s*(\d+\.?\d{0,2})\s+and\s+\$\s*(\d+\.?\d{0,2})",
        r"price\s+range\s+of\s+\$\s*(\d+\.?\d{0,2})\s+(?:to|-)\s+\$\s*(\d+\.?\d{0,2})",
    ]
    for pat in range_patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            low, high = float(m.group(1)), float(m.group(2))
            if 1 <= low <= 500 and low <= high:
                return low, high

    # --- Single final price (424B4) ---
    # Handles prose: "price per share is $16.00"
    # Handles the underwriting table: "Per Share $ 16.00"  (space between $ and digits)
    single_patterns = [
        r"initial public offering price per share is \$\s*(\d+(?:\.\d{1,2})?)",
        r"(?:initial public offering price|price to (?:the )?public)"
            r"[^$\n]{0,80}\$\s*(\d+(?:\.\d{1,2})?)",
        r"combined offering price of \$\s*(\d+(?:\.\d{1,2})?)\s+per\s+share",
        r"public offering price of \$\s*(\d+(?:\.\d{1,2})?)\s+per\s+share",
        r"offering price of \$\s*(\d+(?:\.\d{1,2})?)\s+per\s+(?:share|unit)",
        # Underwriting table row — "Per Share $ 16.00"
        r"per\s+share\s+\$\s*(\d+(?:\.\d{1,2})?)",
        # Standalone dollar amount on its own on the cover (last resort)
        r"^\$\s*(\d+(?:\.\d{1,2})?)$",
    ]
    for pat in single_patterns:
        m = re.search(pat, text, re.IGNORECASE | re.MULTILINE)
        if m:
            price = float(m.group(1))
            if 0.01 <= price <= 500:
                return price, price

    return None, None


def extract_shares_offered(cover: str) -> Optional[int]:
    text = normalize(cover)

    def to_int(s: str) -> Optional[int]:
        n = int(s.replace(",", ""))
        return n if 10_000 <= n <= 1_000_000_000 else None

    # ── Priority 1: explicit "we are offering X shares" ──────────────────
    # handles: straight, "of our", "on a firm commitment basis, X"
    we_patterns = [
        # "we are offering 11,000,000 ordinary shares"
        # "we are offering 3,846,153 of our ordinary shares"
        r"we\s+are\s+offering\s+([\d,]+)"
        r"(?:\s+of(?:\s+our)?)?"                           # optional "of our"
        r"\s+(?:ordinary\s+|class\s+[a-z]\s+)?shares",

        # "we are offering, on a firm commitment basis, 1,050,000 Ordinary Shares"
        r"we\s+are\s+offering[^,\d]{0,60},\s*([\d,]+)"
        r"\s+(?:ordinary\s+|class\s+[a-z]\s+)?shares",
    ]
    for pat in we_patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            n = to_int(m.group(1))
            if n:
                return n

    # ── Priority 2: ADS offerings ─────────────────────────────────────────
    m = re.search(
        r"([\d,]+)\s+(?:american\s+depositary\s+shares|adss?)\b",
        text, re.IGNORECASE
    )
    if m:
        n = to_int(m.group(1))
        if n:
            return n

    # ── Priority 3: cover page headline ──────────────────────────────────
    # catches: "11,000,000 Shares LOAR", "23,500,000 Shares CLASS A",
    #          "1,600,000 Shares of Common Stock", "1,050,000 Ordinary Shares"
    m = re.search(
        r"([\d,]{5,})\s+(?:ordinary\s+|class\s+[a-z]\s+)?shares?\b",
        text, re.IGNORECASE
    )
    if m:
        n = to_int(m.group(1))
        if n:
            return n

    return None


def get_cover_page_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    chunks, total = [], 0
    for tag in soup.find_all(["p", "div", "span", "td"], recursive=True):
        text = tag.get_text(" ", strip=True)
        if text:
            chunks.append(text)
            total += len(text)
        if total > 15_000:
            break
    return " ".join(chunks)


def extract_offering_terms(html: str) -> OfferingTerms:
    cover = get_cover_page_text(html)
    offering_type = classify_offering(cover)
    low, high = extract_price_range(cover)
    shares = extract_shares_offered(cover)

    # Only fall back to LLM for IPOs where we got nothing
    if offering_type == "ipo" and low is None and shares is None:
        return extract_with_llm(cover)

    return OfferingTerms(
        shares_offered=shares,
        price_low=low,
        price_high=high,
        offering_type=offering_type,
    )


def extract_with_llm(cover_text: str) -> OfferingTerms:
    """Fallback: use Claude Haiku to extract when regex fails."""
    import anthropic, json, re

    client = anthropic.Anthropic()

    # Cover page is all we need — no point sending the full doc
    sample = cover_text[:4_000]

    response = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=200,
        messages=[{
            "role": "user",
            "content": f"""Extract offering terms from this prospectus cover page.
Return ONLY a raw JSON object — no markdown, no explanation.
Keys (use null if not found):
  offering_type: one of "ipo", "spac", "follow_on", "unknown"
  shares_offered: integer or null
  price_low: number or null
  price_high: number or null

Cover page:
{sample}"""
        }]
    )

    raw = response.content[0].text.strip()

    # Strip markdown fences if the model added them anyway
    raw = re.sub(r"^```(?:json)?\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)

    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        # Model returned something unparseable — treat as unknown
        return OfferingTerms(extraction_method="llm_parse_error")

    return OfferingTerms(
        shares_offered=data.get("shares_offered"),
        price_low=data.get("price_low"),
        price_high=data.get("price_high"),
        offering_type=data.get("offering_type", "unknown"),
        extraction_method="llm",
    )