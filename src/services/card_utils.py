import re
from typing import Any, Dict, Optional

NON_PLAYABLE_LAYOUTS = {
    "art_series",
    "token",
    "double_faced_token",
    "emblem",
    "front_card",
    "minigame",
}

NON_PLAYABLE_SET_TYPES = {
    "memorabilia",
    "token",
}

NON_PLAYABLE_TYPES = {
    "card",
    "card // card",
}


def normalize_card_name(name: Optional[str]) -> str:
    """
    Normalizes a card name for edition-agnostic matching:
    - Lowercase
    - Strip outer whitespace
    - Normalize internal whitespace
    - Keep only the front face for split / dual face cards (" // ")
    """
    if not name:
        return ""
    cleaned = name.strip().lower()
    cleaned = re.sub(r"\s+", " ", cleaned)
    if "/" in cleaned:
        cleaned = re.sub(r"\s*/+\s*", " // ", cleaned)
        cleaned = cleaned.split(" // ")[0]
    return cleaned


def is_art_card(card: Optional[Dict[str, Any]]) -> bool:
    """Returns True if the given Scryfall card payload represents an art card or memorabilia."""
    if not card or not isinstance(card, dict):
        return False
    layout = (card.get("layout") or "").strip().lower()
    if layout in ("art_series", "front_card"):
        return True
    set_type = (card.get("set_type") or "").strip().lower()
    if set_type == "memorabilia":
        return True
    set_code = (card.get("set") or "").strip().lower()
    # Scryfall art series sets almost universally start with 'a' and are 4 characters (e.g. afin, atmt, aecl, amsh)
    if len(set_code) == 4 and set_code.startswith("a") and set_type in ("memorabilia", "unknown", ""):
        return True
    type_line = (card.get("type_line") or "").strip().lower()
    if type_line in NON_PLAYABLE_TYPES or type_line.startswith("card // card"):
        return True
    # Also check card_faces if any
    for face in card.get("card_faces") or []:
        face_type = (face.get("type_line") or "").strip().lower()
        if face_type in NON_PLAYABLE_TYPES or face_type.startswith("card // card"):
            return True
    return False


# Magic Arena and digital-only set codes
ARENA_AND_DIGITAL_SET_CODES = {
    # Arena beginner / introductory
    "ana", "anb", "xana", "oana", "ajmp", "pana", "parl",
    # Arena anthologies & remastered
    "ea1", "ea2", "ea3", "ha1", "ha2", "ha3", "ha4", "ha5", "ha6", "ha7",
    "aa1", "aa2", "aa3", "aa4", "pa1", "hbg", "j21", "sir", "sis", "akr", "klr", "pio", "om1", "omb",
    # MTGO-only and digital exclusives
    "me1", "me2", "me3", "me4", "vma", "tpr", "td0", "td2", "pz1", "pz2", "prm", "pmoa", "psdg", "past",
}

def is_arena_or_digital_set_code(set_code: Optional[str]) -> bool:
    """Returns True if the set code corresponds to an Arena, Alchemy, or digital-only set."""
    if not set_code:
        return False
    code = set_code.strip().lower()
    if code in ARENA_AND_DIGITAL_SET_CODES:
        return True
    # Alchemy sets: 'y' followed by 2 or 3 alphanumeric characters (e.g. y22, yone, ydmu, yblb, ydft)
    if re.match(r"^y[0-9a-z]{2,3}$", code):
        return True
    return False

def is_digital_or_arena_card(card: Optional[Dict[str, Any]]) -> bool:
    """Returns True if the card represents an MTG Arena, Alchemy, or digital-only card or printing."""
    if not card or not isinstance(card, dict):
        return False

    # 1. Official Alchemy rebalanced card name or collector number
    name = (card.get("name") or "").strip()
    if name.startswith(("A-", "a-")):
        return True
    for face in card.get("card_faces") or []:
        if (face.get("name") or "").strip().startswith(("A-", "a-")):
            return True

    collector_num = str(card.get("collector_number") or "").strip()
    if collector_num.startswith(("A-", "a-")):
        return True

    # 2. Digital flag
    if card.get("digital") is True:
        return True

    # 3. Games availability (paper must be included for physical cards)
    games = card.get("games")
    if games is not None and isinstance(games, list) and "paper" not in games:
        return True

    # 4. Arena security stamp or Alchemy set_type/layout
    if card.get("security_stamp") == "arena":
        return True
    if (card.get("set_type") or "").strip().lower() == "alchemy":
        return True
    if (card.get("layout") or "").strip().lower() == "alchemy":
        return True

    # 5. Set code check
    set_code = (card.get("set") or "").strip().lower()
    if is_arena_or_digital_set_code(set_code):
        return True

    return False

def is_catalog_record_playable(record: Any) -> bool:
    """
    Returns True if a local CardCatalog record represents a real, playable MTG
    card. The catalog row does not store Scryfall layout/set_type, so the art
    card heuristics are reproduced from the fields it does carry: type_line,
    name, collector_number and set_code. Blocks Arena Alchemy rebalances (A-*),
    tokens, emblems, and digital/art sets.
    """
    if record is None:
        return False

    name = (getattr(record, "name", None) or "").strip()
    if name.startswith(("A-", "a-")):
        return False

    collector_number = str(
        getattr(record, "collectorNumber", None)
        or getattr(record, "collector_number", None)
        or ""
    ).strip()
    if collector_number.startswith(("A-", "a-")):
        return False

    type_line = (getattr(record, "typeLine", None) or getattr(record, "type_line", None) or "").strip().lower()
    if type_line in NON_PLAYABLE_TYPES or type_line.startswith("card // card"):
        return False
    if "token" in type_line or "emblem" in type_line:
        return False

    set_code = (getattr(record, "setCode", None) or getattr(record, "set_code", None) or "").strip().lower()
    # Memorabilia / art-series sets use 4-letter codes starting with "a"
    # (e.g. atmt, af30). Keep the heuristic aligned with is_art_card.
    if len(set_code) == 4 and set_code.startswith("a"):
        return False
    if is_arena_or_digital_set_code(set_code):
        return False

    return True

def is_playable_card(card: Optional[Dict[str, Any]]) -> bool:
    """Returns True if the given Scryfall card payload is a real, playable paper MTG card."""
    if not card or not isinstance(card, dict):
        return False
    if is_art_card(card):
        return False
    if is_digital_or_arena_card(card):
        return False
    layout = (card.get("layout") or "").strip().lower()
    if layout in NON_PLAYABLE_LAYOUTS:
        return False
    set_type = (card.get("set_type") or "").strip().lower()
    if set_type in NON_PLAYABLE_SET_TYPES:
        return False
    type_line = (card.get("type_line") or "").strip().lower()
    if type_line in NON_PLAYABLE_TYPES:
        return False
    return True


def is_non_playable_set(set_code: Optional[str], set_type: Optional[str], is_digital: bool = False) -> bool:
    """True for art-series, memorabilia, token, Arena, Alchemy, or digital sets."""
    if is_digital:
        return True
    st = (set_type or "").strip().lower()
    if st in ("alchemy", "memorabilia", "token"):
        return True
    code = (set_code or "").strip().lower()
    if len(code) == 4 and code.startswith("a"):
        return True
    if is_arena_or_digital_set_code(code):
        return True
    return False


def is_non_playable_catalog_fields(
    name: Optional[str] = None,
    type_line: Optional[str] = None,
    set_code: Optional[str] = None,
    collector_number: Optional[str] = None,
) -> bool:
    """True when catalog-like fields describe art, tokens, or digital/Alchemy cards."""
    n = (name or "").strip()
    if n.startswith(("A-", "a-")):
        return True
    cn = str(collector_number or "").strip()
    if cn.startswith(("A-", "a-")):
        return True
    tl = (type_line or "").strip().lower()
    if tl in NON_PLAYABLE_TYPES or tl.startswith("card // card"):
        return True
    if "token" in tl or "emblem" in tl:
        return True
    code = (set_code or "").strip().lower()
    if len(code) == 4 and code.startswith("a"):
        return True
    if is_arena_or_digital_set_code(code):
        return True
    return False

