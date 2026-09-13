from pathlib import Path
import re

from src.services.card_utils import normalize_card_name


FIXTURE = Path(__file__).parent / "fixtures" / "shire-partner-deck.txt"


def test_partner_deck_fixture_is_readable_without_card_name_errors():
    lines = FIXTURE.read_text(encoding="utf-8").splitlines()
    mainboard = []
    sideboard = []
    commanders = []
    section = "main"

    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            continue
        if line.upper() == "SIDEBOARD:":
            section = "sideboard"
            continue
        if line.upper() == "COMMANDERS:":
            section = "commanders"
            continue
        match = re.match(r"^(\d+)\s+(.+)$", line)
        assert match, f"Unparseable deck line: {line}"
        quantity, name = int(match.group(1)), match.group(2)
        assert quantity > 0
        assert normalize_card_name(name)
        target = {"main": mainboard, "sideboard": sideboard, "commanders": commanders}[section]
        target.append((quantity, name))

    assert sum(quantity for quantity, _ in mainboard) == 98
    assert sum(quantity for quantity, _ in sideboard) == 24
    assert commanders == [
        (1, "Frodo, Adventurous Hobbit"),
        (1, "Sam, Loyal Attendant"),
    ]
    assert "Dusk/Dawn" in {name for _, name in mainboard}
