from src.services.card_utils import normalize_card_name

def test_normalize_card_name():
    assert normalize_card_name("Black Lotus") == "black lotus"
    assert normalize_card_name("  Lightning   Bolt  ") == "lightning bolt"
    assert normalize_card_name("Wear // Tear") == "wear"
    assert normalize_card_name("Fire // Ice") == "fire"
    assert normalize_card_name("") == ""
    assert normalize_card_name(None) == ""
