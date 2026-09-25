"""Unit tests for non-playable set/catalog helpers used by the purge script."""

from src.services.card_utils import is_non_playable_catalog_fields, is_non_playable_set


def test_is_non_playable_set_excludes_art_digital_alchemy():
    assert is_non_playable_set("afin", "memorabilia", False) is True
    assert is_non_playable_set("mh3", "expansion", True) is True
    assert is_non_playable_set("y22", "alchemy", False) is True
    assert is_non_playable_set("aa1", "box", False) is True
    assert is_non_playable_set("tkn", "token", False) is True
    assert is_non_playable_set("mh3", "expansion", False) is False
    assert is_non_playable_set("fic", "commander", False) is False


def test_is_non_playable_catalog_fields_excludes_art_and_alchemy_names():
    assert is_non_playable_catalog_fields(
        name="Cloud, Ex-SOLDIER // Cloud, Ex-SOLDIER",
        type_line="Card // Card",
        set_code="afin",
        collector_number="1",
    ) is True
    assert is_non_playable_catalog_fields(
        name="A-Vivi Ornitier",
        type_line="Creature",
        set_code="mh3",
        collector_number="1",
    ) is True
    assert is_non_playable_catalog_fields(
        name="The One Ring",
        type_line="Legendary Artifact",
        set_code="ltr",
        collector_number="A-246",
    ) is True
    assert is_non_playable_catalog_fields(
        name="Cloud, Ex-SOLDIER",
        type_line="Legendary Creature — Human Soldier Mercenary",
        set_code="fic",
        collector_number="2",
    ) is False
