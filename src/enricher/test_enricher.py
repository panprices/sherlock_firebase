import json
import pprint
import uuid
from deepdiff import DeepDiff
from src.enricher.enricher import add_offers_metadata
from src.enricher.enricher_legacy import (
    add_offers_metadata as legacy_add_offers_metadata,
)

# Run these tests with pytest


def _add_offer_id(offer):
    offer["offer_id"] = str(uuid.uuid4())
    return offer


def _get_offers(filepath):
    f = open(filepath, "r")
    offers = json.loads(f.read())
    return list(map(_add_offer_id, offers))


def test_one():
    offers = _get_offers("test_json_dumps/json_dump-1.json")
    legacy_enriched_offers = json.dumps(
        legacy_add_offers_metadata(offers),
        sort_keys=True,
        indent=4,
    )
    enriched_offers = json.dumps(
        add_offers_metadata(offers),
        sort_keys=True,
        indent=4,
    )
    assert legacy_enriched_offers == enriched_offers


def test_all():
    for i in range(0, 14):
        print("Running test for " + str(i))
        offers = _get_offers(f"test_json_dumps/json_dump-{i}.json")
        legacy_enriched_offers = json.dumps(
            legacy_add_offers_metadata(offers),
            sort_keys=True,
            indent=4,
        )
        enriched_offers = json.dumps(
            add_offers_metadata(offers),
            sort_keys=True,
            indent=4,
        )
        assert legacy_enriched_offers == enriched_offers
