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


def _assert_that_none_dcp_is_last(offers):
    indexes_of_nones = [
        i for i, x in enumerate(offers) if x["direct_checkout_price"] is None
    ]
    if len(indexes_of_nones) == 0:
        return

    first_index = indexes_of_nones[0]
    for i in range(first_index, len(offers)):
        assert offers[i]["direct_checkout_price"] is None


def _compare_offers(old_offers, new_offers):
    assert len(new_offers) == len(old_offers)

    # Make sure that offers where direct_checkout_price is None is last in the lists
    _assert_that_none_dcp_is_last(old_offers)
    _assert_that_none_dcp_is_last(new_offers)

    # Compare sorted offers
    old_offers_with_direct_checkout = [
        x for x in old_offers if x["direct_checkout_price"] is not None
    ]
    new_offers_with_direct_checkout = [
        x for x in new_offers if x["direct_checkout_price"] is not None
    ]

    assert len(old_offers_with_direct_checkout) == len(new_offers_with_direct_checkout)

    for i in range(len(old_offers_with_direct_checkout)):
        assert len(old_offers_with_direct_checkout[i]) == len(
            new_offers_with_direct_checkout[i]
        )

        for key in old_offers_with_direct_checkout[i]:
            assert (
                old_offers_with_direct_checkout[i][key]
                == new_offers_with_direct_checkout[i][key]
            )

    # Compare unsorted offers
    old_offers_without_direct_checkout = [
        x for x in old_offers if x["direct_checkout_price"] is None
    ]
    new_offers_without_direct_checkout = [
        x for x in new_offers if x["direct_checkout_price"] is None
    ]

    assert len(old_offers_without_direct_checkout) == len(
        new_offers_without_direct_checkout
    )

    for old_offer in old_offers_without_direct_checkout:
        new_offer = next(
            (
                x
                for x in new_offers_without_direct_checkout
                if x["offer_id"] == old_offer["offer_id"]
            ),
            None,
        )
        assert new_offer is not None

        for key in old_offer:
            assert old_offer[key] == new_offer[key]


def _simplify_offers_list(offers):
    return list(map(lambda offer: dict(offer), offers))


def test_one():
    offers = _get_offers("test_json_dumps/json_dump-1.json")

    legacy_enriched_offers = legacy_add_offers_metadata(offers)
    legacy_enriched_offers = _simplify_offers_list(legacy_enriched_offers)

    enriched_offers = add_offers_metadata(offers)
    enriched_offers = _simplify_offers_list(enriched_offers)

    print(enriched_offers[0])
    # assert False

    _compare_offers(legacy_enriched_offers, enriched_offers)


def test_all():
    for i in range(0, 14):
        print("Running test for " + str(i))
        offers = _get_offers(f"test_json_dumps/json_dump-{i}.json")
        legacy_enriched_offers = legacy_add_offers_metadata(offers)
        legacy_enriched_offers = _simplify_offers_list(legacy_enriched_offers)
        enriched_offers = add_offers_metadata(offers)
        enriched_offers = _simplify_offers_list(enriched_offers)

        _compare_offers(legacy_enriched_offers, enriched_offers)
