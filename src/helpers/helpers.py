from typing import List


def format_search_offer_msg(firebase_msg):
    print(firebase_msg)
    return firebase_msg


def get_user_country_from_fb_context(context):
    """
    Input looks like: 'projects/_/instances/panprices/refs/offers/SE/gAAAAABghpvku8pg8Ic1s47bvU_7O6HZ8bZgdQHN6tCCEoIYcjjuUvr87hJxettqkH3KWzmwS2lc0077uQ6nJkxvOMRB9RGlUw=='
    Output looks like: 'SE'
    """
    user_country = context.resource.split("/")[-2]
    return user_country


def get_offer_sources() -> List[str]:
    """Get a list of offer sources that are working stably."""
    # TODO: change this to `select * from offer_sources after fixing all scrapers`
    offer_sources = []
    offer_sources.extend([f"ebay_{country}" for country in ("DE", "FR", "IT", "UK")])
    offer_sources.extend(["prisjakt_SE", "prisjakt_FI"])
    offer_sources.extend(["pricerunner_DK", "pricerunner_SE", "pricerunner_SE"])
    offer_sources.extend(
        [
            f"kelkoo_{country}"
            for country in ("DE", "DK", "FI", "FR", "NL", "NO", "SE", "UK")
        ]
    )
    return offer_sources
