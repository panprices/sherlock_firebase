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
    offer_sources = [
        "ebay_DE",
        "ebay_FR",
        "ebay_IT",
        "ebay_UK",
        "prisjakt_SE",
        "prisjakt_FI",
        "pricerunner_DK",
        "pricerunner_SE",
        "pricerunner_SE",
        "kelkoo_DE",
        "kelkoo_DK",
        "kelkoo_FI",
        "kelkoo_FR",
        "kelkoo_NL",
        "kelkoo_NO",
        "kelkoo_SE",
        "kelkoo_UK",
        "idealo_DE",
        "idealo_ES",
        "idealo_FR",
        "idealo_IT",
        "idealo_UK",
    ]
    return offer_sources
