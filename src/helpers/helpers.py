from typing import List, Tuple


def format_search_offer_msg(firebase_msg):
    print(firebase_msg)
    return firebase_msg


def get_collection_and_doc_from_fs_resource_path(path: str) -> Tuple[str, str]:
    (collection, document) = path.split("/documents/")[1].split("/")
    return (collection, document)


def get_user_country_from_fs_context(path: str) -> str:
    (collection, document) = get_collection_and_doc_from_fs_resource_path(path)
    return document.split("_")[1]


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
    offer_sources = ["prisjakt", "kelkoo", "pricerunner", "idealo"]
    return offer_sources
