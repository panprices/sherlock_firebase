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
