def format_search_offer_msg(firebase_msg):
    print(firebase_msg)
    return firebase_msg


def get_user_country_from_fb_context(context):
    """
    Input looks like: 'projects/_/instances/panprices/refs/offers/SE'
    Output looks like: 'SE'
    """
    user_country = context.resource.split("/")[-1]
    return user_country
