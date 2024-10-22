

def drop_irrelevant_fields(element):
    fields_to_remove = ['irrelevant_field1', 'irrelevant_field2']
    return {key: value for key, value in element.items() if key not in fields_to_remove}


def clean_text(tweet):
    # Text cleaning logic, like removing URLs, special characters, etc.
    pass

def filter_retweets(tweet):
    # Only allow non-retweets
    return not tweet['is_retweet']