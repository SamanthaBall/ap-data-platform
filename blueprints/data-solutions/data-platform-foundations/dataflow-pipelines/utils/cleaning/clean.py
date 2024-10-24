import apache_beam as beam

class DropIrrelevantFields(beam.DoFn):
    def __init__(self, fields_to_remove):
        self.fields_to_remove = fields_to_remove

    def process(self, element):
        yield {key: value for key, value in element.items() if key not in self.fields_to_remove}




def clean_text(tweet):
    # Text cleaning logic, like removing URLs, special characters, etc.
    pass

def filter_retweets(tweet):
    # Only allow non-retweets
    return not tweet['isRetweet']