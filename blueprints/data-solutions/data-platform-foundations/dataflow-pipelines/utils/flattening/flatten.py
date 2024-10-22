import apache_beam as beam


class flatten(beam.DoFn):
    def process(self, element):
        element = self.flatten_author(element)
        element = self.flatten_entities(element)
        yield element

    def flatten_author(self, element):
        # Logic to flatten author field
        if 'author' in element:
            element['userId'] = element['author']['id']
            element['userName'] = element['author']['userName']
            element['userDescription'] = element['author']['description']
            element['userLocation'] = element['author']['location']
            element['followersCount'] = element['author']['followers']
            element['followingCount'] = element['author']['following']
            element['userFullName'] = element['author']['name']
            element['isVerified'] = element['author']['isVerified']
            element['canDm'] = element['author']['canDm']
            # Add more fields if relevant
        return element

    def flatten_entities(self, element):
        # Logic to flatten entities: hashtags, urls, user mentions
        if 'entities' in element:
            element['hashtags'] = [item['text'] for item in element['entities']['hashtags']]
            element['urls'] = [item['display_url'] for item in element['entities']['urls']]
            element['user_mentions'] = [item['screen_name'] for item in element['entities']['user_mentions']]  #also name?
        return element
