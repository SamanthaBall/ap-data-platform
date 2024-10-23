import apache_beam as beam
import logging
import numpy as np
import os
from fuzzywuzzy import process as fuzzy_process
from geopy.geocoders import OpenCage


# List of country names for fuzzy matching
COUNTRY_NAMES =  [
    'Afghanistan', 'Albania', 'Algeria', 'Andorra', 'Angola', 
    'Antigua and Barbuda', 'Argentina', 'Armenia', 'Australia', 
    'Austria', 'Azerbaijan', 'Bahamas', 'Bahrain', 'Bangladesh', 
    'Barbados', 'Belarus', 'Belgium', 'Belize', 'Benin', 
    'Bhutan', 'Bolivia', 'Bosnia and Herzegovina', 'Botswana', 
    'Brazil', 'Brunei', 'Bulgaria', 'Burkina Faso', 'Burundi', 
    'Cabo Verde', 'Cambodia', 'Cameroon', 'Canada', 'Central African Republic', 
    'Chad', 'Chile', 'China', 'Colombia', 'Comoros', 
    'Congo, Democratic Republic of the', 'Congo, Republic of the', 'Costa Rica', 
    'Croatia', 'Cuba', 'Cyprus', 'Czech Republic', 'Denmark', 
    'Djibouti', 'Dominica', 'Dominican Republic', 'Ecuador', 'Egypt', 
    'El Salvador', 'Equatorial Guinea', 'Eritrea', 'Estonia', 'Eswatini', 
    'Ethiopia', 'Fiji', 'Finland', 'France', 'Gabon', 
    'Gambia', 'Georgia', 'Germany', 'Ghana', 'Greece', 
    'Grenada', 'Guatemala', 'Guinea', 'Guinea-Bissau', 'Guyana', 
    'Haiti', 'Honduras', 'Hungary', 'Iceland', 'India', 
    'Indonesia', 'Iran', 'Iraq', 'Ireland', 'Israel', 
    'Italy', 'Jamaica', 'Japan', 'Jordan', 'Kazakhstan', 
    'Kenya', 'Kiribati', 'Kuwait', 'Kyrgyzstan', 'Laos', 
    'Latvia', 'Lebanon', 'Lesotho', 'Liberia', 'Libya', 
    'Liechtenstein', 'Lithuania', 'Luxembourg', 'Madagascar', 'Malawi', 
    'Malaysia', 'Maldives', 'Mali', 'Malta', 'Marshall Islands', 
    'Mauritania', 'Mauritius', 'Mexico', 'Micronesia', 'Moldova', 
    'Monaco', 'Mongolia', 'Montenegro', 'Morocco', 'Mozambique', 
    'Myanmar', 'Namibia', 'Nauru', 'Nepal', 'Netherlands', 
    'New Zealand', 'Nicaragua', 'Niger', 'Nigeria', 'North Korea', 
    'North Macedonia', 'Norway', 'Oman', 'Pakistan', 'Palau', 
    'Palestine', 'Panama', 'Papua New Guinea', 'Paraguay', 'Peru', 
    'Philippines', 'Poland', 'Portugal', 'Qatar', 'Romania', 
    'Russia', 'Rwanda', 'Saint Kitts and Nevis', 'Saint Lucia', 
    'Saint Vincent and the Grenadines', 'Samoa', 'San Marino', 
    'Sao Tome and Principe', 'Saudi Arabia', 'Senegal', 
    'Serbia', 'Seychelles', 'Sierra Leone', 'Singapore', 
    'Slovakia', 'Slovenia', 'Solomon Islands', 'Somalia', 
    'South Africa', 'South Korea', 'South Sudan', 'Spain', 
    'Sri Lanka', 'Sudan', 'Suriname', 'Sweden', 'Switzerland', 
    'Syria', 'Taiwan', 'Tajikistan', 'Tanzania', 'Thailand', 
    'Togo', 'Tonga', 'Trinidad and Tobago', 'Tunisia', 
    'Turkey', 'Turkmenistan', 'Tuvalu', 'Uganda', 'Ukraine', 
    'United Arab Emirates', 'United Kingdom', 'United States', 
    'Uruguay', 'Uzbekistan', 'Vanuatu', 'Vatican City', 
    'Venezuela', 'Vietnam', 'Yemen', 'Zambia', 'Zimbabwe'
]

class FuzzyMatchLocation(beam.DoFn):
    def process(self, element):
        location = element.get('location', '')
        if not location:
            yield {**element, 'country': ""}
            return

        # Fuzzy matching logic
        match, score = fuzzy_process.extractOne(location, COUNTRY_NAMES)
        
        # If the score is above a threshold (e.g., 85), consider it a match
        if score > 85:
            yield {**element, 'country': match, 'matched': True}
        else:
            # Mark the record as unmatched to be processed later by geocoding
            yield {**element, 'country': None, 'matched': False}



class GeocodeLocation(beam.DoFn):
    def __init__(self):
        self.geolocator = None

    def setup(self):
        self.geolocator = OpenCage(api_key= os.getenv("API_KEY"), user_agent="my_geocoder")

    def process(self, element):
        # Skip geocoding if already matched by fuzzy matching
        if element.get('matched'):
            yield element
            return

        location = element.get('location', '')
        if not location:
            yield {**element, 'country': ""}
            return

        try:
            loc = self.geolocator.geocode(location)
            country = loc.raw['components'].get('country') if loc else np.nan
            yield {**element, 'country': country, 'matched': True}
        except Exception as e:
            logging.error(f"Error geocoding location '{location}': {e}")
            yield {**element, 'country': np.nan, 'matched': False}


