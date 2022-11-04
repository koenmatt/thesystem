#######################################
#   Install and Import Dependencies   #
#######################################

# pip install requests
# pip install numpy
# pip install pandas
# pip install bs4

import requests
import pandas as pd
from bs4 import BeautifulSoup
import random
import time
from datetime import date
import argparse
import warnings
import string
import os
import glob

warnings.filterwarnings('ignore')
parser = argparse.ArgumentParser(description= 'Gathers related terms for a parent phrase.')
parser.add_argument('--parent')
parser.add_argument('--cat')


class Scraper:

    def __init__(self):
        self.parent_index = 0

    def update_index(self):
        self.parent_index += 1

    def send_request(self, url):
        try:
            r = requests.get(url)
        except requests.exceptions.RequestException:
            print("Requests Error...waiting for 30 min")
            time.sleep(1800)
            try:
                r = requests.get(url)
            except requests.exceptions.RequestException:
                print("Requests Error...waiting for 1 hour")
                time.sleep(3600)
                try:
                    r = requests.get(url)
                except requests.exceptions.RequestException:
                    return None
        i = 1
        while r.status_code != 200 and i <= 10:
            random_time = 300 * i
            print("-" * 20)
            print(f"Temporarily blocked - trying again in {random_time} seconds")
            print(f"Attempt: {i}/10")
            print("-" * 20)

            time.sleep(random_time)
            r = requests.get(url)
            print(r.status_code)
            i += 1
        if r.status_code != 200:
            return None
        return r

    def get_related(self, parent_phrase):
        """input (str): parent term from Dave

        output: A list of length 2 with -
                    * element 0 - a list of the parent phrases 8 related searches
                    * element 1 - a list of 'Also Asked' via parent phrase

        """

        parsed_phrase = parent_phrase.split(" ")
        search_term = "+".join(parsed_phrase)

        url = f"https://www.google.com/search?q={search_term}&rlz=1C5CHFA_enUS903US903&oq={search_term}&" \
              f"aqs=chrome.0.69i59l3j0i10i131i433i512j0i512l2j0i10i512j69i60.1632j0j9&sourceid=chrome&ie=UTF-" \
              f"8&cr=countryUS"  # retrieves data for US, not local

        r = self.send_request(url)
        i = 1
        while not r and i <= 10:
            time.sleep(3600 * i)
            r = self.send_request(url)

        if not r:
            raise Exception(f"Scraper Aborted on phrase {parent_phrase}...DEEP ERROR")

        soup = BeautifulSoup(r.content, "html.parser")
        related = [i.get_text() for i in soup.find_all("div", class_ = "BNeawe s3v9rd AP7Wnd lRVwie")]
        alsoAsk = [i.get_text() for i in soup.find_all("div", class_= "Lt3Tzc")]

        if len(related) == 0:
            print(f"Error: No related terms for parent phrase: {parent_phrase}")
            return None
        return [related, alsoAsk]

    def run_nested_batch(self, term_list):

        counter = 0
        relatedBatch = []
        alsoAskBatch = []

        for term in term_list:
            item = self.get_related(term)
            if not item:
                continue

            if not item[0] or not item[1]:
                continue

            relatedBatch += item[0]
            alsoAskBatch += item[1]
            print(term)
            counter += 1

            # Randomly wait around a minute every 6 to 12 requests
            if not counter % random.randint(6, 12):
                wait_time = random.randint(60, 100)
                time.sleep(wait_time)
                print(f"random pause: {wait_time} seconds")
            else:
                time.sleep(random.randint(2, 8))

        return [relatedBatch, alsoAskBatch]

    def remove_duplicates(self, a_list):
        """Removes duplicate terms from a list"""
        return list(set(a_list))

    def clean_terms(self, term_list, category, parent_phrase, alsoAsk):
        df_terms = pd.DataFrame(term_list)
        df_terms.columns = ['Related Term']
        df_terms['Category'] = category
        df_terms['Parent Phrase'] = parent_phrase
        df_terms['alsoAsk'] = alsoAsk


        df_terms.loc[-1] = [parent_phrase, category, parent_phrase, alsoAsk]  # adding a row
        df_terms.index = df_terms.index + 1  # shifting index
        df_terms = df_terms.sort_index()
        return df_terms

    def run_2_batch(self, parent_phrase, category):
        print("-----STARTING FIRST BATCH-----")

        start = self.get_related(parent_phrase)
        if not start:
            time.sleep(3600)
            start = self.get_related(parent_phrase)
        if not start[0] or not start[1]:
            time.sleep(3600)
            start = self.get_related(parent_phrase)

        related_terms = start[0]
        alsoAsk = start[1]
        first_batch = self.run_nested_batch(related_terms)

        if not first_batch:
            time.sleep(3600)
            first_batch = self.run_nested_batch(related_terms)
        if not first_batch[0] or not first_batch[1]:
            time.sleep(3600)
            first_batch = self.run_nested_batch(related_terms)

        first_batch_terms = first_batch[0]
        first_batch_alsoAsk = first_batch[1]


        first_batch_terms_no_duplicates = self.remove_duplicates(first_batch_terms)

        print("-----STARTING SECOND BATCH-----")
        second_batch = self.run_nested_batch(first_batch_terms_no_duplicates)

        second_batch_terms = second_batch[0]
        second_batch_alsoAsk = second_batch[1]

        combined_terms = self.remove_duplicates(first_batch_terms_no_duplicates + second_batch_terms)
        combined_alsoAsk = self.remove_duplicates(second_batch_alsoAsk + first_batch_alsoAsk + alsoAsk)

        df_terms = self.clean_terms(combined_terms, category, parent_phrase, False)
        df_alsoAsk = self.clean_terms(combined_alsoAsk, category, parent_phrase, True)

        today = date.today()
        todays_date = today.strftime("%b_%d_%Y")

        parsed_phrase = parent_phrase.split(" ")
        search_term = "".join(parsed_phrase)
        search_term = search_term.translate(str.maketrans("", "", string.punctuation))


        df_terms.to_csv(os.getcwd() + f"\csv\c{todays_date}_TERMS_ParentPhrase={search_term}.csv")
        df_alsoAsk.to_csv(os.getcwd() + f"\csv\c{todays_date}_ASLO_ASK_ParentPhrase={search_term}.csv")

        master = pd.read_csv("master.csv")
        ind = list(master.index[master['Parent Phrase'] == parent_phrase])[0]
        master.at[ind, 'Number of Unique Related Searches Found'] = len(df_terms)
        master.at[ind, 'Number of Unique People Also Ask Found'] = len(df_alsoAsk)
        master.at[ind, 'Harvested successfully'] = True
        master.to_csv("master.csv")

        return [df_terms, df_alsoAsk]


########################################################################################################################
########################################################################################################################


from google.ads.googleads.client import GoogleAdsClient
import sys
from google.ads.googleads.errors import GoogleAdsException

class googleAPI:
    def __init__(self, date, category):
        try:
            self.G_client = GoogleAdsClient.load_from_storage("requirements.yaml")
        except google.auth.exceptions.RefreshError:
            raise Exception("YAML file not up to date. Need new refresh token. See refresh.py")
        self.date = date
        self.category = category
        self.data = pd.read_csv(f"{date}_merged_CAT={category}.csv")
        self._DEFAULT_LANGUAGE_ID = "1000"  # set to English
        self._DEFAULT_CUSTOMER_ID = "2095457043"

    def _map_locations_ids_to_resource_names(self, client, location_ids):
        """Converts a list of location IDs to resource names.
        Args:
            client: an initialized GoogleAdsClient instance.
            location_ids: a list of location ID strings.
        Returns:
            a list of resource name strings using the given location IDs.
        """
        build_resource_name = client.get_service(
            "GeoTargetConstantService"
        ).geo_target_constant_path
        return [build_resource_name(location_id) for location_id in location_ids]



    def main(self, client, keyword_texts, page_url=None):

        keyword_plan_idea_service = client.get_service("KeywordPlanIdeaService")
        keyword_plan_network = client.get_type("KeywordPlanNetworkEnum").KeywordPlanNetwork.GOOGLE_SEARCH_AND_PARTNERS
        keyword_annotation = client.enums.KeywordPlanKeywordAnnotationEnum

        # Either keywords or a page_url are required to generate keyword ideas
        # so this raises an error if neither are provided.
        if not (keyword_texts or page_url):
            raise ValueError(
                "At least one of keywords or page URL is required, "
                "but neither was specified."
            )

        request = client.get_type("GenerateKeywordIdeasRequest")
        request.customer_id = self._DEFAULT_CUSTOMER_ID
        request.include_adult_keywords = False
        request.keyword_plan_network = keyword_plan_network
        request.keyword_annotation = keyword_annotation

        # To generate keyword ideas with only a list of keywords and no page_url
        # we need to initialize a KeywordSeed object and set the "keywords" field
        # to be a list of StringValue objects.
        if keyword_texts and not page_url:
            request.keyword_seed.keywords.extend(keyword_texts)

        # To generate keyword ideas using both a list of keywords and a page_url we
        # need to initialize a KeywordAndUrlSeed object, setting both the "url" and
        # "keywords" fields.
        if keyword_texts and page_url:
            request.keyword_and_url_seed.url = page_url
            request.keyword_and_url_seed.keywords.extend(keyword_texts)

        keyword_ideas = keyword_plan_idea_service.generate_keyword_ideas(
            request=request
        )

        list_keywords = []
        for idea in keyword_ideas:
            competition_value = idea.keyword_idea_metrics.competition.name
            list_keywords.append(idea)

        return list_keywords

    def google_to_df(self, google_out):
        list_to_excel = []
        for x in range(len(google_out)):
            list_months = []
            list_searches = []
            list_annotations = []
            for y in google_out[x].keyword_idea_metrics.monthly_search_volumes:
                list_months.append(str(y.month)[12::] + " - " + str(y.year))
                list_searches.append(y.monthly_searches)

            for y in google_out[x].keyword_annotations.concepts:
                list_annotations.append(y.concept_group.name)

            list_to_excel.append([google_out[x].text, google_out[x].keyword_idea_metrics.avg_monthly_searches,
                                  str(google_out[x].keyword_idea_metrics.competition)[28::],
                                  google_out[x].keyword_idea_metrics.competition_index,
                                  list_searches, list_months, list_annotations])

        df = pd.DataFrame(list_to_excel)
        df.columns = ['Keyword', 'Average Searches', 'Competition Level', 'Competition Index', 'Searches Past Months', 'Past Months', 'List Annotations']
        return df



    def split_terms(self, n, l):
        return [l[i:i + n] for i in range(0, len(l), n)]

    def run_googleAPI(self):
        master = pd.DataFrame()

        terms = self.split_terms(5, self.data['Related Term'])

        counter = 1
        for term in terms:
            print(f"{counter} of {len(self.data['Related Term'])}")
            counter += 1
            try:
                data = self.main(self.G_client, term)
            except google.auth.exceptions.ResourceCountLimitExceededError:
                print("Too many requests, sleeping for 30 minutes")
                time.sleep(1800)
                data = self.main(self.G_client, [term])

            if not data:
                continue

            google_data = self.google_to_df(data)
            random_time = random.randint(8, 12)


            if not counter % random_time:
                time.sleep(random_time)
                print(f"random wait time:{random_time}")
            time.sleep(5)
            print(term)
            if not counter % 30:
                print("sleeping for 1 minute")
                time.sleep(60)
            aboveThreshold = google_data[google_data['Average Searches'] >= 1000]
            aboveThreshold['Parent Phrase'] = str(self.data[self.data['Related Term'] == term]['Parent Phrase'].iloc[0])
            aboveThreshold['Category'] = str(self.data[self.data['Related Term'] == term]['Category'].iloc[0])
            master = pd.concat([master, aboveThreshold])

        final_test = master.sort_values(by=['Average Searches'], ascending = False)
        months = final_test['Past Months'].iloc[0]
        for i in range(12):
            final_test[months[i]] = final_test['Searches Past Months'].str[i]
        final_test = final_test.drop(['Searches Past Months', 'Past Months'], axis = 1)
        toDave = final_test.reset_index().drop(['index'], axis = 1)
        today = date.today()
        todays_date = today.strftime("%b_%d_%Y")

        toDave.to_csv(f"{self.date}_googleAPI_CAT={self.category}.csv")

        return toDave



def main(parent_path, category):
    parent_phrases = pd.read_csv(parent_path)
    for phrase in parent_phrases["Parent Phrase"]:
        print(f"Scraping Parent Phrase: {phrase}")
        s = Scraper()
        try:
            s.run_2_batch(phrase, category)
        except:
            time.sleep(3600)
            s.run_2_batch(phrase, category)
        time.sleep(1000)
        s.update_index()

    files = glob.glob(os.getcwd() + "\csv\*")

    df = pd.concat(map(pd.read_csv, files), ignore_index=True)

    today = date.today()
    todays_date = today.strftime("%b_%d_%Y")

    df.to_csv(f"{todays_date}_merged_CAT={category}.csv")

    print("SUCCESSFUL GOOGLE SCRAPE")
    print("~"* 20)
    print(f"Stored as: {todays_date}_merged_CAT={category}.csv")
    print("Proceeding to google API connection...")

    g = googleAPI(todays_date, category)
    g.run_googleAPI()


if __name__ == '__main__':
    args = parser.parse_args()
    if not args.parent:
        raise AttributeError("No list of phrases")
    main(args.parent, args.cat)
