

# pip install google-ads

import requests
import pandas as pd
import numpy as np
from google.ads.googleads.client import GoogleAdsClient
import argparse
import sys
from google.ads.googleads.errors import GoogleAdsException
from datetime import date
import random

G_client = GoogleAdsClient.load_from_storage("requirements.yaml")
_DEFAULT_LANGUAGE_ID = "1000" #set to English
_DEFAULT_CUSTOMER_ID = "2095457043"

def _map_locations_ids_to_resource_names(client, location_ids):
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

def main(client, keyword_texts, page_url=None, customer_id = _DEFAULT_CUSTOMER_ID, language_id=_DEFAULT_LANGUAGE_ID):

    keyword_plan_idea_service = client.get_service("KeywordPlanIdeaService")

    keyword_competition_level_enum = client.get_type("KeywordPlanCompetitionLevelEnum").KeywordPlanCompetitionLevel

    keyword_plan_network = client.get_type("KeywordPlanNetworkEnum").KeywordPlanNetwork.GOOGLE_SEARCH_AND_PARTNERS

    #     location_rns = _map_locations_ids_to_resource_names(client, location_ids)

    #     language_rn = client.get_service("LanguageConstants").language_constant_path(language_id)

    keyword_annotation = client.enums.KeywordPlanKeywordAnnotationEnum

    # Either keywords or a page_url are required to generate keyword ideas
    # so this raises an error if neither are provided.
    if not (keyword_texts or page_url):
        raise ValueError(
            "At least one of keywords or page URL is required, "
            "but neither was specified."
        )


    # Only one of the fields "url_seed", "keyword_seed", or
    # "keyword_and_url_seed" can be set on the request, depending on whether
    # keywords, a page_url or both were passed to this function.
    request = client.get_type("GenerateKeywordIdeasRequest")
    request.customer_id = customer_id
    #     request.language = "en"
    #     request.geo_target_constants = location_rns
    request.include_adult_keywords = False
    request.keyword_plan_network = keyword_plan_network
    request.keyword_annotation = keyword_annotation



    # To generate keyword ideas with only a page_url and no keywords we need
    # to initialize a UrlSeed object with the page_url as the "url" field.
    # if not keyword_texts and page_url:
    #     request.url_seed.url = url_seed

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


def google_to_df(google_out):
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


        list_to_excel.append([google_out[x].text, google_out[x].keyword_idea_metrics.avg_monthly_searches, str(google_out[x].keyword_idea_metrics.competition)[28::], google_out[x].keyword_idea_metrics.competition_index, list_searches, list_months, list_annotations ])

    df = pd.DataFrame(list_to_excel)
    df.columns = ['Keyword', 'Average Searches', 'Competition Level', 'Competition Index', 'Searches Past Months', 'Past Months', 'List Annotations']
    return df


def run_all(keyword, export = False, threshold = None):
    """Keyword must be stored in a list ie. ["keyword"]"""
    google_main = main(G_client, keyword)

    df = google_to_df(google_main)


def merge_similar_category(df):
    duplicates = df[df.duplicated(subset=['Related Term'],keep=False)]

    for val in list(duplicates['Related Term']):
        new_val = "/".join(list(df[df['Related Term'] == val]['Parent Phrase']))
        df.loc[df["Related Term"] == val, ['Parent Phrase']] = new_val

    return df.drop_duplicates(subset = "Related Term")


def run_googleAPI(df):
    import warnings
    import time
    warnings.filterwarnings('ignore')

    master = pd.DataFrame()

    df = merge_similar_category(df)
    counter = 1
    for related in df['Related Term']:
        print(f"{counter} of {len(df['Related Term'])}")
        counter += 1
        try:
            data = main(G_client, [related])
        except:
            print("Too many requests, sleeping for 300 seconds")
            time.sleep(300)
            data = main(G_client, [related])
        if len(data) == 0:
            continue
        googleData = google_to_df(data)
        random_time = random.randint(8, 12)
        if counter % random_time == 0:
            time.sleep(random_time)
            print(f"random wait time:{random_time}")
        time.sleep(5)
        print(related)
        if counter % 30 == 0:
            print("sleeping for 1 minute")
            time.sleep(60)
        aboveThreshold = googleData[googleData['Average Searches'] >= 8000]
        aboveThreshold['Parent Phrase'] = str(df[df['Related Term'] == related]['Parent Phrase'].iloc[0])
        aboveThreshold['Category'] = str(df[df['Related Term'] == related]['Category'].iloc[0])
        master = pd.concat([master, aboveThreshold])

    final_test = master.sort_values(by=['Average Searches'], ascending = False)
    months = final_test['Past Months'].iloc[0]
    for i in range(12):
        final_test[months[i]] = final_test['Searches Past Months'].str[i]
    final_test = final_test.drop(['Searches Past Months', 'Past Months'], axis = 1)
    toDave = final_test.reset_index().drop(['index'], axis = 1)
    today = date.today()
    todays_date = today.strftime("%b_%d_%Y")

    almost = list(df['Parent Phrase'].unique())
    new_list = [item for item in almost if "/" not in item]
    final = "_".join(new_list)


    toDave.to_csv(f"{todays_date}terms_with_googleAPI_PARENT_PHRASES={final}.csv")

    return toDave
