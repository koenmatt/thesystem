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
warnings.filterwarnings('ignore')

parser = argparse.ArgumentParser(description= 'Gathers related terms for a parent phrase.')
parser.add_argument('--parent')


def get_related(parent_phrase):
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

    r = requests.get(url)

    i = 1
    while r.status_code != 200 and i <= 10:
        random_time = random.randint(1800) * i
        print("-" * 20)
        print(f"Temporarily blocked - trying again in {random_time} seconds")
        print(f"Attempt: {i}/10")
        print("-" * 20)

        time.sleep(random_time)
        r = requests.get(url)
        i += 1

    if r.status_code != 200:
        raise Exception("Scraper Aborted - over 10 requests")

    soup = BeautifulSoup(r.content, "html.parser")
    related = [i.get_text() for i in soup.find_all("div", class_ = "BNeawe s3v9rd AP7Wnd lRVwie")]
    alsoAsk = [i.get_text() for i in soup.find_all("div", class_= "Lt3Tzc")]
    if len(related) == 0:
        print(f"Error: No related terms for parent phrase: {parent_phrase}")
        return None
    return [related, alsoAsk]


def run_nested_batch(term_list):
    """input: list(str) of terms

        Gathers data for each nested related term

    """
    counter = 0
    relatedBatch = []
    alsoAskBatch = []

    for term in term_list:
        item = get_related(term)
        if not item:
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


def remove_duplicates(a_list):
    """Removes duplicate terms from a list"""
    return list(set(a_list))


def clean_terms(term_list, category, parent_phrase):
    df_terms = pd.DataFrame(term_list)
    df_terms.columns = ['Related Term']
    df_terms['Category'] = category
    df_terms['Parent Phrase'] = parent_phrase


    df_terms.loc[-1] = [parent_phrase, category, parent_phrase]  # adding a row
    df_terms.index = df_terms.index + 1  # shifting index
    df_terms = df_terms.sort_index()
    return df_terms

def run_2_batch(parent_phrase, category):
    print("-----STARTING FIRST BATCH-----")

    start = get_related(parent_phrase)

    related_terms = start[0]
    alsoAsk = start[1]
    first_batch = run_nested_batch(related_terms)

    if not first_batch:
        return None

    first_batch_terms = first_batch[0]
    first_batch_alsoAsk = first_batch[1]


    first_batch_terms_no_duplicates = remove_duplicates(first_batch_terms)

    print("-----STARTING SECOND BATCH-----")
    second_batch = run_nested_batch(first_batch_terms_no_duplicates)

    second_batch_terms = second_batch[0]
    second_batch_alsoAsk = second_batch[1]


    combined_terms = remove_duplicates(first_batch_terms_no_duplicates + second_batch_terms)
    combined_alsoAsk = remove_duplicates(second_batch_alsoAsk + first_batch_alsoAsk + alsoAsk)


    df_terms = clean_terms(combined_terms, category, parent_phrase)
    df_alsoAsk = clean_terms(combined_alsoAsk, category, parent_phrase)

    today = date.today()
    todays_date = today.strftime("%b_%d_%Y")

    df_terms.to_csv(f"{todays_date}_TERMS_ParentPhrase={parent_phrase}.csv")
    df_alsoAsk.to_csv(f"{todays_date}_ASLO_ASK_ParentPhrase={parent_phrase}.csv")

    return [df_terms, df_alsoAsk]


def main(parent_path):
    parent_phrases = pd.read_csv(parent_path)
    for phrase in parent_phrases:
        print(f"Scraping Parent Phrase: {phrase}")
        run_2_batch(phrase, "students")
        time.sleep(1000)


if __name__ == '__main__':
    args = parser.parse_args()
    if not args.parent:
        raise AttributeError("No list of phrases")
    main(args.parent)
