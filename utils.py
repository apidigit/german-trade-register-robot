import re
import csv

from watchfiles import Change
import os

DOWNLOADS_FOLDER_PATH = os.path.join(os.getcwd(), 'downloads')


def replace_german_chars(input_string):
    return input_string.replace('ü', 'ue').replace('ö', 'oe').replace('ä', 'ae').replace('ß', 'ss')


def clean_company_id(input_string):
    return replace_german_chars(input_string).replace('(', '').replace(')', '').replace('.', '-').replace('--', '-')


def get_company_id(file_path):
    company_id = file_path.split('_')[-1].split('+')[0]
    return company_id


def added_pdf_filter(change: Change, file_path: str) -> bool:
    return change.value == Change.added and file_path.endswith('.pdf')


def added_xml_filter(change: Change, file_path: str) -> bool:
    return change.value == Change.added and file_path.endswith('.xml')


def clean_cache_filename(file_path):
    temp_file_name = file_path.replace(DOWNLOADS_FOLDER_PATH + '/', '')
    temp_file_name = re.sub('-[0-9]{14}', '', temp_file_name).lower()
    return temp_file_name.split('/')[-1].replace('.', '-').replace('_', '-').replace('+', '-').strip() + '.txt'


def get_streets(postal_code) -> set:
    with open(f"plzplz.de/streets/{postal_code}.txt", "r", encoding='utf-8') as file:
        return set([street.strip() for street in file.readlines()])


def read_postal_code_csv_file() -> list:
    return list(csv.DictReader(open('resources/postal_codes.csv', 'r', encoding='utf-8')))

