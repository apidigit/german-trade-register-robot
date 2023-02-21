import re
import csv

from watchfiles import Change
import os

DOWNLOADS_FOLDER_PATH = os.path.join(os.getcwd(), 'downloads')

CREATE_COMPANY_TABLE = '''
                CREATE TABLE IF NOT EXISTS companies( 
                    `id` VARCHAR(255) NOT NULL PRIMARY KEY,
                    `register_references` VARCHAR(255)  NOT NULL,
                    `name` VARCHAR(255) NOT NULL, 
                    `headquarter_city` VARCHAR(255) NOT NULL, 
                    `headquarter_postal_code` VARCHAR(15), 
                    `headquarter_street` VARCHAR(255),
                    `headquarter_address_supplement` VARCHAR(255), 
                    `currently_registered` BOOLEAN, 
                    `business_purpose` VARCHAR(5000), 
                    `share_capital_amount` VARCHAR(15), 
                    `share_capital_currency` VARCHAR(15), 
                    `incorporation_date` VARCHAR(255), 
                    `last_registry_update` VARCHAR(255), 
                    `phone` VARCHAR(255),
                    `mobile` VARCHAR(255),
                    `create_date_time` VARCHAR(35) NOT NULL,
                    `last_update_date_time` VARCHAR(35));'''

CREATE_CEO_TABLE = '''
                CREATE TABLE IF NOT EXISTS company_ceos( 
                    `id` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    `company_id` VARCHAR(255) NOT NULL,
                    `titel` VARCHAR(15) NULL, 
                    `gender` VARCHAR(15) NULL, 
                    `first_name` VARCHAR(55), 
                    `last_name` VARCHAR(55) NOT NULL, 
                    `birth_name` VARCHAR(255), 
                    `residence_city` VARCHAR(255), 
                    `birthdate` VARCHAR(15), 
                    `create_date_time` VARCHAR(35) NOT NULL,
                    `last_update_date_time` VARCHAR(35),
                    FOREIGN KEY (`company_id`) REFERENCES companies(`id`),
                    CONSTRAINT `uc_ceo` UNIQUE (`company_id`, `first_name`, `last_name`));'''

CREATE_HISTORY_TABLE = '''
                CREATE TABLE IF NOT EXISTS company_histories(
                    `id` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    `company_id` VARCHAR(255) NOT NULL,
                    `position` int NOT NULL, 
                    `name` VARCHAR(255) NOT NULL, 
                    `city` VARCHAR(255) NOT NULL,
                    `create_date_time` VARCHAR(35) NOT NULL,
                    `last_update_date_time` VARCHAR(35),
                    FOREIGN KEY (`company_id`) REFERENCES companies(`id`),
                    CONSTRAINT `uc_history` UNIQUE (`company_id`, `name`, `city`));'''

CREATE_PROCURA_TABLE = '''
                CREATE TABLE IF NOT EXISTS company_procura( 
                    `id` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    `company_id` VARCHAR(255) NOT NULL, 
                    `titel` VARCHAR(15) NULL, 
                    `gender` VARCHAR(15) NULL, 
                    `first_name` VARCHAR(55), 
                    `last_name` VARCHAR(55) NOT NULL, 
                    `email` VARCHAR(255),
                    `mobile` VARCHAR(255),
                    `phone` VARCHAR(255),
                    `create_date_time` VARCHAR(35) NOT NULL,
                    `last_update_date_time` VARCHAR(35),
                    FOREIGN KEY (`company_id`) REFERENCES companies(`id`),
                    CONSTRAINT `uc_procura` UNIQUE (`company_id`, `first_name`, `last_name`));'''

CREATE_LOCATION_TABLE = '''
                CREATE TABLE IF NOT EXISTS company_locations( 
                    `id` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    `company_id` VARCHAR(255) NOT NULL,
                    `city` VARCHAR(255) NOT NULL, 
                    `postal_code` VARCHAR(15) NOT NULL, 
                    `street` VARCHAR(255) NOT NULL,
                    `address_supplement` VARCHAR(255),
                    `email` VARCHAR(255),
                    `phone` VARCHAR(255),
                    `mobile` VARCHAR(255),
                    `create_date_time` VARCHAR(35) NOT NULL,
                    `last_update_date_time` VARCHAR(35),
                    FOREIGN KEY (`company_id`) REFERENCES companies(`id`),
                    CONSTRAINT `uc_location` UNIQUE (`company_id`, `city`, `postal_code`, `street`));'''

CREATE_CONTACT_TABLE = '''CREATE TABLE IF NOT EXISTS company_contacts( `id` BIGINT NOT NULL AUTO_INCREMENT 
            PRIMARY KEY, `company_id` VARCHAR(255) NOT NULL, `titel` VARCHAR(15) NULL, `gender` VARCHAR(15) NULL, 
            `first_name` VARCHAR(55), `last_name` VARCHAR(55) NOT NULL, `position` VARCHAR(255), `location` BIGINT 
            NULL, `office_email` VARCHAR(255), `private_email` VARCHAR(255), `office_mobile` VARCHAR(255), 
            `office_phone` VARCHAR(255), `private_mobile` VARCHAR(255), `private_phone` VARCHAR(255), 
            `create_date_time` VARCHAR(35) NOT NULL, `last_update_date_time` VARCHAR(35), FOREIGN KEY (`company_id`) 
            REFERENCES companies(`id`), FOREIGN KEY (`location`) REFERENCES company_locations(`id`), CONSTRAINT 
            `uc_contact` UNIQUE (`company_id`, `first_name`, `last_name`, `office_email`));'''


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


# def clean_cache_filename(file_path):
#     temp_file_name = file_path.replace(DOWNLOADS_FOLDER_PATH + '/', '')
#     temp_file_name = re.sub('-[0-9]{14}', '', temp_file_name).lower()
#     return temp_file_name.split('/')[-1].replace('.', '-').replace('_', '-').replace('+', '-').strip() + '.txt'


def get_streets(postal_code) -> set:
    with open(f"plzplz.de/streets/{postal_code}.txt", "r", encoding='utf-8') as file:
        return set([street.strip() for street in file.readlines()])


def read_postal_code_csv_file() -> list:
    return list(csv.DictReader(open('resources/postal_codes.csv', 'r', encoding='utf-8')))


def get_register_options(register_type: str) -> set:
    with open(f"resources/{register_type.lower()}_options_germany.txt", "r", encoding='utf-8') as file:
        return set([option.strip() for option in file.readlines()])


def cache_street_html(postal_code, response):
    with open(f"plzplz.de/cache/{postal_code}.html", "w", encoding='utf-8') as file:
        file.write(response)


def write_street_results(postal_code, response):
    with open(f"plzplz.de/streets/{postal_code}.txt", "w", encoding='utf-8') as file:
        for street in response:
            file.write(f"{street}\n")


def read_file(file_path):
    with open(file_path, "r", encoding='utf-8') as file:
        return file.read()


def cache_company(company_id, now):
    with open(f"handelsregister.de/cache/companies/{company_id}.txt", "w", encoding='utf-8') as file:
        file.write(f"{now}")


def cache_query(cache_id, now):
    with open(f"handelsregister.de/cache/queries/{cache_id}.txt", "w", encoding='utf-8') as file:
        file.write(f"{now}")


def read_company_cache(company_id):
    file_name = f"handelsregister.de/cache/companies/{company_id}.txt"
    if not os.path.exists(file_name):
        return None
    with open(file_name, "r", encoding='utf-8') as file:
        return file.readline()


def read_query_cache(company_id):
    file_name = f"handelsregister.de/cache/queries/{company_id}.txt"
    if not os.path.exists(file_name):
        return None
    with open(file_name, "r", encoding='utf-8') as file:
        return file.readline()
