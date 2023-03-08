# -*- coding: utf-8 -*-

import os
import time
from datetime import datetime, timezone

from scrapy import Selector
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

import utils


class HandelsRegister:
    def __init__(self, search_query: {}, link_type: "AD"):
        if search_query is None:
            raise ValueError
        self.document_type = link_type
        self.register_types = search_query["register_types"]
        self.register_options = set()
        self.register_options = search_query["register_options"]
        self.streets = search_query["streets"]
        self.cities = search_query["cities"]
        self.postal_codes = set()
        self.postal_codes = search_query["postal_codes"]
        self.keywords = search_query["keywords"]
        self.keywords_match_option = search_query["keywords_match_option"]
        self.keywords_similar_sounding = search_query["keywords_similar_sounding"]

    def get_driver(self):
        opt = webdriver.ChromeOptions()
        opt.add_argument("--start-maximized")
        preferences = {
            "download.default_directory": os.path.join(os.getcwd(), f'handelsregister.de/{self.document_type}'),
            "safebrowsing.enabled": "false", "plugins.always_open_pdf_externally": "true",
            "profile.default_content_setting_values.automatic_downloads": 1}
        opt.add_experimental_option("prefs", preferences)
        driver = webdriver.Chrome(ChromeDriverManager().install(), chrome_options=opt)
        return driver

    @staticmethod
    def __scroll_down_page(driver):
        current_scroll_position, new_height = 0, 1
        while current_scroll_position <= new_height:
            current_scroll_position += 40
            driver.execute_script("window.scrollTo(0, {});".format(current_scroll_position))
            new_height = driver.execute_script("return document.body.scrollHeight")

    def start_request(self):
        print("keyword:", self.keywords)
        keywords_match_option = self.get_keyword_match_option()
        print("keywords_match_option:", keywords_match_option)

        csv_postal_code = utils.read_postal_code_csv_file()

        for register_type in self.get_register_types():
            print("register_type:", register_type)
            for city in self.get_cities(csv_postal_code):
                print("city:", city)
                for postal_code in self.get_postal_codes(csv_postal_code, city):
                    print("postal_code:", postal_code)
                    for register_option in self.get_register_options(register_type):
                        print("register_option:", register_option)
                        # for street in utils.get_streets(postal_code):
                        # print("street:", street)

                        now = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
                        query_cache_id = register_type + "-" + city + "-" + postal_code + "-" + register_option
                        if self.keywords is not None and len(self.keywords) > 0:
                            query_cache_id = query_cache_id + '-' + str(keywords_match_option) + '-' + self.keywords
                        query_cache_id = utils.replace_german_chars(query_cache_id).lower().replace(' ', '-')
                        query_caching_date = utils.read_query_cache(query_cache_id)

                        if query_caching_date is not None:
                            print(f"{query_cache_id} cached at ", query_caching_date)
                            continue

                        driver = self.get_driver()
                        driver.maximize_window()
                        time.sleep(1)
                        driver.get('https://www.handelsregister.de/rp_web/erweitertesuche.xhtml')
                        time.sleep(5)

                        if self.keywords is not None and len(self.keywords) > 0:
                            WebDriverWait(driver, 30).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, '[id="form:schlagwoerter"]')))

                            time.sleep(1)
                            driver.find_element(By.CSS_SELECTOR, '[id="form:schlagwoerter"]').send_keys(self.keywords)

                            time.sleep(1)
                            driver.find_element(By.CSS_SELECTOR,
                                                f'label[for="form:schlagwortOptionen:{keywords_match_option}"]').click()

                        WebDriverWait(driver, 30).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, '[id="form:registerNummer"]')))
                        self.__scroll_down_page(driver)

                        time.sleep(1)
                        driver.find_element(By.CSS_SELECTOR, '[id="form:registerArt_label"]').click()

                        time.sleep(1)
                        driver.find_element(By.CSS_SELECTOR, f'li[data-label="{register_type}"]').click()

                        time.sleep(1)
                        driver.find_element(By.CSS_SELECTOR, '[id="form:rechtsform_label"]').click()

                        time.sleep(1)
                        driver.find_element(By.CSS_SELECTOR, f'li[data-label="{register_option}"]').click()

                        time.sleep(1)
                        driver.find_element(By.CSS_SELECTOR, '[id="form:postleitzahl"]').send_keys(postal_code)

                        time.sleep(1)
                        driver.find_element(By.CSS_SELECTOR, '[id="form:ort"]').send_keys(city)

                        # time.sleep(0.5)
                        # driver.find_element(By.CSS_SELECTOR, '[id="form:strasse"]').send_keys(street)

                        time.sleep(1)
                        driver.find_element(By.CSS_SELECTOR, '[id="form:ergebnisseProSeite_label"]').click()

                        time.sleep(1)
                        driver.find_element(By.CSS_SELECTOR, f'li[data-label="100"]').click()

                        time.sleep(1)
                        driver.find_element(By.CSS_SELECTOR, f'[id="form:btnSuche"]').click()

                        time.sleep(7)
                        WebDriverWait(driver, 40).until(EC.presence_of_element_located(
                            (By.CSS_SELECTOR, 'div[id="ergebnissForm:selectedSuchErgebnisFormTable"]')))
                        # self.__scroll_down_page(driver)

                        response = Selector(text=driver.page_source)
                        company_rows = response.css('.borderBottom3')

                        print("number of companies found:", len(company_rows))

                        if len(company_rows) == 0:
                            utils.cache_query(query_cache_id, now)
                            print("---- no company found. cache it and skip it ----")
                            driver.quit()
                            continue

                        for row_data in company_rows:
                            register_references = ' '.join(
                                [row.strip() for row in row_data.css('td.fontTableNameSize *::text').getall() if
                                 row.strip() != ''])

                            company_caching_id = utils.clean_company_id(
                                register_references.strip().replace(' ', '-').lower())
                            company_caching_date = utils.read_company_cache(
                                company_caching_id + '-' + self.document_type)

                            if company_caching_date is None:

                                result_table_id = row_data.css('table::attr(id)').getall().pop()

                                try:
                                    time.sleep(5)
                                    driver.execute_script("arguments[0].scrollIntoView(true);",
                                                          driver.find_element(By.XPATH,
                                                                              f'//table[@id="{result_table_id}"]//span[contains(text(), "{self.document_type}")]/..'))
                                    time.sleep(5)
                                    driver.find_element(By.XPATH,
                                                        f'//table[@id="{result_table_id}"]//span[contains(text(), "{self.document_type}")]/..').click()

                                    time.sleep(3)
                                    WebDriverWait(driver, 30).until(
                                        EC.presence_of_element_located(
                                            (By.CSS_SELECTOR, '[id="form:kostenpflichtigabrufen"]')))
                                    driver.find_element(By.CSS_SELECTOR, '[id="form:kostenpflichtigabrufen"]').click()

                                    time.sleep(5)

                                    driver.back()

                                    WebDriverWait(driver, 30).until(EC.presence_of_element_located(
                                        (By.CSS_SELECTOR, 'div[id="ergebnissForm:selectedSuchErgebnisFormTable"]')))
                                    time.sleep(1)
                                    self.__scroll_down_page(driver)
                                    utils.cache_company(company_caching_id + '-' + self.document_type, now)

                                except Exception as ex:
                                    print('error', ex)
                                    sel = Selector(text=driver.page_source)
                                    if sel.css('[id="form:kostenpflichtigabrufen"]').get():
                                        driver.back()

                                        WebDriverWait(driver, 30).until(EC.presence_of_element_located((
                                            By.CSS_SELECTOR,
                                            'div[id="ergebnissForm:selectedSuchErgebnisFormTable"]')))
                                        time.sleep(5)
                                        self.__scroll_down_page(driver)
                                    else:
                                        time.sleep(5)
                                        pass

                            else:
                                print(f"{company_caching_id} found in cache:", company_caching_date)
                                continue

                        time.sleep(20)
                        driver.quit()

    def get_register_types(self) -> set:
        register_types_catalog = {"HRA", "HRB", "GnR", "PR", "VR"}

        if self.register_types is None or len(self.register_types) == 0:
            return register_types_catalog

        # ensure that register_types has only valid values
        invalid_register_types = self.register_types.difference(register_types_catalog)

        if len(invalid_register_types) > 0:
            raise Exception('valid register types are: "HRA", "HRB", "GnR", "PR" and "VR"')

        if len(self.register_types) == len(register_types_catalog):
            return {"all"}

        return self.register_types

    def get_register_options(self, register_type_filter: str) -> set:
        if register_type_filter == "VR":
            return {"all"}
        register_options = utils.get_register_options(register_type_filter)
        if self.register_options is not None and len(self.register_options) > 0:
            intersection_options = self.register_options.intersection(register_options)
            return intersection_options
        return register_options

    def get_cities(self, csv_file_content) -> set:
        collected_cities = set()
        if self.cities is None or len(self.cities) == 0:
            for csv_dict in csv_file_content[:]:
                item = csv_dict.get('ort')
                collected_cities.add(item)
            return collected_cities
        return self.cities

    def get_postal_codes(self, csv_file_content, city_filter: str) -> set:
        postal_codes_set = set()
        for csv_dict in csv_file_content[:]:
            postal_code_item = csv_dict.get('plz')
            city_item = csv_dict.get('ort')
            if postal_code_item is not None and city_item is not None and utils.replace_german_chars(city_item.strip()).lower() == utils.replace_german_chars(city_filter.strip()).lower() and (postal_code_item in self.postal_codes or len(self.postal_codes) == 0):
                postal_codes_set.add(postal_code_item)
                print(postal_code_item)
        return postal_codes_set

    def get_streets(self, postal_code_filter: str) -> set:
        all_streets = utils.get_streets(postal_code_filter)
        if self.streets is not None and len(self.streets) > 0:
            return self.streets.intersection(all_streets)
        return all_streets

    def get_keyword_match_option(self) -> int:
        if self.keywords_match_option is None or self.keywords_match_option == "one":
            return 1
        if self.keywords_match_option == "all":
            return 0
        if self.keywords_match_option == "exact":
            return 2
        raise Exception('valid keywords options are: "all", "one" and "exact"')

    def query_has_no_result(self) -> bool:
        return False

    def query_completed_successfully(self) -> bool:
        return False


if __name__ == '__main__':
    file_type = "SI"

    query = {"register_types": {"HRB", "HRA"},
             "register_options": {"Kommanditgesellschaft", "Aktiengesellschaft", "Europäische Aktiengesellschaft (SE)",
                                  "Gesellschaft mit beschränkter Haftung"},
             "cities": {"Mannheim", "Ludwigshafen"},
             "streets": {},
             # "keywords": 'Logistik Logistics Transport Spedition Cargo',
             "keywords": 'Chemie Pharma Abbvie BASF Bayer',
             "keywords_match_option": "one",
             "keywords_similar_sounding": False,
             "postal_codes": {}}

    obj = HandelsRegister(query, file_type)
    obj.start_request()
