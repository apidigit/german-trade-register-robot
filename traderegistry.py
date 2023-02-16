# -*- coding: utf-8 -*-

import json
import os
import time
from datetime import datetime, timezone

import pymysql
from scrapy import Selector
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

import utils
from mysql import db, cursor


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

                        if self.query_has_no_result() or self.query_completed_successfully():
                            continue

                        driver = self.get_driver()
                        driver.get('https://www.handelsregister.de/rp_web/erweitertesuche.xhtml')
                        time.sleep(3)

                        if self.keywords is not None and len(self.keywords) > 0:

                            WebDriverWait(driver, 30).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, '[id="form:schlagwoerter"]')))

                            time.sleep(0.5)
                            driver.find_element(By.CSS_SELECTOR, '[id="form:schlagwoerter"]').send_keys(self.keywords)

                            time.sleep(0.5)
                            driver.find_element(By.CSS_SELECTOR,
                                                f'label[for="form:schlagwortOptionen:{keywords_match_option}"]').click()

                        WebDriverWait(driver, 30).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, '[id="form:registerNummer"]')))
                        self.__scroll_down_page(driver)

                        time.sleep(0.5)
                        driver.find_element(By.CSS_SELECTOR, '[id="form:registerArt_label"]').click()

                        time.sleep(0.5)
                        driver.find_element(By.CSS_SELECTOR, f'li[data-label="{register_type}"]').click()

                        time.sleep(0.5)
                        driver.find_element(By.CSS_SELECTOR, '[id="form:rechtsform_label"]').click()

                        time.sleep(0.5)
                        driver.find_element(By.CSS_SELECTOR, f'li[data-label="{register_option}"]').click()

                        time.sleep(0.5)
                        driver.find_element(By.CSS_SELECTOR, '[id="form:postleitzahl"]').send_keys(postal_code)

                        time.sleep(0.5)
                        driver.find_element(By.CSS_SELECTOR, '[id="form:ort"]').send_keys(city)

                        # time.sleep(0.5)
                        # driver.find_element(By.CSS_SELECTOR, '[id="form:strasse"]').send_keys(street)

                        time.sleep(0.5)
                        driver.find_element(By.CSS_SELECTOR, '[id="form:ergebnisseProSeite_label"]').click()
                        time.sleep(0.5)
                        driver.find_element(By.CSS_SELECTOR, f'li[data-label="100"]').click()
                        time.sleep(0.5)
                        driver.find_element(By.CSS_SELECTOR, f'[id="form:btnSuche"]').click()
                        time.sleep(5)

                        WebDriverWait(driver, 40).until(EC.presence_of_element_located(
                            (By.CSS_SELECTOR, 'div[id="ergebnissForm:selectedSuchErgebnisFormTable"]')))
                        response = Selector(text=driver.page_source)

                        for row_data in response.css('.borderBottom3'):
                            history = row_data.xpath('.//td[contains(text(), "History")]/following::table[1]')
                            history_list = list()

                            register_references = ' '.join(
                                [row.strip() for row in row_data.css('td.fontTableNameSize *::text').getall() if
                                 row.strip() != ''])

                            company_id = utils.clean_company_id(
                                register_references.split('court')[-1].strip().replace(' ', '-').lower())

                            caching_date = utils.read_company_cache(company_id, self.document_type)

                            if caching_date is None:

                                for history_row in history.css('tr.ui-widget-content'):
                                    history_list.append(
                                        {
                                            'position': history_row.css('.RegPortErg_HistorieZn>span::text').re_first(
                                                '\d+'),
                                            'name': history_row.css('.RegPortErg_HistorieZn>span::text').get('').split(
                                                '.)')[-1].strip(),
                                            'city': history_row.css('.RegPortErg_SitzStatus>span::text').get('').split(
                                                '.)')[-1].strip(),
                                        }
                                    )

                                item = {
                                    'register': register_references,
                                    'name': row_data.css('span[class="marginLeft20"]::text').get('').strip(),
                                    'city': row_data.css('.sitzSuchErgebnisse>span::text').get('').strip(),
                                    'currently_registered': True if 'currently registered' in row_data.css(
                                        'td:nth-child(3)>span::text').get('') else False,
                                    'history': json.dumps(history_list)
                                }

                                now = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

                                result_table_id = row_data.css('table::attr(id)').getall().pop()

                                try:
                                    driver.execute_script("arguments[0].scrollIntoView(true);",
                                                          driver.find_element(By.XPATH,
                                                                              f'//table[@id="{result_table_id}"]//span[contains(text(), "{self.document_type}")]/..'))
                                    time.sleep(0.5)
                                    driver.find_element(By.XPATH,
                                                        f'//table[@id="{result_table_id}"]//span[contains(text(), "{self.document_type}")]/..').click()

                                    time.sleep(3)
                                    WebDriverWait(driver, 30).until(
                                        EC.presence_of_element_located(
                                            (By.CSS_SELECTOR, '[id="form:kostenpflichtigabrufen"]')))
                                    driver.find_element(By.CSS_SELECTOR, '[id="form:kostenpflichtigabrufen"]').click()

                                    time.sleep(3)

                                    driver.back()

                                    WebDriverWait(driver, 30).until(EC.presence_of_element_located(
                                        (By.CSS_SELECTOR, 'div[id="ergebnissForm:selectedSuchErgebnisFormTable"]')))
                                    time.sleep(5)

                                except Exception as ex:
                                    print('error', ex)
                                    sel = Selector(text=driver.page_source)
                                    if sel.css('[id="form:kostenpflichtigabrufen"]').get():
                                        driver.back()

                                        WebDriverWait(driver, 30).until(EC.presence_of_element_located((
                                            By.CSS_SELECTOR,
                                            'div[id="ergebnissForm:selectedSuchErgebnisFormTable"]')))
                                        time.sleep(3)
                                    else:
                                        time.sleep(2)
                                        pass

                                try:
                                    cursor.execute('''
                                                    INSERT INTO `companies`(
                                                        `id`, 
                                                        `register_references`, 
                                                        `name`, 
                                                        `headquarter_city`, 
                                                        `currently_registered`,  
                                                        `create_date_time`) 
                                                        VALUES (
                                                            %s,%s,%s,%s,%s,%s
                                                        )
                                                    ''',
                                                   (
                                                       company_id,
                                                       item['register'],
                                                       item['name'],
                                                       item['city'],
                                                       item['currently_registered'],
                                                       now
                                                   ))

                                    utils.cache_company(company_id, self.document_type, now)

                                    db.commit()

                                except pymysql.Error as e:
                                    print("Error %d: %s" % (e.args[0], e.args[1]))

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
            if postal_code_item is not None and city_item is not None and utils.replace_german_chars(
                    city_item.strip()).lower() == utils.replace_german_chars(
                    city_filter.strip()).lower() and postal_code_item in self.postal_codes:
                postal_codes_set.add(postal_code_item)
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
    query = {"register_types": {"HRB"},
             "register_options": {},
             "cities": {"Mannheim"},
             "streets": {},
             "postal_codes": {'68305', '68309', '68167', '68307', '68169', '68159', '68161', '68163', '68165', '68132',
                              '68139', '68131', '68127', '68149', '68156', '68147', '68145', '68143', '68141', '68134',
                              '68122', '68148', '68051', '68144', '68137', '68140', '68299', '68124', '68301', '68135',
                              '68151', '68136', '68300', '68112', '68298', '68133', '68150', '68123', '68146', '68142',
                              '68197', '68126', '68302', '68121', '68138', '68130', '68128', '68157', '68259', '68056',
                              '67061', '67059', '68623', '68517', '67082', '67055', '67076', '67077', '67057', '67075',
                              '67056', '67078', '68519', '67069', '67079', '68199', '67065', '68549', '67227', '68619',
                              '68542', '68239', '67063', '67071', '67067', '68535', '67122', '68219', '67225', '68526',
                              '68229', '67240', '69493', '67141', '69469', '67545', '69221', '68642', '69465', '69502',
                              '67259', '69198', '67112', '67117', '67258', '68782', '67165', '69514', '64653', '67468',
                              '67551', '67547', '69123', '67549', '67245', '69517', '68723', '67133', '69488', '64646',
                              '64683', '68721', '67136', '67166', '69214', '67125', '69124', '67229', '69115', '68775',
                              '69120', '69121', '64625', '67105', '67134', '69509', '68647', '67256', '67550', '67158',
                              '67246', '67126', '64624', '64623', '69117', '69109', '69108', '69112', '69111', '69110',
                              '67127', '69253', '69259', '64668', '67167', '69126', '69207', '67161', '68649', '67098',
                              '67591', '69118', '67251', '69518', '67459', '67271', '67281', '67346', '64673', '67159',
                              '64579', '68766', '67283', '68764', '67273', '69483', '67580', '64658', '64686', '67149',
                              '67574', '67343', '67342', '67340', '64665', '69250', '69181', '67169', '67278', '67373',
                              '67590', '67150', '68804'},
             "keywords": {},
             "keywords_match_option": "one",
             "keywords_similar_sounding": False}

    obj = HandelsRegister(query, file_type)
    obj.start_request()
