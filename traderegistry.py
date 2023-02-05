# -*- coding: utf-8 -*-

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import pymysql
import utils
from scrapy import Selector
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager


class HandelsRegister:
    def __init__(self):
        try:
            self.conn = pymysql.connect(user='handelsregister_un', passwd='handelsregister_pw', host='localhost',
                                        port=3307, db='handelsregister_db', use_unicode=True, charset='utf8')
            self.cursor = self.conn.cursor()

            self.cursor.execute('''
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
                    `last_update_date_time` VARCHAR(35));''')

            self.cursor.execute('''
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
                    CONSTRAINT `uc_ceo` UNIQUE (`company_id`, `first_name`, `last_name`));''')

            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS company_histories(
                    `id` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    `company_id` VARCHAR(255) NOT NULL,
                    `position` int NOT NULL, 
                    `name` VARCHAR(255) NOT NULL, 
                    `city` VARCHAR(255) NOT NULL,
                    `create_date_time` VARCHAR(35) NOT NULL,
                    `last_update_date_time` VARCHAR(35),
                    FOREIGN KEY (`company_id`) REFERENCES companies(`id`),
                    CONSTRAINT `uc_history` UNIQUE (`company_id`, `name`, `city`));''')

            self.cursor.execute('''
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
                    CONSTRAINT `uc_procura` UNIQUE (`company_id`, `first_name`, `last_name`));''')

            self.cursor.execute('''
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
                    CONSTRAINT `uc_location` UNIQUE (`company_id`, `city`, `postal_code`, `street`));''')

            self.cursor.execute('''CREATE TABLE IF NOT EXISTS company_contacts( `id` BIGINT NOT NULL AUTO_INCREMENT 
            PRIMARY KEY, `company_id` VARCHAR(255) NOT NULL, `titel` VARCHAR(15) NULL, `gender` VARCHAR(15) NULL, 
            `first_name` VARCHAR(55), `last_name` VARCHAR(55) NOT NULL, `position` VARCHAR(255), `location` BIGINT 
            NULL, `office_email` VARCHAR(255), `private_email` VARCHAR(255), `office_mobile` VARCHAR(255), 
            `office_phone` VARCHAR(255), `private_mobile` VARCHAR(255), `private_phone` VARCHAR(255), 
            `create_date_time` VARCHAR(35) NOT NULL, `last_update_date_time` VARCHAR(35), FOREIGN KEY (`company_id`) 
            REFERENCES companies(`id`), FOREIGN KEY (`location`) REFERENCES company_locations(`id`), CONSTRAINT 
            `uc_contact` UNIQUE (`company_id`, `first_name`, `last_name`, `office_email`));''')

            self.conn.commit()
        except (AttributeError, pymysql.OperationalError) as e:
            raise e

    @staticmethod
    def get_driver():
        opt = webdriver.ChromeOptions()
        opt.add_argument("--start-maximized")
        preferences = {
            "download.default_directory": os.path.join(os.getcwd(), 'handelsregister.de/AD'),
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

    def start_request(self, search_query=None):
        if search_query is None:
            raise ValueError
        keyword = search_query["keywords"]
        print("keyword:", keyword)
        keywords_match_option = self.get_keyword_match_option(search_query["keywords_match_option"])
        print("keywords_match_option:", keywords_match_option)

        csv_postal_code = utils.read_postal_code_csv_file()
        for register_type in self.get_register_types(search_query["register_types"]):
            print("register_type:", register_type)
            for register_option in self.get_register_options(register_type):
                print("register_option:", register_option)
                for city in self.get_cities(csv_postal_code, search_query["cities"]):
                    print("city:", city)
                    for postal_code in self.get_postal_codes(csv_postal_code, city):
                        print("postal_code:", postal_code)
                        # for street in utils.get_streets(postal_code):
                        # print("street:", street)
                        driver = self.get_driver()
                        driver.get('https://www.handelsregister.de/rp_web/erweitertesuche.xhtml')
                        time.sleep(3)

                        WebDriverWait(driver, 30).until(
                            # EC.presence_of_element_located((By.CSS_SELECTOR, '[id="form:registerNummer"]')))
                            EC.presence_of_element_located((By.CSS_SELECTOR, '[id="form:schlagwoerter"]')))
                        self.__scroll_down_page(driver)

                        # TODO: keyword (variable: keyword)
                        time.sleep(0.5)
                        driver.find_element(By.CSS_SELECTOR, '[id="form:schlagwoerter"]').send_keys(keyword)

                        # TODO: keywords match option (variable: keywords_match_option)
                        time.sleep(0.5)
                        driver.find_element(By.CSS_SELECTOR,
                                            f'label[for="form:schlagwortOptionen:{keywords_match_option}"]').click()

                        WebDriverWait(driver, 30).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, '[id="form:registerNummer"]')))
                        self.__scroll_down_page(driver)

                        # TODO: register type (variable: register_type)
                        time.sleep(0.5)
                        driver.find_element(By.CSS_SELECTOR, '[id="form:registerArt_label"]').click()

                        # TODO: register option (variable: register_option)
                        time.sleep(0.5)
                        driver.find_element(By.CSS_SELECTOR, f'li[data-label="{register_type}"]').click()

                        # TODO: postal code (variable: postal_code)
                        time.sleep(0.5)
                        driver.find_element(By.CSS_SELECTOR, '[id="form:postleitzahl"]').send_keys(postal_code)

                        time.sleep(0.5)
                        driver.find_element(By.CSS_SELECTOR, '[id="form:ort"]').send_keys(city)

                        # TODO: street (variable: street)
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
                                'register': ' '.join(
                                    [row.strip() for row in row_data.css('td.fontTableNameSize *::text').getall() if
                                     row.strip() != '']),
                                'name': row_data.css('span[class="marginLeft20"]::text').get('').strip(),
                                'city': row_data.css('.sitzSuchErgebnisse>span::text').get('').strip(),
                                'currently_registered': True if 'currently registered' in row_data.css(
                                    'td:nth-child(3)>span::text').get('') else False,
                                'history': json.dumps(history_list)
                            }
                            print('history: ', history_list)
                            print('currently_registered: ', item['currently_registered'])
                            print("register: ", item['register'])
                            company_id = utils.clean_company_id(
                                item['register'].split('court')[-1].strip().replace(' ', '-').lower())
                            print("id: ", company_id)
                            print("name: ", item['name'])
                            print("city: ", item['city'])
                            now = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
                            print("created_datetime: ", now)
                            print(" ")
                            print(" ")
                            try:
                                self.cursor.execute('''
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
                                self.conn.commit()

                            except pymysql.Error as e:
                                print("Error %d: %s" % (e.args[0], e.args[1]))

                        for res in response.css('.borderBottom3>table::attr(id)').getall():
                            try:
                                driver.execute_script("arguments[0].scrollIntoView(true);",
                                                      driver.find_element(By.XPATH,
                                                                          f'//table[@id="{res}"]//span[contains(text(), "AD")]/..'))
                                time.sleep(0.5)
                                driver.find_element(By.XPATH,
                                                    f'//table[@id="{res}"]//span[contains(text(), "AD")]/..').click()

                                time.sleep(3)
                                WebDriverWait(driver, 30).until(
                                    EC.presence_of_element_located(
                                        (By.CSS_SELECTOR, '[id="form:kostenpflichtigabrufen"]')))
                                driver.find_element(By.CSS_SELECTOR, '[id="form:kostenpflichtigabrufen"]').click()
                                detail_sel = Selector(text=driver.page_source)
                                pdf_name = detail_sel.css('div.ui-panel-content>span::text').get(
                                    '').strip().replace(' ', '-')
                                # print(pdf_name)
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
                        time.sleep(20)
                        driver.quit()

    @staticmethod
    def get_register_types(register_types: set) -> set:
        register_types_catalog = {"HRA", "HRB", "GnR", "PR", "VR"}

        if register_types is None or len(register_types) == 0:
            return register_types_catalog

        # ensure that register_types has only valid values
        invalid_register_types = register_types.difference(register_types_catalog)

        if len(invalid_register_types) > 0:
            raise Exception('valid register types are: "HRA", "HRB", "GnR", "PR" and "VR"')

        if len(register_types) == len(register_types_catalog):
            return { "all" }

        return register_types

    @staticmethod
    def get_register_options(register_type_filter) -> set:
        return {"all"}

    @staticmethod
    def get_cities(csv_file_content, cities: set) -> set:
        collected_cities = set()
        if cities is None or len(cities) == 0:
            # read all cities from the file (in read only mode)
            # return HandelsRegister.read_csv_column(postal_codes_by_city_df, 'ort')
            for csv_dict in csv_file_content[:]:
                item = csv_dict.get('ort')
                print(item)
                collected_cities.add(item)

            return collected_cities

        return cities

    @staticmethod
    def get_postal_codes(csv_file_content, city_filter: str) -> set:
        postal_codes_set = set()
        for csv_dict in csv_file_content[:]:
            postal_code_item = csv_dict.get('plz')
            city_item = csv_dict.get('ort')
            if postal_code_item is not None and city_item is not None and utils.replace_german_chars(
                    city_item.strip()).lower() == utils.replace_german_chars(city_filter.strip()).lower():
                postal_codes_set.add(postal_code_item)
        return postal_codes_set

    @staticmethod
    def get_streets(postal_code_filter: str) -> set:
        return utils.get_streets(postal_code_filter)

    @staticmethod
    def get_keyword_match_option(keywords_match_option: str) -> int:
        if keywords_match_option is None or keywords_match_option == "one":
            return 1
        if keywords_match_option == "all":
            return 0
        if keywords_match_option == "exact":
            return 2
        raise Exception('valid keywords options are: "all", "one" and "exact"')


if __name__ == '__main__':
    obj = HandelsRegister()

    query = {"register_types": {"HRB"},
             "cities": {"Mannheim"},
             "postal_codes": {},
             "keywords": {},
             "keywords_match_option": "one",
             "keywords_similar_sounding": False}

    obj.start_request(query)
