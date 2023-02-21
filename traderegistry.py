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
                                    time.sleep(2)
                                    driver.execute_script("arguments[0].scrollIntoView(true);",
                                                          driver.find_element(By.XPATH,
                                                                              f'//table[@id="{result_table_id}"]//span[contains(text(), "{self.document_type}")]/..'))
                                    time.sleep(3)
                                    driver.find_element(By.XPATH,
                                                        f'//table[@id="{result_table_id}"]//span[contains(text(), "{self.document_type}")]/..').click()

                                    time.sleep(5)
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
             "register_options": {"Kommanditgesellschaft", "Aktiengesellschaft", "Europäische Aktiengesellschaft (SE)",
                                  "Gesellschaft mit beschränkter Haftung"},
             "cities": {},
             "streets": {},
             "keywords": 'Logistik Logistics Transport Spedition Cargo',
             "keywords_match_option": "one",
             "keywords_similar_sounding": False,
             "postal_codes": {'55129', '63931', '76857', '76777', '55268', '76831', '64823', '67806', '69427', '55266',
                              '55237', '64853', '74921', '67729', '76878', '74867', '69429', '76829', '64732', '64572',
                              '55239', '55276', '64401', '64407', '67487', '64754', '67378', '67483', '74933', '74889',
                              '64280', '64279', '64277', '64273', '64278', '64272', '64271', '64276', '64274', '65468',
                              '64287', '64283', '64293', '64295', '64289', '64285', '74869', '67725', '64409', '64380',
                              '64305', '64308', '64304', '76865', '67727', '74928', '65428', '55599', '63916', '67678',
                              '64560', '64297', '69436', '67316', '67585', '64760', '67310', '67583', '67304', '67587',
                              '64579', '68766', '67283', '68764', '67273', '69483', '67580', '64658', '64686', '67149',
                              '64683', '68721', '67136', '67166', '69214', '67125', '69124', '67229', '69115', '68775',
                              '64720', '64354', '74934', '67295', '67480', '74918', '74858', '64759', '64521', '67472',
                              '67056', '67078', '68519', '67069', '67079', '68199', '67065', '68549', '67227', '68619',
                              '67061', '67059', '68623', '68517', '67082', '67055', '67076', '67077', '67057', '67075',
                              '67127', '69253', '69259', '64668', '67167', '69126', '69207', '67161', '68649', '67098',
                              '67246', '67126', '64624', '64623', '69117', '69109', '69108', '69112', '69111', '69110',
                              '67259', '69198', '67112', '67117', '67258', '68782', '67165', '69514', '64653', '67468',
                              '67363', '67489', '74937', '55232', '76709', '64395', '76726', '74939', '74927', '76669',
                              '67429', '69234', '64367', '67360', '74909', '69231', '67361', '67319', '64753', '64756',
                              '67551', '67547', '69123', '67549', '67245', '69517', '68723', '67133', '69488', '64646',
                              '67574', '67343', '67342', '67340', '64665', '69250', '69181', '67169', '67278', '67373',
                              '67577', '67308', '67578', '64397', '64757', '55234', '67311', '68789', '69159', '64319',
                              '67586', '67434', '67482', '64405', '67366', '76661', '67368', '55278', '74931', '67305',
                              '67590', '67150', '68804', '67269', '67454', '67157', '67147', '69434', '67599', '67575',
                              '67591', '69118', '67251', '69518', '67459', '67271', '67281', '67346', '64673', '67159',
                              '67808', '76676', '67475', '76698', '76756', '55283', '76835', '64275', '64281', '64270',
                              '67816', '64347', '76707', '74925', '64346', '67292', '64711', '76879', '76706', '74915',
                              '68122', '68148', '68051', '68144', '68137', '68140', '68299', '68124', '68301', '68135',
                              '68139', '68131', '68127', '68149', '68156', '68147', '68145', '68143', '68141', '68134',
                              '68151', '68136', '68300', '68112', '68298', '68133', '68150', '68123', '68146', '68142',
                              '68197', '68126', '68302', '68121', '68138', '68130', '68128', '68157', '68259', '68056',
                              '68229', '67240', '69493', '67141', '69469', '67545', '69221', '68642', '69465', '69502',
                              '68305', '68309', '68167', '68307', '68169', '68159', '68161', '68163', '68165', '68132',
                              '68542', '68239', '67063', '67071', '67067', '68535', '67122', '68219', '67225', '68526',
                              '68794', '67376', '69239', '69245', '64584', '67466', '67435', '67280', '67317', '67598',
                              '68809', '64678', '64342', '67374', '69251', '64385', '68799', '67592', '67152', '67354',
                              '69120', '69121', '64625', '67105', '67134', '69509', '68647', '67256', '67550', '67158',
                              '69151', '64689', '64404', '67146', '67582', '67593', '69190', '69189', '67595', '69226',
                              '69168', '67596', '68753', '67433', '69257', '67365', '69256', '64743', '64589', '67377',
                              '69254', '67294', '67307', '69412', '67473', '76725', '76724', '64372', '69242', '67297',
                              '69439', '76689', '76351', '67680', '64739', '55286', '55296', '76761', '64846', '76703',
                              '74821', '67813', '55578', '76764', '76344', '64807', '64390', '67819', '67681', '64307',
                              '74838', '74930', '63328', '76770', '55597', '64750', '64331', '64291', '63329', '63225',
                              '63299', '63303', '64546', '63222', '63223', '64839', '63322', '64543', '64542', '64545',
                              '63128', '64859', '64569', '63263', '63110', '60547', '60589', '60591', '60521', '60544',
                              '60525', '60590', '60523', '60546', '60524', '60519', '63256', '63258', '63257', '64832',
                              '65450', '65479', '65425', '63069', '63150', '65439', '65795', '65451', '63147', '63126',
                              '63177', '63106', '65784', '60528', '60326', '65933', '60529', '55299', '64850', '60549',
                              '60438', '65931', '60329', '60327', '63071', '60325', '60486', '60599', '60596', '60598',
                              '60594', '63075', '63065', '63073', '63067', '63059', '63060', '63057', '63061', '63062',
                              '63063', '63064', '65934', '65936', '65474', '60311', '60313', '60648', '65929', '60318',
                              '60322', '60310', '60314', '60316', '60385', '60308', '60487', '60320', '60323', '60306',
                              '55294', '63179', '65239', '65830', '60628', '60377', '60291', '60284', '60223', '65926',
                              '60286', '60608', '60646', '60647', '60303', '60265', '60060', '60309', '60614', '60637',
                              '60278', '60645', '60222', '60304', '60622', '60620', '65927', '60279', '60627', '60616',
                              '60261', '60257', '60624', '60302', '60617', '60288', '60644', '60379', '60277', '60699',
                              '60641', '60651', '60289', '60609', '60612', '60252', '60632', '60633', '60638', '60216',
                              '60283', '60267', '60615', '60380', '60631', '60634', '60270', '60272', '60626', '60301',
                              '60653', '60268', '60643', '60275', '60280', '60305', '60630', '60611', '60307', '60274',
                              '60294', '60621', '60635', '60600', '60295', '60652', '60255', '60263', '60185', '60254',
                              '60623', '60297', '60607', '60619', '60610', '60298', '60264', '60613', '60629', '60259',
                              '60290', '60276', '60300', '60262', '60382', '60636', '60256', '60296', '65925', '60639',
                              '60285', '60625', '60260', '60258', '60273', '65719', '60431', '64747', '55130', '65462',
                              '60389', '60435', '60489', '60488', '60386', '63165', '63500', '63762', '60433', '65205',
                              '55131', '63532', '65835', '63533', '65717', '65843', '55246', '60388', '65760', '63499',
                              '60439', '65842', '65844', '65840', '63512', '65207', '55124', '55116', '55128', '65212',
                              '65209', '65218', '65213', '65215', '65210', '65824', '55252', '65812', '65758', '65756',
                              '65755', '65757', '65754', '65823', '55122', '63456', '63853', '55270', '63477', '65779',
                              '63811', '55147', '55149', '55148', '55101', '55098', '55146', '55097', '55150', '55100',
                              '60437', '63791', '60428', '60484', '60482', '60429', '60485', '60483', '60423', '60422',
                              '60424', '61449', '55118', '55120', '63457', '55127', '63801', '65810', '55099', '63538',
                              '63796', '61440', '63814', '61118', '63785', '65203', '61437', '61436', '61435', '61434',
                              '61476', '61462', '63438', '63443', '63442', '63446', '63475', '63444', '63447', '63441',
                              '63741', '63452', '63454', '63450', '55288', '63843', '63743', '61474', '61350', '61352',
                              '61348', '65187', '65189', '65199', '65197', '65817', '65191', '63755', '61138', '63834',
                              '63784', '65175', '65179', '65174', '65177', '65169', '65170', '65172', '65171', '65166',
                              '65180', '65178', '65176', '65164', '65173', '65182', '65181', '65167', '65214', '63868',
                              '61137', '63867', '55291', '63906', '55126', '63754', '63939', '63925', '63739', '65193',
                              '61342', '61346', '61345', '61341', '61300', '61343', '65185', '65183', '65195', '61184',
                              '63776', '65201', '63486', '63736', '63735', '63716', '63715', '63737', '63864', '55257',
                              '63937', '63526', '63543', '55271', '63911', '61381', '63517', '65527', '61182', '61116',
                              '61111', '63839', '63849', '63820', '65510', '61479', '63806', '63840', '61130', '65396',
                              '63773', '55263', '63808', '61378', '61379', '55262', '63924', '65343', '63768', '61273',
                              '61194', '61389', '63505', '63897', '61191', '63933', '65344', '63863', '63579', '65346',
                              '63856', '61206', '63594', '63920', '63546', '63934', '65345', '63829', '55437', '65529',
                              '65399', '65388', '65232', '61167', '63549', '55218', '65341', '63825', '61267', '65347',
                              '63875', '63872', '63826', '65509', '65507', '63846', '63674', '61169', '63874', '55216',
                              '63694', '63877', '61276', '55435', '65375', '63927', '63857', '63584', '74847', '76863',
                              '74906', '69437', '76297', '74924', '74862', '76774', '67728', '76773', '67471', '67677',
                              '67691', '67724', '67693', '76642', '76643', '76826', '76828', '76825', '76824', '67722',
                              '75031', '67817', '76877', '76684', '76833', '74887', '76694', '76771', '67814', '76646',
                              '76437', '76473', '76549', '76530', '76532', '76534', '77815', '77833', '77836', '76571',
                              '76275', '76316', '76571', '76474', '76767', '76131', '76133', '76149', '76185', '76187',
                              '76135', '76189', '76137'}}

    obj = HandelsRegister(query, file_type)
    obj.start_request()
