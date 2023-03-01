# -*- coding: utf-8 -*-
import glob
import os
import re
from csv import writer
from csv import DictWriter
from datetime import datetime, timezone

import fitz
import pymysql
import xmltodict
from watchfiles import watch

import utils
from mysql import db, cursor


class PdfReader:

    def __init__(self):
        self.append_header = True

    @staticmethod
    def german_words_replacing(input_string):
        return input_string.replace('ü', 'ue').replace('ö', 'oe').replace('ä', 'ae').replace('ß', 'ss')

    def process_files(self, file_pattern="handelsregister.de/SI/*.xml"):
        for file_path in glob.glob(file_pattern):
            if file_path.endswith('.xml'):
                self.process_xml_file(file_path)
            elif file_path.endswith('.pdf'):
                self.process_pdf_file(file_path)

    def process_xml_file(self, file_path):

        postal_codes = {
            '76870', '76863', '76770', '76872', '76779', '76751', '76116', '76314', '76477', '76183', '76125'
            '76744', '76764',  '76831', '76865', '76889', '76359', '76467', '76126', '76182', '76109', '76253',
            '76072', '76115', '76071', '76133', '76073', '76119', '76097', '76098', '76107', '76117', '76246',
            '76287', '76448', '76199', '76776', '76133', '76070', '76273', '76118', '76247', '76245', '76120',
            '76437', '76473', '76549', '76530', '76532', '76534', '77815', '77833', '77836', '76275', '76316',
            '76571', '76474', '76767', '76131', '76133', '76149', '76185', '76187', '76135', '76189', '76137',
            '67227', '68159', '68259', '68157', '68128', '68130', '68138', '68121', '68302', '68126', '68197',
            '68142', '68146', '68123', '68150', '68133', '76185', '68298', '68112', '68300', '68136', '68151',
            '68135', '68301', '68124', '68299', '68140', '68137', '68144', '68051', '68148', '68122', '68134',
            '68141', '68143', '68145', '68147', '68156', '68149', '68127', '68131', '68139', '68132', '68161',
            '68165', '68167', '68163', '68305', '68169', '67059', '67061', '67063', '67065', '67067', '67069',
            '67071', '68307', '68309'
        }
        ceos = []
        liable_persons = []
        liable_companies = []
        person_limited_partners = []
        prokurist_persons = []
        vorstands = []
        executive_directors = []

        company_name = ''
        company_typ = ''
        company_headquarter_city = ''
        company_headquarter_country = ''
        address_typ = ''
        company_address_street = ''
        company_address_haus_number = None
        company_address_postal_code = ''
        company_address_city = ''
        company_address_country = ''

        trade_court = ''
        trade_register_number = ''
        primary_key = ''

        results = []

        with open(file_path) as fd:
            doc = xmltodict.parse(fd.read())
            payload = doc['XJustiz_Daten']

            participation = list(payload['Grunddaten']['Verfahrensdaten']['Beteiligung'])
            fach_register = payload['Fachdaten_Register']
            company_purpose = fach_register['Basisdaten_Register']['Gegenstand_oder_Geschaeftszweck']
            register_gmbh_capital = ''
            # register_kg_capital = payload['Fachdaten_Register']['Zusatzangaben']['Personengesellschaft']['Zusatz_KG']['Daten_Kommanditist']
            if 'Zusatzangaben' in fach_register and 'Kapitalgesellschaft' in fach_register[
                'Zusatzangaben'] and 'Zusatz_GmbH' in fach_register['Zusatzangaben']['Kapitalgesellschaft']:
                register_gmbh_capital = fach_register['Zusatzangaben']['Kapitalgesellschaft']['Zusatz_GmbH'][
                    'Stammkapital']

            # print("1. participation", participation[0])
            # print("2. participation", participation[1])

            for part in participation:

                # print(part)

                role = part['Rolle']

                if type(role) == dict:
                    role_name = role['Rollenbezeichnung']['content'].strip()

                if type(role) == list:
                    role_name = role[0]['Rollenbezeichnung']['content'].strip()

                if role_name == 'Rechtsträger' and 'Organisation' in part['Beteiligter']:
                    company = part['Beteiligter']['Organisation']
                    company_name = company['Bezeichnung']['Bezeichnung_Aktuell']
                    company_typ = company['Rechtsform']['content']
                    company_headquarter_city = company['Sitz']['Ort']
                    company_headquarter_country = company['Sitz']['Staat']['content']
                    address_typ = company['Anschrift']['Anschriftstyp']['content']
                    company_address_street = company['Anschrift']['Strasse']
                    if 'Hausnummer' in company['Anschrift']:
                        company_address_haus_number = company['Anschrift']['Hausnummer']
                    company_address_postal_code = company['Anschrift']['Postleitzahl']
                    company_address_city = company['Anschrift']['Ort']
                    company_address_country = company['Anschrift']['Staat']['content']

                elif role_name == 'Registergericht' and 'Organisation' in part['Beteiligter']:
                    trade_court = part['Beteiligter']['Organisation']['Bezeichnung']['Bezeichnung_Aktuell']
                    trade_register_number = part['Rolle']['Geschaeftszeichen']
                    primary_key = (trade_court + '-' + trade_register_number).replace(' ', '-').strip().lower()
                    primary_key = utils.replace_german_chars(primary_key)

                elif role_name == 'Registergericht' and 'Natuerliche_Person' in part['Beteiligter']:
                    trade_court = part['Beteiligter']['Natuerliche_Person']['Voller_Name']['Nachname']
                    trade_register_number = part['Rolle']['Geschaeftszeichen']
                    primary_key = (trade_court + '-' + trade_register_number).replace(' ', '-').strip().lower()
                    primary_key = utils.replace_german_chars(primary_key)

                elif (role_name == 'Kommanditist' or role_name == 'Kommanditist(in)') and 'Natuerliche_Person' in part[
                    'Beteiligter']:
                    person_limited_partner = part['Beteiligter']['Natuerliche_Person']
                    person_limited_partner_firstname = person_limited_partner['Voller_Name']['Vorname']
                    person_limited_partner_lastname = person_limited_partner['Voller_Name']['Nachname']

                    person_limited_partner_birthdate = None
                    if 'Geburt' in person_limited_partner:
                        person_limited_partner_birthdate = person_limited_partner['Geburt']['Geburtsdatum']

                    person_limited_partner_profession = None
                    if 'Beruf' in person_limited_partner:
                        person_limited_partner_profession = person_limited_partner['Beruf']

                    person_limited_partner_residency_city = person_limited_partner['Anschrift']['Ort']
                    person_limited_partner_residency_country = person_limited_partner['Anschrift']['Staat']['content']

                    person_limited_partners.append({'firstname': person_limited_partner_firstname,
                                                    'lastname': person_limited_partner_lastname,
                                                    'birthdate': person_limited_partner_birthdate,
                                                    'profession': person_limited_partner_profession,
                                                    'residency_city': person_limited_partner_residency_city,
                                                    'residency_country': person_limited_partner_residency_country})

                elif (role_name == 'Kommanditist' or role_name == 'Kommanditist(in)') and 'Organisation' in part[
                    'Beteiligter']:
                    company = part['Beteiligter']['Organisation']
                    company_name = company['Bezeichnung']['Bezeichnung_Aktuell']
                    company_typ = company['Rechtsform']['content']
                    company_headquarter_city = company['Sitz']['Ort']
                    company_headquarter_country = company['Sitz']['Staat']['content']
                    address_typ = company['Anschrift']['Anschriftstyp']['content']

                    if 'Hausnummer' in company['Anschrift']:
                        company_address_haus_number = company['Anschrift']['Hausnummer']

                    if 'Strasse' in company['Anschrift']:
                        company_address_street = company['Anschrift']['Strasse']

                    if 'Postleitzahl' in company['Anschrift']:
                        company_address_postal_code = company['Anschrift']['Postleitzahl']

                    company_address_city = company['Anschrift']['Ort']
                    company_address_country = company['Anschrift']['Staat']['content']

                elif (role_name == 'Prokurist' or role_name == 'Prokurist(in)') and 'Natuerliche_Person' in part[
                    'Beteiligter']:
                    prokurist_person = part['Beteiligter']['Natuerliche_Person']
                    prokurist_person_firstname = prokurist_person['Voller_Name']['Vorname']
                    prokurist_person_lastname = prokurist_person['Voller_Name']['Nachname']

                    prokurist_person_birthdate = None
                    if 'Geburt' in prokurist_person:
                        prokurist_person_birthdate = prokurist_person['Geburt']['Geburtsdatum']

                    prokurist_person_profession = None
                    if 'Beruf' in prokurist_person:
                        prokurist_person_profession = prokurist_person['Beruf']

                    prokurist_person_residency_city = prokurist_person['Anschrift']['Ort']
                    prokurist_person_residency_country = prokurist_person['Anschrift']['Staat']['content']

                    prokurist_persons.append({'firstname': prokurist_person_firstname,
                                              'lastname': prokurist_person_lastname,
                                              'birthdate': prokurist_person_birthdate,
                                              'profession': prokurist_person_profession,
                                              'residency_city': prokurist_person_residency_city,
                                              'residency_country': prokurist_person_residency_country})

                elif (
                        role_name == 'Persönlich haftender Gesellschafter' or role_name == 'Persönlich haftende(r) Gesellschafter(in)') and 'Natuerliche_Person' in \
                        part['Beteiligter']:
                    person_limited_partner = part['Beteiligter']['Natuerliche_Person']
                    person_limited_partner_firstname = person_limited_partner['Voller_Name']['Vorname']
                    person_limited_partner_lastname = person_limited_partner['Voller_Name']['Nachname']
                    person_limited_partner_birthdate = None
                    if 'Geburt' in person_limited_partner:
                        person_limited_partner_birthdate = person_limited_partner['Geburt']['Geburtsdatum']
                    person_limited_partner_profession = None
                    if 'Beruf' in person_limited_partner:
                        person_limited_partner_profession = person_limited_partner['Beruf']

                    person_limited_partner_residency_city = person_limited_partner['Anschrift']['Ort']
                    person_limited_partner_residency_country = person_limited_partner['Anschrift']['Staat']['content']

                    liable_persons.append({'firstname': person_limited_partner_firstname,
                                           'lastname': person_limited_partner_lastname,
                                           'birthdate': person_limited_partner_birthdate,
                                           'profession': person_limited_partner_profession,
                                           'residency_city': person_limited_partner_residency_city,
                                           'residency_country': person_limited_partner_residency_country})

                elif (
                        role_name == 'Persönlich haftender Gesellschafter' or role_name == 'Persönlich haftende(r) Gesellschafter(in)') and 'Organisation' in \
                        part['Beteiligter']:
                    limited_partner = part['Beteiligter']['Organisation']
                    limited_partner_name = limited_partner['Bezeichnung']['Bezeichnung_Aktuell']
                    limited_partner_headquarter_city = limited_partner['Sitz']['Ort']
                    limited_partner_headquarter_country = limited_partner['Sitz']['Staat']['content']

                    liable_companies.append(
                        {"partner_name": limited_partner_name, "headquarter_city": limited_partner_headquarter_city,
                         "headquarter_country": limited_partner_headquarter_country})

                elif role_name == 'Geschäftsführer' or role_name == 'Geschäftsführer(in)':
                    person = part['Beteiligter']['Natuerliche_Person']
                    ceo_firstname = person['Voller_Name']['Vorname']
                    ceo_lastname = person['Voller_Name']['Nachname']
                    ceo_birthdate = None
                    if 'Geburt' in person:
                        ceo_birthdate = person['Geburt']['Geburtsdatum']

                    ceo_profession = None
                    if 'Beruf' in person:
                        ceo_profession = person['Beruf']

                    ceo_residency_city = person['Anschrift']['Ort']
                    ceo_residency_country = person['Anschrift']['Staat']['content']

                    ceos.append({'firstname': ceo_firstname,
                                 'lastname': ceo_lastname,
                                 'birthdate': ceo_birthdate,
                                 'profession': ceo_profession,
                                 'residency_city': ceo_residency_city,
                                 'residency_country': ceo_residency_country})

                # if role_name == 'Prokurist' and 'Organisation' in part['Beteiligter']:
                elif (role_name == 'Liquidator' or role_name == 'Liquidator(in)') and 'Natuerliche_Person' in part[
                    'Beteiligter']:
                    # print("TODO: liquidator")
                    continue

                elif role_name == 'Vorstand' and 'Natuerliche_Person' in part['Beteiligter']:
                    # print("Vorstand")
                    vorstand = part['Beteiligter']['Natuerliche_Person']
                    vorstand_firstname = vorstand['Voller_Name']['Vorname']
                    vorstand_lastname = vorstand['Voller_Name']['Nachname']

                    vorstand_birthdate = None
                    if 'Geburt' in vorstand:
                        vorstand_birthdate = vorstand['Geburt']['Geburtsdatum']

                    vorstand_profession = None
                    if 'Beruf' in vorstand:
                        vorstand_profession = vorstand['Beruf']

                    vorstand_residency_city = vorstand['Anschrift']['Ort']
                    vorstand_residency_country = vorstand['Anschrift']['Staat']['content']

                    vorstands.append({'firstname': vorstand_firstname,
                                      'lastname': vorstand_lastname,
                                      'birthdate': vorstand_birthdate,
                                      'profession': vorstand_profession,
                                      'residency_city': vorstand_residency_city,
                                      'residency_country': vorstand_residency_country})

                elif role_name == 'Abwickler' and 'Natuerliche_Person' in part['Beteiligter']:
                    # print("############ TODO: Abwickler", part)
                    continue

                elif role_name == 'Hauptniederlassung' and 'Organisation' in part['Beteiligter']:
                    # print("############ TODO: Hauptniederlassung", part)
                    continue

                elif role_name == 'Geschäftsführender Direktor' and 'Natuerliche_Person' in part['Beteiligter']:
                    # print("Geschäftsführender Direktor")
                    executive_director = part['Beteiligter']['Natuerliche_Person']
                    executive_director_firstname = executive_director['Voller_Name']['Vorname']
                    executive_director_lastname = executive_director['Voller_Name']['Nachname']

                    executive_director_birthdate = None
                    if 'Geburt' in executive_director:
                        executive_director_birthdate = executive_director['Geburt']['Geburtsdatum']

                    executive_director_profession = None
                    if 'Beruf' in executive_director:
                        executive_director_profession = executive_director['Beruf']

                    executive_director_residency_city = executive_director['Anschrift']['Ort']
                    executive_director_residency_country = executive_director['Anschrift']['Staat']['content']
                    executive_directors.append({'firstname': executive_director_firstname,
                                                'lastname': executive_director_lastname,
                                                'birthdate': executive_director_birthdate,
                                                'profession': executive_director_profession,
                                                'residency_city': executive_director_residency_city,
                                                'residency_country': executive_director_residency_country})
                else:
                    # print("############ TODO: " + role_name, part)
                    continue

        company_name_l = company_name.lower()
        if company_purpose is not None:
            company_purpose_l = company_purpose.lower()
        else:
            company_purpose_l = ''

        field_names = ['Unternehmen', 'Sitz', 'Anschrift: Straße', 'Anschrift: PLZ', 'Anschrift: City',
                       'Anschrift: Land', 'Stammkapital', 'Gegenstand/Geschäftszweck']

        if (
                'logistik' in company_name_l or 'logistics' in company_name_l or 'cargo' in company_name_l or 'spedition' in company_name_l or 'transport' in company_name_l or 'logistik' in company_purpose_l or 'logistics' in company_purpose_l or 'cargo' in company_purpose_l or 'spedition' in company_purpose_l or 'transport' in company_purpose_l) and company_address_postal_code in postal_codes:
            print("Unternehmen:", company_name)
            # print("company_typ:", company_typ)
            print("Sitz:", company_headquarter_city + ", " + company_headquarter_country)
            # print("address_typ:", address_typ)
            print("Anschrift:")
            print(company_address_street + " " + company_address_haus_number)
            print(company_address_postal_code + " " + company_address_city)
            print(company_address_country)
            # print("trade_court:", trade_court)
            # print("trade_register_number:", trade_register_number)
            # print("primary_key:", primary_key)
            if len(ceos) > 0:
                print("Geschäftsführer:")
                for ceo in ceos:
                    print(ceo['firstname'] + " " + ceo['lastname'])
            # print("Persönlich haftender Gesellschafter:", liable_companies)
            # print("Kommanditist:", person_limited_partners)
            if len(executive_directors) > 0:
                print("Geschäftsführender Direktor:")
                for executive_director in executive_directors:
                    print(executive_director['firstname'] + " " + executive_director['lastname'])
            gmbh_capital = '-'
            if len(register_gmbh_capital) > 0:
                gmbh_capital = register_gmbh_capital['Zahl'] + ' ' + register_gmbh_capital['Waehrung']

            print("Stammkapital:", gmbh_capital)

            print("Gegenstand/Geschäftszweck:", company_purpose)
            print("")

            results.append({
                "Unternehmen": company_name,
                "Sitz": company_headquarter_city + ", " + company_headquarter_country,
                "Anschrift: Straße": company_address_street + " " + company_address_haus_number,
                "Anschrift: PLZ": company_address_postal_code,
                "Anschrift: City": company_address_city,
                "Anschrift: Land": company_address_country,
                "Stammkapital": gmbh_capital,
                "Gegenstand/Geschäftszweck": company_purpose
            })

        for result in results:
            print(result)
            self.append_dict_as_row("output.csv", result, field_names)

    def append_dict_as_row(self, file_name, dict_of_elem, field_names):
        # Open file in append mode
        with open(file_name, 'a+', newline='') as write_obj:
            # Create a writer object from csv module
            dict_writer = DictWriter(write_obj, fieldnames=field_names)
            # Add dictionary as wor in the csv
            if self.append_header:
                dict_writer.writeheader()
                self.append_header = False
            dict_writer.writerow(dict_of_elem)

    def process_pdf_file(self, file_path):
        doc = fitz.Document(file_path)
        pages = doc.page_count
        count = 0
        all_data = list()

        for page in range(pages):
            page_obj = doc.load_page(page)
            count += 1
            page_content = page_obj.get_text()
            all_data.append(page_content)

        doc.close()

        final_data = '\n'.join(all_data)

        pattern = "Geschäftsanschrift:.*\n"

        match = re.search(pattern, final_data)

        business_address = ''
        if match:
            extracted_text = match.group()
            business_address = self.german_words_replacing(extracted_text)

        pattern = re.compile("Gegenstand des Unternehmens:(.*?)\n\d+\.", re.DOTALL)
        match = re.search(pattern, final_data)
        business_purpose = ''
        if match:
            extracted_text = match.group(1)
            business_purpose = self.german_words_replacing(extracted_text)

        share_capital = ''
        regex = re.compile(r"(\d+(?:[.,]\d+)*),(\d+)\s(EUR|DEN)")

        capital_match = re.search(regex, final_data)
        if capital_match:
            share_capital = capital_match.group()

        pattern = re.compile("Geschäftsführer:(.*?)\n")
        match = re.search(pattern, final_data)

        ceo_data = ''
        if match:
            extracted_text = match.group(1)
            ceo_data = extracted_text

        ceo_data_list = ceo_data.split(',')
        ceo_birthdate = ''

        regex = re.compile(r"(\d{2})\.(\d{2})\.(\d{4})")

        match = regex.search(ceo_data)
        if match:
            day, month, year = map(int, match.groups())
            ceo_birthdate = str(datetime(year, month, day).strftime('%d.%m.%Y'))

        ceo_city = self.german_words_replacing(ceo_data_list[-2] if len(ceo_data) > 1 else '')
        ceo_name = self.german_words_replacing(
            ceo_data.replace(ceo_city, '').replace(ceo_birthdate, '').replace(', ', ' ').replace(',', ''))

        ceo_name = ceo_name.split('*')[0] if '*' in ceo_name else ceo_name

        pattern = re.compile("Prokura:(.*?)\n\d+\.", re.DOTALL)
        match = re.search(pattern, final_data)
        Prokura = ''
        if match:
            extracted_text = match.group(1)
            Prokura = self.german_words_replacing(extracted_text)

        pattern = re.compile("Gesellschaftsvertrag vom (.*?)\n")
        match = re.search(pattern, final_data)

        incorporation_date = ''
        if match:
            extracted_text = match.group(1)
            incorporation_date = self.german_words_replacing(extracted_text)

        pattern = re.compile("Tag der letzten Eintragung:\n(.*?)\n")
        match = re.search(pattern, final_data)

        last_entry = ''
        if match:
            extracted_text = match.group(1)
            last_entry = self.german_words_replacing(extracted_text)

        now = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        print(share_capital)
        print("incorporation_dated:", incorporation_date)
        share_capital_amount = share_capital.split(' ')[0].replace('.', '').replace(',', '.').strip()
        share_capital_currency = share_capital.split(' ')[-1].strip()
        print("share_capital_amount:", share_capital)
        print("share_capital_amount:", share_capital_amount)
        print("share_capital_currency:", share_capital_currency)
        print("ceo_birthdate:", ceo_birthdate)
        print("ceo_name:", ceo_name)

        # self.persist_company(company_data)

    def persist_company(self, company_data):
        try:

            cursor.execute('''
                            INSERT INTO `company_ceos`(
                                `company_id`, 
                                `first_name`,  
                                `last_name`,  
                                `residence_city`,  
                                `birthdate`,  
                                `create_date_time`) 
                                VALUES (
                                    %s,%s,%s,%s,%s,%s
                                )
                            ''',
                           (
                               primary_key,
                               ceo_first_name,
                               ceo_name,
                               ceo_city,
                               ceo_birthdate,
                               now
                           ))

            db.commit()
            # os.remove(file_path)
        except pymysql.Error as e:
            print(e)
            print("Error %d: %s" % (e.args[0], e.args[1]))


if __name__ == '__main__':
    obj = PdfReader()
    obj.process_files()
    for watched_paths in watch(os.path.join(os.getcwd(), 'handelsregister.de/SI'), watch_filter=utils.added_xml_filter):
        for pathname in watched_paths:
            obj.process_files(pathname[1])
