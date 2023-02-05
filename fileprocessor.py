# -*- coding: utf-8 -*-
import os
import re
import glob
from datetime import datetime, timezone
from watchfiles import watch, Change

import fitz
import pymysql


class PdfReader:

    def __init__(self):
        self.conn = pymysql.connect(user='handelsregister_un', passwd='handelsregister_pw', host='localhost', port=3307,
                                    db='handelsregister_db', use_unicode=True, charset='utf8')
        self.cursor = self.conn.cursor()

    @staticmethod
    def german_words_replacing(input_string):
        return input_string.replace('ü', 'ue').replace('ö', 'oe').replace('ä', 'ae').replace('ß', 'ss')

    def process_files(self, file_pattern="handelsregister.de/AD/*.pdf"):
        for file_path in glob.glob(file_pattern):
            file_id = file_path.split('_')[-1].split('+')[0]
            doc = fitz.Document(file_path)
            pages = doc.page_count
            count = 0
            all_data = list()
            for page in range(pages):
                page_obj = doc.load_page(page)
                count += 1
                page_content = page_obj.get_text()
                all_data.append(page_content)

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
            ceo_date = ''

            regex = re.compile(r"(\d{2})\.(\d{2})\.(\d{4})")

            match = regex.search(ceo_data)
            if match:
                day, month, year = map(int, match.groups())
                ceo_date = str(datetime(year, month, day).strftime('%d.%m.%Y'))

            ceo_city = self.german_words_replacing(ceo_data_list[-2] if len(ceo_data) > 1 else '')
            ceo_name = self.german_words_replacing(
                ceo_data.replace(ceo_city, '').replace(ceo_date, '').replace(', ', ' ').replace(',', ''))

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
            print("ceo_birthdate:", ceo_date)
            try:
                self.cursor.execute(
                    # f"UPDATE `companies` SET `headquarter_address_supplement`='{business_address}',`business_purpose`='{business_purpose}',`share_capital_amount`='{share_capital_amount}',`ceo_last_name`='{ceo_name}',`ceo_residence_city`='{ceo_city}',`ceo_birthdate`='{ceo_date}',`incorporation_date`='{incorporation_date}', `last_registry_update`='{last_entry}', `last_update_date_time`='{now}'  WHERE `ID` LIKE '%-{file_id}'"
                    f"UPDATE `companies` SET `headquarter_address_supplement`='{business_address}',`business_purpose`='{business_purpose}',`share_capital_amount`='{share_capital_amount}', `incorporation_date`='{incorporation_date}', `last_registry_update`='{last_entry}', `last_update_date_time`='{now}'  WHERE `ID` LIKE '%-{file_id}'"
                )
                # TODO `prokura`='{Prokura}',
                # TODO `ceo_last_name`='{ceo_name}',`ceo_residence_city`='{ceo_city}',`ceo_birthdate`='{ceo_date}',

                self.conn.commit()
                doc.close()
                os.remove(file_path)

            except pymysql.Error as e:
                print(e)
                print("Error %d: %s" % (e.args[0], e.args[1]))

    @staticmethod
    def added_pdf_filter(change: Change, file_path: str) -> bool:
        return change.value == Change.added and file_path.endswith('.pdf')


if __name__ == '__main__':
    obj = PdfReader()
    obj.process_files()
    for watched_paths in watch(os.path.join(os.getcwd(), 'handelsregister.de/AD'), watch_filter=obj.added_pdf_filter):
        for pathname in watched_paths:
            obj.process_files(pathname[1])
