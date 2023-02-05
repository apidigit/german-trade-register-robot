# -*- coding: utf-8 -*-
import csv
import os

import scrapy
from scrapy import Selector
from scrapy.crawler import CrawlerProcess
import utils


class PlzplzSpiderSpider(scrapy.Spider):
    name = 'plzplz_spider'
    start_urls = ['https://quotes.toscrape.com/']

    reset_html_cache_flag = False
    reset_txt_cache_flag = False

    custom_settings = {
        'HTTPERROR_ALLOW_ALL': True,
    }
    headers = {
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-language': 'en-US,en;q=0.9',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, '
                      'like Gecko) Chrome/109.0.0.0 Safari/537.36',
    }

    base_search_url = 'https://plzplz.de/PLZ_{}/#plz-ergebnisse'

    def parse(self, response):
        for input_dict in utils.read_postal_code_csv_file()[:]:
            postal_code = input_dict.get('plz', '')

            if self.reset_html_cache_flag:
                yield scrapy.Request(
                    url=self.base_search_url.format(postal_code),
                    headers=self.headers,
                    callback=self.parse_search_page,
                    meta={'postal_code': postal_code}
                )
            else:
                file_path = f'plzplz.de/cache/{postal_code}.html'
                if os.path.isfile(file_path):
                    sel = Selector(text=self.cache_html_file(file_path))
                    if self.reset_txt_cache_flag:
                        all_streets = sel.css('.psc_results>tr>td:nth-child(2)>a::text').getall()
                        self.write_street_file(postal_code, all_streets)
                    else:
                        file_path = f'plzplz.de/streets/{postal_code}.txt'
                        if os.path.isfile(file_path):
                            pass
                        else:
                            all_streets = sel.css('.psc_results>tr>td:nth-child(2)>a::text').getall()
                            self.write_street_file(postal_code, all_streets)

                else:
                    yield scrapy.Request(
                        url=self.base_search_url.format(postal_code),
                        headers=self.headers,
                        callback=self.parse_search_page,
                        meta={'postal_code': postal_code}
                    )

    def parse_search_page(self, response):
        postal_code = response.meta.get('postal_code')
        file_path = f'plzplz.de/streets/{postal_code}.txt'
        if self.reset_txt_cache_flag:
            all_streets = response.css('.psc_results>tr>td:nth-child(2)>a::text').getall()
            self.write_street_file(postal_code, all_streets)
        else:
            if os.path.isfile(file_path):
                pass
            else:
                all_streets = response.css('.psc_results>tr>td:nth-child(2)>a::text').getall()

                self.write_street_file(postal_code, all_streets)

        self.write_html_file(postal_code, response.text)

    @staticmethod
    def write_html_file(postal_code, response):
        with open(f"plzplz.de/cache/{postal_code}.html", "w", encoding='utf-8') as file:
            file.write(response)

    @staticmethod
    def write_street_file(postal_code, response):
        with open(f"plzplz.de/streets/{postal_code}.txt", "w", encoding='utf-8') as file:
            for street in response:
                file.write(f"{street}\n")

    @staticmethod
    def cache_html_file(file_path):
        with open(file_path, "r", encoding='utf-8') as file:
            return file.read()


process = CrawlerProcess()
process.crawl(PlzplzSpiderSpider)
process.start()
