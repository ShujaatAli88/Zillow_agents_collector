import requests
from log_handler import logger
from constants import Constants
from time import sleep
from lxml import etree
from models import AgentDataModel
import json
import pandas as pd
import os


proxies={
        "http": "http://hzuesdkw-rotate:a4lx7aqeyd7m@p.webshare.io:80/",
        "https": "http://hzuesdkw-rotate:a4lx7aqeyd7m@p.webshare.io:80/"
    }
class AgentsScraper:
    def __init__(self):
        self.zguid = None
        self.zgsession = None

    def home_page_request(self, retries=3, delay=2):
        """Request the home page to set cookies with retry logic."""
        logger.info("Requesting the home page to set cookies...")
        for attempt in range(1, retries + 1):
            try:
                response = requests.get(
                    'https://www.zillow.com/',
                    headers=Constants.HOME_PAGE_HEADERS.value,
                    timeout=10
                )
                if response.status_code == 200:
                    logger.info("Successfully requested the home page.")
                    cookies = response.cookies.get_dict()
                    self.zguid = cookies.get("zguid")
                    self.zgsession = cookies.get("zgsession")
                    self.cookies = cookies
                    logger.info(f"Cookies set successfully: {cookies}")
                    return  # Exit after success
                else:
                    logger.warning(f"Request failed with status: {response.status_code}")
            except Exception as e:
                logger.error(f"Attempt {attempt} failed: {e}")

            if attempt < retries:
                logger.info(f"Retrying in {delay} seconds... (Attempt {attempt + 1}/{retries})")
                sleep(delay)
            else:
                logger.error("All retries exhausted.")
                return None
    
    def jsesssion_api_request(self, retries=3, delay=2):
        """Request the JSESSIONID API to set cookies with retry logic."""
        logger.info("Requesting the JSESSIONID API to set cookies...")

        for attempt in range(1, retries + 1):
            try:
                # Ensure cookies are available
                if not self.zguid or not self.zgsession:
                    logger.warning("Missing 'zguid' or 'zgsession'. Did home_page_request() run?")
                    return None

                # Update constants cookies
                Constants.COOKIES.value['zguid'] = self.zguid
                Constants.COOKIES.value['zgsession'] = self.zgsession

                response = requests.get(
                    'https://www.zillow.com/ajax/nav/UserNavAsync.htm',
                    params=Constants.PARAMS.value,
                    headers=Constants.HEADERS.value,
                    cookies=Constants.COOKIES.value,
                    timeout=10
                )

                if response.status_code == 200:
                    logger.info("JSESSIONID API request successful.")
                    self.cookies.update(response.cookies.get_dict())
                    logger.info(f"Updated cookies: {self.cookies}")
                    return  # Exit after success
                else:
                    logger.warning(f"Attempt {attempt}: Received status code {response.status_code}")

            except Exception as e:
                logger.error(f"Attempt {attempt}: Exception occurred - {e}")

            if attempt < retries:
                logger.info(f"Retrying JSESSIONID API in {delay} seconds... (Attempt {attempt + 1}/{retries})")
                sleep(delay)
            else:
                logger.error("All retries for JSESSIONID API exhausted.")
                return None
    
    def request_agents_api(self, page_number, retries=3, delay=2):
        logger.info(f"Requesting the agents API for Page Number: {page_number}")

        for attempt in range(1, retries + 1):
            try:
                params = {'page': str(page_number)}
                response = requests.get(
                    'https://www.zillow.com/professionals/real-estate-agent-reviews/jacksonville-fl/',
                    params=params,
                    cookies=self.cookies,
                    headers=Constants.AGENTS_HEADERS.value,
                    timeout=10
                )

                if response.status_code == 200:
                    logger.info(f"Successfully fetched agents page {page_number}")
                    return response
                else:
                    logger.warning(f"Attempt {attempt}: Non-200 response: {response.status_code}")

            except Exception as e:
                logger.error(f"Attempt {attempt}: Exception during agent page request: {e}")

            if attempt < retries:
                logger.info(f"Retrying agents API in {delay} seconds... (Attempt {attempt + 1}/{retries})")
                sleep(delay)
            else:
                logger.error(f"All retries failed for agents page {page_number}")
                return None
    
    def get_all_agents_links(self,agents_data_response,page_number):
        logger.info(f"Getting all The agents detail page links on page number:{page_number}")
        try:
            tree = etree.HTML(agents_data_response.text)
            total_agents_found = tree.xpath("(//span[contains(@class,'jTpxxT')])[1]/text()[1]")
            logger.info(f"\033[92mTotal Agents Found For This Search are: {total_agents_found[0]} Agents.\033[0m")
            agents_links = tree.xpath("//div[contains(@class,'Grid')]/a/@href")
            if agents_links:
                logger.info(f"Found {len(agents_links)} agents links on page number: {page_number}.")
                return agents_links
            else:
                logger.warning(f"No agents links found on page number: {page_number}.")
                return None
        except Exception as e:
            logger.error(f"An error occurred while parsing the agents response: {e}")
            return None
    
    def request_agent_link(self,agent_link):
        logger.info(f"processing agent link: {agent_link}")
        try:
            respons = requests.get(
                agent_link,
                headers=Constants.DETAIL_PAGE_HEADERS.value,
                cookies=self.cookies,
                # proxies=proxies
            )
            if respons.status_code == 200:
                logger.info(f"Request Successfull to agent link: {agent_link}")
                with open(f'./agent_detail_page.html', 'w', encoding='utf-8') as file:
                    file.write(respons.text)
                return respons
        except Exception as e:
            logger.error(f"An error occurred while processing the agent link {agent_link}: {e}")
            return None

    def parse_agent_detal_page(self,agent_detail_page_response):
        logger.info("Parsing the agent detail page...")
        try:
            tree = etree.HTML(agent_detail_page_response.text)
            agent_name = tree.xpath("//div[contains(@class,'ProfileFooter')]//h1[contains(@class,'StyledHeading')]/text()")
            phones = tree.xpath("//a[starts-with(@href, 'tel:')]/text()")
            emails = tree.xpath("//a[starts-with(@href, 'mailto:')]/text()")
            addresses = tree.xpath("//a[contains(@href, 'maps.google.com')]/@href")
            company_name = tree.xpath("//span[contains(@class, 'Text-c11n-8-107-0') and contains(@class, 'hJOiOT')]/text()")
            number_of_sales_last_12_months =tree.xpath("//span[contains(text(),'sales last 12 months')]/preceding-sibling::span/strong/text()")
            total_sales = tree.xpath("//span[contains(text(),'total sales')]/preceding-sibling::span/strong/text()")
            price_range = tree.xpath("//span[contains(text(),'price range')]/preceding-sibling::span/strong/text()")
            average_price = tree.xpath("//span[contains(text(),'average price')]/preceding-sibling::span/strong/text()")
            years_of_experience = tree.xpath("//span[contains(text(),'years of experience')]/preceding-sibling::span/strong/text()")
            data_dic = {
                "agent_name": agent_name[0] if agent_name else None,
                "phone_number_1":  phones[0] if phones else "Phone Numbers Not available",
                "phone_number_2" : phones[1] if phones else "-",
                "agent_email_address": emails[0] if emails else "Email Not Available",
                "agent_location": addresses[0] if addresses else "Location not Available",  
                "company_name": company_name[0] if company_name else None,
                "number_of_sales_last_12_months": number_of_sales_last_12_months[0] if number_of_sales_last_12_months else None,
                "total_sales": total_sales[0] if total_sales else None,
                "price_range": price_range[0] if price_range else None,
                "average_price": average_price[0] if average_price else None,
                "years_of_experience": years_of_experience[0] if years_of_experience else None,
            }
            logger.info(f"Agent Details Scraped: {data_dic}")
            return data_dic
        except Exception as error:
            logger.error(f"An error occurred while parsing the agent detail page: {error}")
            return None

    def validate_date(self,scraped_data):
        logger.info("Validating The Scraped Data.")
        try:
            data_model = AgentDataModel(
                agent_name=scraped_data.get("agent_name"),
                agent_email_address=scraped_data.get("agent_email_address"),
                phone_number_1=scraped_data.get("phone_number_1"),
                phone_number_2=scraped_data.get("phone_number_2"),
                agent_location=scraped_data.get("agent_location"),
                company_name=scraped_data.get("company_name"),
                number_of_sales_last_12_months=scraped_data.get("number_of_sales_last_12_months"),
                total_sales=scraped_data.get("total_sales"),
                price_range=scraped_data.get("price_range"),
                average_price=scraped_data.get("average_price"),
                years_of_experience=scraped_data.get("years_of_experience")
            )
            logger.info(f"Data Validated Successfully : {data_model}")
            return data_model
        except Exception as err:
            logger.error(f"Error while Validating the Scraped Data:{err}")
            return None
    
    def save_scraped_data(self, data):
        logger.info("Saving the scraped data (append mode)...")
        try:
            file_path = "zillow_agents_data.csv"
            df = pd.DataFrame([data])
            file_exists_and_has_data = os.path.exists(file_path) and os.stat(file_path).st_size > 0

            df.to_csv(file_path, mode='a', index=False, encoding='utf-8', header=not file_exists_and_has_data)
            logger.info(f"Appended record to {file_path}")
        except Exception as error:
            logger.error(f"Error while saving scraped data: {error}")


def main():
    STATUS = True
    page_number = 1
    scraper = AgentsScraper()
    scraper.home_page_request()
    scraper.jsesssion_api_request()
    while True:
        logger.info(f"Sleeping for 5 seconds before requesting page {page_number}...")
        sleep(5)
        agents_data_response = scraper.request_agents_api(page_number)
        if not agents_data_response:
            STATUS = False
            return STATUS

        agents_links = scraper.get_all_agents_links(agents_data_response,page_number)
        if not agents_links:
            STATUS = False
            return

        for agent_link in agents_links:
            agent_detail_page_response = scraper.request_agent_link(agent_link)
            scraped_data = scraper.parse_agent_detal_page(agent_detail_page_response)
            data_model = scraper.validate_date(scraped_data)
            data = data_model.dict()
            scraper.save_scraped_data(data)
            # break

        # break

        page_number += 1