import requests
from log_handler import logger
from constants import Constants
from time import sleep
from lxml import etree
from models import AgentDataModel
import json
import pandas as pd
import os
from proxy_handler import get_proxies
from bq_handler import BigQueryHandler


class AgentsScraper:
    def __init__(self):
        self.zguid = None
        self.zgsession = None
        self.proxies = get_proxies()
        self.querydata_handler = BigQueryHandler()
        self.querydata_handler.connect()  # connect to Big Query...

    def home_page_request(self, retries=12, delay=2):
        """Request the home page to set cookies with retry logic."""
        logger.info("Requesting the home page to set cookies...")
        for attempt in range(1, retries + 1):
            try:
                response = requests.get(
                    "https://www.zillow.com/",
                    headers=Constants.HOME_PAGE_HEADERS.value,
                    timeout=10,
                    proxies=self.proxies,
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
                    logger.warning(
                        f"Request failed with status: {response.status_code}"
                    )
            except Exception as e:
                logger.error(f"Attempt {attempt} failed: {e}")

            if attempt < retries:
                logger.info(
                    f"Retrying in {delay} seconds... (Attempt {attempt + 1}/{retries})"
                )
                sleep(delay)
            else:
                logger.error("All retries exhausted.")
                return None

    def jsesssion_api_request(self, retries=12, delay=2):
        """Request the JSESSIONID API to set cookies with retry logic."""
        logger.info("Requesting the JSESSIONID API to set cookies...")

        for attempt in range(1, retries + 1):
            try:
                # Ensure cookies are available
                if not self.zguid or not self.zgsession:
                    logger.warning(
                        "Missing 'zguid' or 'zgsession'. Did home_page_request() run?"
                    )
                    return None

                # Update constants cookies
                Constants.COOKIES.value["zguid"] = self.zguid
                Constants.COOKIES.value["zgsession"] = self.zgsession

                response = requests.get(
                    "https://www.zillow.com/ajax/nav/UserNavAsync.htm",
                    params=Constants.PARAMS.value,
                    headers=Constants.HEADERS.value,
                    cookies=Constants.COOKIES.value,
                    timeout=10,
                    proxies=self.proxies,
                )

                if response.status_code == 200:
                    logger.info("JSESSIONID API request successful.")
                    self.cookies.update(response.cookies.get_dict())
                    logger.info(f"Updated cookies: {self.cookies}")
                    return  # Exit after success
                else:
                    logger.warning(
                        f"Attempt {attempt}: Received status code {response.status_code}"
                    )

            except Exception as e:
                logger.error(f"Attempt {attempt}: Exception occurred - {e}")

            if attempt < retries:
                logger.info(
                    f"Retrying JSESSIONID API in {delay} seconds... (Attempt {attempt + 1}/{retries})"
                )
                sleep(delay)
            else:
                logger.error("All retries for JSESSIONID API exhausted.")
                return None

    def request_agents_api(self, page_number, retries=12, delay=2):
        logger.info(f"Requesting the agents API for Page Number: {page_number}")

        for attempt in range(1, retries + 1):
            try:
                params = {
                    "page": f"{page_number}",
                    "priceRange": "250000,700000",
                }
                response = requests.get(
                    "https://www.zillow.com/professionals/real-estate-agent-reviews/raleigh-nc/",
                    params=params,
                    cookies=self.cookies,
                    headers=Constants.AGENTS_HEADERS.value,
                    timeout=10,
                    proxies=self.proxies,
                )

                if response.status_code == 200:
                    logger.info(f"Successfully fetched agents page {page_number}")
                    return response
                else:
                    logger.warning(
                        f"Attempt {attempt}: Non-200 response: {response.status_code}"
                    )

            except Exception as e:
                logger.error(
                    f"Attempt {attempt}: Exception during agent page request: {e}"
                )

            if attempt < retries:
                logger.info(
                    f"Retrying agents API in {delay} seconds... (Attempt {attempt + 1}/{retries})"
                )
                sleep(delay)
            else:
                logger.error(f"All retries failed for agents page {page_number}")
                return None

    def get_all_agents_links(self, agents_data_response, page_number):
        logger.info(
            f"Getting all The agents detail page links on page number:{page_number}"
        )
        try:
            tree = etree.HTML(agents_data_response.text)
            total_agents_found = tree.xpath(
                "(//span[contains(@class,'jTpxxT')])[1]/text()[1]"
            )
            logger.info(
                f"\033[92mTotal Agents Found For This Search are: {total_agents_found[0]} Agents.\033[0m"
            )
            agents_links = tree.xpath("//div[contains(@class,'Grid')]/a/@href")
            if agents_links:
                logger.info(
                    f"Found {len(agents_links)} agents links on page number: {page_number}."
                )
                return agents_links
            else:
                logger.warning(f"No agents links found on page number: {page_number}.")
                return None
        except Exception as e:
            logger.error(f"An error occurred while parsing the agents response: {e}")
            return None

    def request_agent_link(self, agent_link, retries=12, delay=3):
        logger.info(f"Processing agent link: {agent_link}")

        for attempt in range(1, retries + 1):
            try:
                response = requests.get(
                    agent_link,
                    headers=Constants.DETAIL_PAGE_HEADERS.value,
                    cookies=self.cookies,
                    proxies=self.proxies,
                    timeout=15,
                )

                if response.status_code == 200:
                    logger.info(f"✅ Request successful for agent link: {agent_link}")
                    return response
                else:
                    logger.warning(
                        f"Attempt {attempt}: Failed with status code {response.status_code} "
                        f"for agent link: {agent_link}"
                    )

            except Exception as e:
                logger.error(
                    f"Attempt {attempt}: Exception occurred while processing {agent_link} - {e}"
                )

            if attempt < retries:
                logger.info(
                    f"Retrying in {delay} seconds... (Attempt {attempt + 1}/{retries})"
                )
                sleep(delay)
            else:
                logger.error(
                    f"❌ All {retries} attempts failed for agent link: {agent_link}"
                )

        return None  # Return None if all retries fail

    def parse_agent_detail_page(self, agent_detail_page_response):
        logger.info("Parsing the agent detail page...")
        try:
            tree = etree.HTML(agent_detail_page_response.text)

            # Extract the raw JSON from the __NEXT_DATA__ script tag
            script_content = tree.xpath("//script[@id='__NEXT_DATA__']/text()")
            if not script_content:
                raise ValueError("No __NEXT_DATA__ script tag found.")

            json_data = json.loads(script_content[0])  # Convert string to dict

            # Optional: Save JSON for debugging
            # with open('agent_detail_data.json', "w", encoding="utf-8") as f:
            #     json.dump(json_data, f, indent=4)
            return json_data
        except Exception as error:
            logger.error(
                f"An error occurred while parsing the agent detail page: {error}"
            )
            return None

    def validate_date(self, scraped_data):
        logger.info("Validating The Scraped Data.")
        try:
            data_model = AgentDataModel(**scraped_data)
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
            file_exists_and_has_data = (
                os.path.exists(file_path) and os.stat(file_path).st_size > 0
            )

            df.to_csv(
                file_path,
                mode="a",
                index=False,
                encoding="utf-8",
                header=not file_exists_and_has_data,
            )
            logger.info(f"Appended record to {file_path}")
        except Exception as error:
            logger.error(f"Error while saving scraped data: {error}")

    def get_information(self, json_data):
        logger.info("getting The Insights from The response.")
        try:
            # Check if json_data is valid and contains required keys
            props = json_data.get("props")
            if not props:
                logger.error("Missing 'props' in JSON data.")
                return None

            page_props = props.get("pageProps")
            if not page_props:
                logger.error("Missing 'pageProps' in props.")
                return None

            display_user = page_props.get("displayUser")
            if not display_user:
                logger.error("Missing 'displayUser' in pageProps.")
                return None

            agent_name = display_user.get("name")
            businessName = display_user.get("businessName")

            business_address = display_user.get("businessAddress", {})
            address_1 = business_address.get("address1")
            address_2 = business_address.get("address2")
            city = business_address.get("city")
            state = business_address.get("state")
            postalCode = business_address.get("postalCode")

            phoneNumbers = display_user.get("phoneNumbers", {})
            cell = phoneNumbers.get("cell")
            brokerage = phoneNumbers.get("brokerage")
            email = display_user.get("email")

            agent_licenses = page_props.get("agentLicenses")
            license_status = agent_licenses[0].get("status")
            license_type = agent_licenses[0].get("license_type")
            expiration = agent_licenses[0].get("expiration")
            agent_license = agent_licenses[0]["text"] if agent_licenses else None

            agent_sales = page_props.get("agentSalesStats", {})
            total_sales = agent_sales.get("countAllTime")
            total_sales_last_12_months = agent_sales.get("countLastYear")
            min_price_range = agent_sales.get("priceRangeThreeYearMin")
            max_price_range = agent_sales.get("priceRangeThreeYearMax")
            average_price_range = agent_sales.get("averageValueThreeYear")

            data_dic = {
                "Agent Name": agent_name,
                "Agent Email": email,
                "Bussiness Name": businessName,
                "Agent Phone": cell,
                "Brokerage Phone": brokerage,
                "Agent License": agent_license,
                "License Status": license_status,
                "License Type": license_type,
                "License Expiration Date": expiration,
                "Address_1": address_1,
                "Address_2": address_2,
                "City": city,
                "State": state,
                "Postal Code": postalCode,
                "Total Sales": total_sales,
                "Total Sales Last 12 Months": total_sales_last_12_months,
                "Minimum Price Range": f"${min_price_range}"
                if min_price_range
                else None,
                "Maximum Price Range": f"${max_price_range}"
                if max_price_range
                else None,
                "Average Price Range": f"${average_price_range}"
                if average_price_range
                else None,
            }

            logger.info(f"Scraped Data is : {data_dic}")
            return data_dic

        except Exception as error:
            logger.error(f"Error While Getting agent information: {error}")
            return None


def main():
    STATUS = True
    page_number = 10
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

        agents_links = scraper.get_all_agents_links(agents_data_response, page_number)
        if not agents_links:
            STATUS = False
            return

        for agent_link in agents_links:
            agent_detail_page_response = scraper.request_agent_link(agent_link)
            json_data = scraper.parse_agent_detail_page(agent_detail_page_response)
            scraped_data = scraper.get_information(json_data)
            data_model = scraper.validate_date(scraped_data)
            if data_model:
                data = data_model.dict()
                df = pd.DataFrame([data])
                scraper.querydata_handler.insert_data(df)
                # scraper.save_scraped_data(data)
            else:
                logger.error("Skipping save because validation failed.")
            # break

        # break
        if page_number == 25:
            logger.info("All Pages Scraped Breaking The loop.")
            break

        page_number += 1
