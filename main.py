from log_handler import logger
from agents_collector import main as agents_scraper_main

def main():
    """Main function to run the agents scraper."""
    logger.info("Starting the agents scraper...")
    agents_scraper_main()
    logger.info("Agents scraper finished.")

if __name__ == "__main__":
    main()