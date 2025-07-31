from log_handler import logger


def get_proxies():
    logger.info("Fetching proxies...")
    try:
        proxies={
            "http": "http://hzuesdkw-rotate:a4lx7aqeyd7m@p.webshare.io:80/",
            "https": "http://hzuesdkw-rotate:a4lx7aqeyd7m@p.webshare.io:80/"
        }
        return proxies
    except Exception as e:
        logger.error(f"Error getting proxies: {e}")
        proxies = None