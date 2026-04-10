from app_logger import configure_logger
from ingest_raw import ingest_raw

if __name__ == "__main__":
    logger = configure_logger()
    logger.info('main.py start')
    ingest_raw('shipment_status', 'dev')
    ingest_raw('shipment_products', 'dev')