#!/usr/bin/env python3
import logging
import os

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)

from news_pipeline.standby_ingestor import run

if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        logging.info("Standby ingestor stopped by user")
    except Exception as e:
        logging.error("Standby ingestor crashed: %s", e)
        raise
