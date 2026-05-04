import sys
import logging

import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.ingest import main as ingest_main
from pipeline.transform import main as transform_main
from pipeline.provision import main as provision_main


# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    try:
        logger.info("Starting full pipeline execution")

        # Run Bronze
        logger.info("Running ingest.py")
        ingest_main()

        # Run Silver
        logger.info("Running transform.py")
        transform_main()

        # Run Gold
        logger.info("Running provision.py")
        provision_main()

        logger.info("Pipeline completed successfully")
        sys.exit(0)

    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()