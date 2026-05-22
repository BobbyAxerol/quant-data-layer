import logging
import os
import time

from app.alpha.strategy import DataLayerClient, MovingAverageCrossAlpha


def configure_logging() -> None:
    import os
    from app.logging_config import setup_logging

    # Create alpha-specific log directory
    alpha_log_dir = "/app/logs/alpha"
    os.makedirs(alpha_log_dir, exist_ok=True)

    # Setup logging (this uses alpha_log_dir for alpha logs)
    setup_logging(logs_dir=alpha_log_dir)


def main() -> None:
    configure_logging()

    base_url = os.getenv("DATA_LAYER_URL", "http://data_layer:8100")
    symbol = os.getenv("ALPHA_SYMBOL", "SSI")
    high_tf = os.getenv("ALPHA_HIGH_TF", "60min")
    fast = int(os.getenv("ALPHA_FAST", 8))
    slow = int(os.getenv("ALPHA_SLOW", 20))
    redis_host = os.getenv("REDIS_HOST", "redis_service")
    redis_port = int(os.getenv("REDIS_PORT", 6379))
    redis_db = int(os.getenv("REDIS_DB", 2))

    client = DataLayerClient(
        base_url=base_url,
        redis_host=redis_host,
        redis_port=redis_port,
        redis_db=redis_db,
    )

    while True:
        try:
            alpha = MovingAverageCrossAlpha(
                client=client,
                symbol=symbol,
                high_tf=high_tf,
                fast=fast,
                slow=slow,
            )
            alpha.run()
        except Exception as exc:
            logging.exception("Alpha service failed: %s", exc)
            logging.info("Restarting alpha service in 30 seconds...")
            time.sleep(30)


if __name__ == "__main__":
    main()
