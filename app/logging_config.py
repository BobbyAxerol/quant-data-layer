"""
Centralized logging configuration for data_layer service.
Handles both file and console output with rotation.
"""
import os
import logging
import logging.handlers


def setup_logging(logs_dir: str = "/app/logs") -> logging.Logger:
    """
    Configure file and console logging with rotation.
    
    Args:
        logs_dir: Directory to store log files
        
    Returns:
        Configured root logger
    """
    # Create logs directory if it doesn't exist
    os.makedirs(logs_dir, exist_ok=True)
    
    # Get root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    
    # Remove existing handlers to avoid duplicates
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Format
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    # ─ File handler with rotation
    log_file = os.path.join(logs_dir, "app.log")
    file_handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=10_000_000,  # 10MB per file
        backupCount=5         # Keep 5 rotated backups
    )
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)
    
    # ─ Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    root_logger.info(f"Logging initialized. Logs: {log_file}")
    
    return root_logger
