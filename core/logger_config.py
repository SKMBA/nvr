# # core\logger_config.py
# import logging 
# logger =logging .getLogger ("nvr_logger")
# logger .setLevel (logging .DEBUG )
# if not logger .hasHandlers ():
#     handler =logging .StreamHandler ()
#     formatter =logging .Formatter (
#     "[%(asctime)s] %(levelname)s - %(message)s",
#     datefmt ="%Y-%m-%d %H:%M:%S"
#     )
#     handler .setFormatter (formatter )
#     logger .addHandler (handler )

 ################################
### Optimised
import logging
import os
import json
from pathlib import Path
from typing import Union, Any
from core.app_config import AppConfig

def setup_logger(config: Union[dict, Any]) -> logging.Logger:
    """
    Setup logger based on configuration object or dictionary.
    
    Args:
        config: Configuration object (from AppConfig) or dictionary
        
    Returns:
        Configured logger instance
    """
    # Handle both object notation and dictionary access
    if hasattr(config, 'logging'):
        log_config = config.logging
    else:
        log_config = config.get("logging", {})
    
    # Extract configuration values with defaults
    logger_name = getattr(log_config, 'name', None) or log_config.get('name', 'nvr_logger')
    level_str = getattr(log_config, 'level', None) or log_config.get('level', 'DEBUG')
    console_enabled = getattr(log_config, 'console_enabled', None)
    if console_enabled is None:
        console_enabled = log_config.get('console_enabled', True)
    file_enabled = getattr(log_config, 'file_enabled', None)
    if file_enabled is None:
        file_enabled = log_config.get('file_enabled', False)
    
    log_format = getattr(log_config, 'format', None) or log_config.get('format', '[%(asctime)s] %(levelname)s - %(message)s')
    date_format = getattr(log_config, 'datefmt', None) or log_config.get('datefmt', '%Y-%m-%d %H:%M:%S')
    
    # Get logger instance
    logger = logging.getLogger(logger_name)
    
    # Set logging level
    level = getattr(logging, level_str.upper(), logging.DEBUG)
    logger.setLevel(level)
    
    # Clear existing handlers to avoid duplicates
    if logger.hasHandlers():
        logger.handlers.clear()
    
    # Create formatter
    formatter = logging.Formatter(log_format, datefmt=date_format)
    
    # Setup console handler if enabled
    if console_enabled:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    # Setup file handler if enabled
    if file_enabled:
        log_folder = getattr(log_config, 'folder', None) or log_config.get('folder', 'logs')
        log_file = getattr(log_config, 'file', None) or log_config.get('file', 'app.log')
        
        # Create log directory if it doesn't exist
        Path(log_folder).mkdir(parents=True, exist_ok=True)
        
        log_path = Path(log_folder) / log_file
        file_handler = logging.FileHandler(log_path)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger

# Initialize logger using AppConfig
try:
    app_config = AppConfig(str(Path("config\\app_config.json").resolve()))
    config_obj = app_config.get_config()
    logger = setup_logger(config_obj)
    
    # For backward compatibility - ensure 'nvr_logger' is available
    if hasattr(config_obj.logging, 'name') and config_obj.logging.name != 'nvr_logger':
        # If config uses different name, also setup nvr_logger for compatibility
        nvr_logger = logging.getLogger('nvr_logger')
        if not nvr_logger.hasHandlers():
            # Copy handlers from main logger
            for handler in logger.handlers:
                nvr_logger.addHandler(handler)
            nvr_logger.setLevel(logger.level)
    
except (FileNotFoundError, json.JSONDecodeError, Exception) as e:
    # Fallback to basic configuration if config loading fails
    logger = logging.getLogger("nvr_logger")
    logger.setLevel(logging.DEBUG)
    if not logger.hasHandlers():
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "[%(asctime)s] %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.warning(f"Failed to load config, using default logging: {e}")

# Export the configured logger for backward compatibility
__all__ = ['logger', 'setup_logger']