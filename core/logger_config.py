# core/logger_config.py

import logging
import os
import json
import re
import threading
from pathlib import Path
from typing import Union, Any
from datetime import datetime, timedelta
from core.app_config import AppConfig


class DailyRotatingFileHandler(logging.FileHandler):
    """
    Custom file handler that implements daily log rotation with size-based splitting
    and automatic retention management.
    """
    
    def __init__(self, log_folder, base_filename, max_size_bytes=0, 
                 auto_delete_old_logs=False, keep_last_n_days=7, mode='a', 
                 encoding=None, delay=False):
        """
        Initialize the rotating file handler.
        
        Args:
            log_folder: Directory to store log files
            base_filename: Base name for log files (without extension)
            max_size_bytes: Maximum size per file in bytes (0 = no limit)
            auto_delete_old_logs: Whether to automatically delete old logs
            keep_last_n_days: Number of days to retain when auto-deleting
            mode: File open mode
            encoding: File encoding
            delay: Whether to delay file opening
        """
        self.log_folder = Path(log_folder)
        self.base_filename = base_filename
        self.max_size_bytes = max_size_bytes
        self.auto_delete_old_logs = auto_delete_old_logs
        self.keep_last_n_days = keep_last_n_days
        self.current_date = None
        self.current_sequence = 0
        self._lock = threading.Lock()
        
        # Create log directory if needed
        self.log_folder.mkdir(parents=True, exist_ok=True)
        
        # Initialize with today's log file
        self._determine_current_file()
        
        # Clean up old logs if configured
        if self.auto_delete_old_logs:
            self._cleanup_old_logs()
        
        # Initialize parent FileHandler
        super().__init__(str(self.current_file_path), mode, encoding, delay)
    
    def _determine_current_file(self):
        """Determine the current log file based on date and existing files."""
        today = datetime.now().date()
        self.current_date = today
        
        # Find existing files for today to determine sequence
        date_str = today.strftime('%Y-%m-%d')
        pattern = f"{self.base_filename}-{date_str}*.log"
        existing_files = list(self.log_folder.glob(pattern))
        
        if not existing_files:
            # No files for today, start with sequence 0 (no suffix)
            self.current_sequence = 0
            filename = f"{self.base_filename}-{date_str}.log"
        else:
            # Find the latest file and check if we need a new one due to size
            latest_file = self._get_latest_file_for_date(date_str)
            
            if self.max_size_bytes > 0 and latest_file.stat().st_size >= self.max_size_bytes:
                # Current file is too large, create next sequence
                self.current_sequence = self._get_next_sequence(date_str)
                if self.current_sequence == 1:
                    filename = f"{self.base_filename}-{date_str}.1.log"
                else:
                    filename = f"{self.base_filename}-{date_str}.{self.current_sequence}.log"
            else:
                # Use existing latest file
                self.current_sequence = self._extract_sequence_from_file(latest_file)
                filename = latest_file.name
        
        self.current_file_path = self.log_folder / filename
    
    def _get_latest_file_for_date(self, date_str):
        """Get the latest (highest sequence) log file for a given date."""
        pattern = f"{self.base_filename}-{date_str}*.log"
        files = list(self.log_folder.glob(pattern))
        if not files:
            return None
        
        # Sort by sequence number
        def extract_seq(file_path):
            match = re.search(rf"{re.escape(self.base_filename)}-{re.escape(date_str)}(?:\.(\d+))?\.log$", file_path.name)
            return int(match.group(1)) if match and match.group(1) else 0
        
        return max(files, key=extract_seq)
    
    def _extract_sequence_from_file(self, file_path):
        """Extract sequence number from filename."""
        match = re.search(r'\.(\d+)\.log$', file_path.name)
        return int(match.group(1)) if match else 0
    
    def _get_next_sequence(self, date_str):
        """Get the next sequence number for the given date."""
        pattern = f"{self.base_filename}-{date_str}*.log"
        files = list(self.log_folder.glob(pattern))
        
        if not files:
            return 0
        
        sequences = []
        for file_path in files:
            match = re.search(rf"{re.escape(self.base_filename)}-{re.escape(date_str)}(?:\.(\d+))?\.log$", file_path.name)
            if match:
                seq = int(match.group(1)) if match.group(1) else 0
                sequences.append(seq)
        
        return max(sequences) + 1 if sequences else 1
    
    def _should_rotate(self):
        """Check if log rotation is needed (date change or size limit)."""
        today = datetime.now().date()
        
        # Check if date has changed
        if today != self.current_date:
            return True
        
        # Check if current file exceeds size limit
        if self.max_size_bytes > 0:
            try:
                if self.current_file_path.exists() and self.current_file_path.stat().st_size >= self.max_size_bytes:
                    return True
            except OSError:
                # If we can't check file size, don't rotate
                pass
        
        return False
    
    def _rotate_if_needed(self):
        """Rotate log file if needed and update current file path."""
        if not self._should_rotate():
            return
        
        with self._lock:
            # Double-check after acquiring lock
            if not self._should_rotate():
                return
            
            # Close current file
            if hasattr(self, 'stream') and self.stream:
                self.stream.close()
                self.stream = None
            
            # Determine new file
            self._determine_current_file()
            
            # Update the handler's file path
            self.baseFilename = str(self.current_file_path)
            
            # Clean up old logs if configured
            if self.auto_delete_old_logs:
                self._cleanup_old_logs()
    
    def _cleanup_old_logs(self):
        """Remove old log files based on retention policy."""
        if not self.auto_delete_old_logs or self.keep_last_n_days <= 0:
            return
        
        try:
            cutoff_date = datetime.now().date() - timedelta(days=self.keep_last_n_days)
            pattern = f"{self.base_filename}-*.log"
            
            for log_file in self.log_folder.glob(pattern):
                # Extract date from filename
                match = re.search(rf"{re.escape(self.base_filename)}-(\d{{4}}-\d{{2}}-\d{{2}})(?:\.\d+)?\.log$", log_file.name)
                if match:
                    try:
                        file_date = datetime.strptime(match.group(1), '%Y-%m-%d').date()
                        if file_date < cutoff_date:
                            log_file.unlink()
                            # Log cleanup action (but avoid infinite recursion)
                            print(f"Deleted old log file: {log_file.name}")
                    except (ValueError, OSError) as e:
                        # Log parsing or deletion error - skip this file
                        print(f"Warning: Could not process log file {log_file.name}: {e}")
        
        except Exception as e:
            # Don't let cleanup errors break logging
            print(f"Warning: Log cleanup failed: {e}")
    
    def emit(self, record):
        """Emit a record, rotating if necessary."""
        try:
            # Check if rotation is needed before writing
            self._rotate_if_needed()
            
            # Call parent emit method
            super().emit(record)
            
        except Exception:
            # Don't let rotation errors break logging
            self.handleError(record)


def _parse_size_string(size_str):
    """
    Parse size string (e.g., '100MB', '1GB') into bytes.
    
    Args:
        size_str: Size string with unit (MB, GB) or integer
        
    Returns:
        Size in bytes, or 0 if parsing fails
    """
    if isinstance(size_str, (int, float)):
        return int(size_str)
    
    if not isinstance(size_str, str):
        return 0
    
    # Remove whitespace and convert to uppercase
    size_str = size_str.strip().upper()
    
    # Extract number and unit
    match = re.match(r'^(\d+(?:\.\d+)?)\s*(MB|GB|M|G)?$', size_str)
    if not match:
        return 0
    
    number = float(match.group(1))
    unit = match.group(2) or ''
    
    # Convert to bytes
    multipliers = {
        'MB': 1024 * 1024,
        'M': 1024 * 1024,
        'GB': 1024 * 1024 * 1024,
        'G': 1024 * 1024 * 1024,
        '': 1  # No unit = bytes
    }
    
    return int(number * multipliers.get(unit, 1))


def _get_rotation_config(log_config):
    """
    Extract and validate rotation configuration from log config.
    
    Args:
        log_config: Logging configuration object or dictionary
        
    Returns:
        Dictionary with rotation settings and defaults
    """
    # Handle both object notation and dictionary access for rotation config
    rotation_config = None
    if hasattr(log_config, 'rotation'):
        rotation_config = log_config.rotation
    elif isinstance(log_config, dict):
        rotation_config = log_config.get('rotation', {})
    
    if not rotation_config:
        # No rotation config - return defaults that disable rotation
        return {
            'auto_delete_old_logs': False,
            'keep_last_n_days': 7,
            'max_log_file_size_bytes': 0
        }
    
    # Extract rotation settings with defaults
    auto_delete = getattr(rotation_config, 'auto_delete_old_logs', None)
    if auto_delete is None:
        auto_delete = rotation_config.get('auto_delete_old_logs', False)
    
    keep_days = getattr(rotation_config, 'keep_last_n_days', None)
    if keep_days is None:
        keep_days = rotation_config.get('keep_last_n_days', 7)
    
    max_size = getattr(rotation_config, 'max_log_file_size', None)
    if max_size is None:
        max_size = rotation_config.get('max_log_file_size', 0)
    
    # Parse and validate size
    max_size_bytes = _parse_size_string(max_size)
    
    # Validate settings
    keep_days = max(1, int(keep_days)) if keep_days else 7
    
    return {
        'auto_delete_old_logs': bool(auto_delete),
        'keep_last_n_days': keep_days,
        'max_log_file_size_bytes': max_size_bytes
    }


def setup_logger(config: Union[dict, Any]) -> logging.Logger:
    """
    Setup logger based on configuration object or dictionary.
    Now supports daily log rotation with size-based splitting and retention management.
    
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
    
    # Extract configuration values with defaults (unchanged from original)
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
    
    # Setup console handler if enabled (unchanged from original)
    if console_enabled:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    # Setup file handler if enabled (enhanced with rotation support)
    if file_enabled:
        log_folder = getattr(log_config, 'folder', None) or log_config.get('folder', 'logs')
        log_file = getattr(log_config, 'file', None) or log_config.get('file', 'app.log')
        
        # Create log directory if it doesn't exist
        Path(log_folder).mkdir(parents=True, exist_ok=True)
        
        # Extract rotation configuration
        rotation_settings = _get_rotation_config(log_config)
        
        # Choose handler based on whether rotation is configured
        if rotation_settings['max_log_file_size_bytes'] > 0 or rotation_settings['auto_delete_old_logs']:
            # Use rotating handler with enhanced features
            base_filename = Path(log_file).stem  # Remove .log extension for rotation naming
            
            try:
                file_handler = DailyRotatingFileHandler(
                    log_folder=log_folder,
                    base_filename=base_filename,
                    max_size_bytes=rotation_settings['max_log_file_size_bytes'],
                    auto_delete_old_logs=rotation_settings['auto_delete_old_logs'],
                    keep_last_n_days=rotation_settings['keep_last_n_days']
                )
            except Exception as e:
                # Fallback to standard FileHandler if rotation setup fails
                logger.warning(f"Failed to setup log rotation, falling back to standard logging: {e}")
                log_path = Path(log_folder) / log_file
                file_handler = logging.FileHandler(log_path)
        else:
            # Use standard handler for backward compatibility (original behavior)
            log_path = Path(log_folder) / log_file
            file_handler = logging.FileHandler(log_path)
        
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


# Initialize logger using AppConfig (unchanged from original)
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
    # Fallback to basic configuration if config loading fails (unchanged from original)
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

# Export the configured logger for backward compatibility (unchanged)
__all__ = ['logger', 'setup_logger']