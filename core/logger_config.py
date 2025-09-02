# core\logger_config.py
import logging
import os
import json
import re
import threading
from pathlib import Path
from typing import Union, Any, Optional
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


# Global configuration and shared handlers
_shared_handlers = []
_config_loaded = False
_config_lock = threading.Lock()


def _setup_shared_handlers(config: Union[dict, Any]) -> list:
    """Setup shared handlers that all loggers can use."""
    if hasattr(config, 'logging'):
        log_config = config.logging
    else:
        log_config = config.get("logging", {})
    
    handlers = []
    
    # Extract configuration
    console_enabled = getattr(log_config, 'console_enabled', None)
    if console_enabled is None:
        console_enabled = log_config.get('console_enabled', True)
    
    file_enabled = getattr(log_config, 'file_enabled', None)
    if file_enabled is None:
        file_enabled = log_config.get('file_enabled', False)
    
    log_format = getattr(log_config, 'format', None) or log_config.get('format', '[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s')
    date_format = getattr(log_config, 'datefmt', None) or log_config.get('datefmt', '%Y-%m-%d %H:%M:%S')
    
    formatter = logging.Formatter(log_format, datefmt=date_format)
    
    # Console handler
    if console_enabled:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        handlers.append(console_handler)
    
    # File handler
    if file_enabled:
        log_folder = getattr(log_config, 'folder', None) or log_config.get('folder', 'logs')
        log_file = getattr(log_config, 'file', None) or log_config.get('file', 'app.log')
        
        Path(log_folder).mkdir(parents=True, exist_ok=True)
        rotation_settings = _get_rotation_config(log_config)
        
        if rotation_settings['max_log_file_size_bytes'] > 0 or rotation_settings['auto_delete_old_logs']:
            base_filename = Path(log_file).stem
            try:
                file_handler = DailyRotatingFileHandler(
                    log_folder=log_folder,
                    base_filename=base_filename,
                    max_size_bytes=rotation_settings['max_log_file_size_bytes'],
                    auto_delete_old_logs=rotation_settings['auto_delete_old_logs'],
                    keep_last_n_days=rotation_settings['keep_last_n_days']
                )
            except Exception as e:
                # Fallback to standard logging
                log_path = Path(log_folder) / log_file
                file_handler = logging.FileHandler(log_path)
        else:
            log_path = Path(log_folder) / log_file
            file_handler = logging.FileHandler(log_path)
        
        file_handler.setFormatter(formatter)
        handlers.append(file_handler)
    
    return handlers


def _load_shared_config():
    """Load configuration and setup shared handlers once."""
    global _shared_handlers, _config_loaded
    
    with _config_lock:
        if _config_loaded:
            return
        
        try:
            app_config = AppConfig(str(Path("config\\app_config.json").resolve()))
            config_obj = app_config.get_config()
            _shared_handlers = _setup_shared_handlers(config_obj)
            _config_loaded = True
        except (FileNotFoundError, json.JSONDecodeError, Exception) as e:
            # Fallback to default console handler
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S"
            )
            handler.setFormatter(formatter)
            _shared_handlers = [handler]
            _config_loaded = True
            
            # Log warning using a temporary logger
            temp_logger = logging.getLogger("logger_config")
            temp_logger.setLevel(logging.DEBUG)
            temp_logger.addHandler(handler)
            temp_logger.warning(f"Failed to load config, using default logging: {e}")


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    Get a logger for the calling module.
    
    Args:
        name: Logger name. If None, uses the calling module's __name__.
              For backward compatibility, 'nvr_logger' is still supported.
    
    Returns:
        Configured logger instance with shared handlers.
    
    Example:
        # Automatic module-based naming (recommended)
        logger = get_logger()  # Will use __name__ from calling module
        
        # Explicit naming (backward compatibility)
        logger = get_logger('nvr_logger')
        
        # Custom naming
        logger = get_logger('camera_worker.Tapo')
    """
    # Ensure shared configuration is loaded
    _load_shared_config()
    
    # Determine logger name
    if name is None:
        # Auto-detect calling module name
        import inspect
        frame = inspect.currentframe()
        try:
            # Go up the call stack to find the calling module
            caller_frame = frame.f_back
            if caller_frame and '__name__' in caller_frame.f_globals:
                name = caller_frame.f_globals['__name__']
            else:
                name = 'nvr_logger'  # Fallback
        finally:
            del frame  # Prevent reference cycles
    
    # Get or create logger
    logger_instance = logging.getLogger(name)
    
    # Configure logger if not already configured
    if not logger_instance.handlers:
        logger_instance.setLevel(logging.DEBUG)
        
        # Add shared handlers
        for handler in _shared_handlers:
            logger_instance.addHandler(handler)
        
        # Prevent propagation to avoid duplicate messages
        logger_instance.propagate = False
    
    return logger_instance


def setup_logger(config: Union[dict, Any]) -> logging.Logger:
    """
    Legacy function for backward compatibility.
    
    Args:
        config: Configuration object or dictionary
    
    Returns:
        Logger instance (defaults to 'nvr_logger' for compatibility)
    """
    global _shared_handlers, _config_loaded
    
    with _config_lock:
        _shared_handlers = _setup_shared_handlers(config)
        _config_loaded = True
    
    return get_logger('nvr_logger')


def get_module_logger() -> logging.Logger:
    """
    Convenience function to get a logger for the calling module.
    Equivalent to get_logger() but more explicit.
    
    Returns:
        Logger instance named after the calling module.
    """
    return get_logger()


def reset_logging_config():
    """Reset the logging configuration. Useful for tests."""
    global _shared_handlers, _config_loaded
    
    with _config_lock:
        # Clear all existing loggers
        for logger_name in list(logging.Logger.manager.loggerDict.keys()):
            logger_instance = logging.getLogger(logger_name)
            logger_instance.handlers.clear()
            logger_instance.setLevel(logging.NOTSET)
        
        _shared_handlers.clear()
        _config_loaded = False


# Initialize shared configuration on module import
_load_shared_config()

# Backward compatibility: create the default nvr_logger
logger = get_logger('nvr_logger')

__all__ = ['logger', 'get_logger', 'get_module_logger', 'setup_logger', 'reset_logging_config']