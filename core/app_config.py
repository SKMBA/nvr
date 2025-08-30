# core/app_config.py
import json
import copy
from pathlib import Path
from typing import Dict, Any, Union, List
import importlib.resources as pkg_resources
from logger import config as config_pkg  # <- your packaged config folder

class Dict2Obj:
    """Helper to convert dict -> object recursively for attribute access."""
    
    def __init__(self, d: Union[Dict, List, Any]):
        if isinstance(d, dict):
            for k, v in d.items():
                setattr(self, k, self._convert_value(v))
        elif isinstance(d, list):
            # For lists, just store as-is since we can't set attributes
            self.__dict__.update({'_list_data': d})
    
    def _convert_value(self, value: Any) -> Any:
        """Recursively convert nested structures."""
        if isinstance(value, dict):
            return Dict2Obj(value)
        elif isinstance(value, list):
            return [Dict2Obj(item) if isinstance(item, dict) else item for item in value]
        return value
    
    def __getattr__(self, name: str) -> Any:
        """Handle list access for converted list objects."""
        if name == '_list_data':
            return super().__getattribute__(name)
        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")

class AppConfig:
    """Application configuration manager with fallback to packaged defaults."""
    
    def __init__(self, config_path: Union[str, Path, None] = None, filename: str = "app_config.json"):
        self.config_path = Path(config_path) if config_path else None
        self.filename = filename
        self._data: Dict[str, Any] = {}
        self._load_config()
    
    def _load_config(self) -> None:
        """Load JSON config from user path if given, otherwise from packaged defaults."""
        try:
            if self.config_path and self.config_path.exists():
                with open(self.config_path, "r", encoding="utf-8") as f:
                    self._data = json.load(f)
            else:
                # Load from packaged config
                with pkg_resources.files(config_pkg).joinpath(self.filename).open("r", encoding="utf-8") as f:
                    self._data = json.load(f)
        except (json.JSONDecodeError, IOError, AttributeError) as e:
            raise RuntimeError(f"Failed to load configuration from {self.config_path or 'packaged config'}: {e}")
    
    def get_config(self, as_object: bool = True) -> Union[Dict2Obj, Dict[str, Any]]:
        """
        Return deep copy of config data.
        
        Args:
            as_object (bool): If True, return as nested object with attribute access.
            
        Returns:
            Configuration data as object or dictionary
        """
        data_copy = copy.deepcopy(self._data)
        return Dict2Obj(data_copy) if as_object else data_copy
    
    def get_section(self, section: str, as_object: bool = True) -> Union[Dict2Obj, Dict[str, Any], None]:
        """
        Get a specific configuration section.
        
        Args:
            section: Section name (e.g., 'logging', 'database')
            as_object: If True, return as object with attribute access
            
        Returns:
            Section data or None if not found
        """
        section_data = self._data.get(section)
        if section_data is None:
            return None
        
        data_copy = copy.deepcopy(section_data)
        return Dict2Obj(data_copy) if as_object else data_copy
    
    @property
    def data(self) -> Dict[str, Any]:
        """Return a deep copy of the config data to prevent external mutation."""
        return copy.deepcopy(self._data)
    
    def reload(self) -> None:
        """Reload configuration from file."""
        self._load_config()
    
    def __repr__(self) -> str:
        return f"AppConfig(config_path={self.config_path}, filename={self.filename})"