# core/config_validator.py
from typing import Dict, Any, List, Optional, Tuple


from core.logger_config import logger

class ConfigValidationError(Exception):
    """Raised when configuration validation fails."""
    pass


class ConfigValidator:
    """Validates camera configuration parameters."""
    
    # Required fields that must be present
    REQUIRED_FIELDS = {
        'url': str,
        'threshold': (int, float),
        'area': (int, float)
    }
    
    # Optional fields with defaults and types
    OPTIONAL_FIELDS = {
        'sub_url': (str, None),  # None means fallback to 'url'
        'motion_timeout': ((int, float), 1.5),
        'pre_record_time': ((int, float), 5),
        'post_record_time': ((int, float), 5),
        'fps': ((int, float), 15),
        'width': (int, 1920),
        'height': (int, 1080),
        'name': (str, "Unknown Camera"),
        'enabled': (bool, True)
    }
    
    @classmethod
    def validate_camera_config(cls, camera_id: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate and normalize a single camera configuration.
        
        Args:
            camera_id: Camera identifier
            config: Camera configuration dictionary
            
        Returns:
            Normalized configuration with defaults applied
            
        Raises:
            ConfigValidationError: If validation fails
        """
        if not isinstance(config, dict):
            raise ConfigValidationError(f"Camera '{camera_id}' configuration must be a dictionary")
        
        validated_config = config.copy()
        errors = []
        
        # Validate required fields
        for field, expected_type in cls.REQUIRED_FIELDS.items():
            if field not in config:
                errors.append(f"Missing required field '{field}'")
                continue
                
            value = config[field]
            if not isinstance(value, expected_type):
                type_names = expected_type.__name__ if hasattr(expected_type, '__name__') else str(expected_type)
                errors.append(f"Field '{field}' must be of type {type_names}, got {type(value).__name__}")
        
        # Validate and apply defaults for optional fields
        for field, (expected_type, default_value) in cls.OPTIONAL_FIELDS.items():
            if field in config:
                value = config[field]
                if not isinstance(value, expected_type):
                    type_names = expected_type.__name__ if hasattr(expected_type, '__name__') else str(expected_type)
                    errors.append(f"Field '{field}' must be of type {type_names}, got {type(value).__name__}")
            else:
                # Apply default
                if field == 'sub_url' and default_value is None:
                    # Special case: fallback to main URL
                    validated_config[field] = config.get('url')
                else:
                    validated_config[field] = default_value
                    
        # Additional validation rules
        try:
            cls._validate_ranges(camera_id, validated_config, errors)
            cls._validate_urls(camera_id, validated_config, errors)
        except Exception as e:
            errors.append(f"Validation error: {str(e)}")
        
        if errors:
            error_msg = f"Camera '{camera_id}' configuration validation failed:\n" + "\n".join(f"  - {error}" for error in errors)
            raise ConfigValidationError(error_msg)
        
        return validated_config
    
    @classmethod
    def _validate_ranges(cls, camera_id: str, config: Dict[str, Any], errors: List[str]) -> None:
        """Validate numeric ranges."""
        # Motion detection parameters
        if 'threshold' in config and not (0 <= config['threshold'] <= 255):
            errors.append("'threshold' must be between 0 and 255")
            
        if 'area' in config and config['area'] <= 0:
            errors.append("'area' must be greater than 0")
            
        # Timing parameters
        if 'motion_timeout' in config and config['motion_timeout'] <= 0:
            errors.append("'motion_timeout' must be greater than 0")
            
        if 'pre_record_time' in config and config['pre_record_time'] < 0:
            errors.append("'pre_record_time' must be non-negative")
            
        if 'post_record_time' in config and config['post_record_time'] < 0:
            errors.append("'post_record_time' must be non-negative")
            
        # Video parameters
        if 'fps' in config and not (1 <= config['fps'] <= 60):
            errors.append("'fps' must be between 1 and 60")
            
        if 'width' in config and not (160 <= config['width'] <= 4096):
            errors.append("'width' must be between 160 and 4096")
            
        if 'height' in config and not (120 <= config['height'] <= 2160):
            errors.append("'height' must be between 120 and 2160")
    
    @classmethod
    def _validate_urls(cls, camera_id: str, config: Dict[str, Any], errors: List[str]) -> None:
        """Validate URL formats."""
        for url_field in ['url', 'sub_url']:
            if url_field in config and config[url_field]:
                url = config[url_field]
                if not isinstance(url, str):
                    continue  # Type error already caught above
                    
                if not url.strip():
                    errors.append(f"'{url_field}' cannot be empty")
                    continue
                    
                # Basic URL validation
                if not (url.startswith('rtsp://') or url.startswith('http://') or url.startswith('https://')):
                    errors.append(f"'{url_field}' must start with rtsp://, http://, or https://")
    
    @classmethod
    def validate_all_cameras(cls, cameras_config: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """
        Validate all camera configurations.
        
        Args:
            cameras_config: Dictionary of camera_id -> config
            
        Returns:
            Dictionary of validated and normalized configurations
            
        Raises:
            ConfigValidationError: If any camera configuration is invalid
        """
        if not isinstance(cameras_config, dict):
            raise ConfigValidationError("Cameras configuration must be a dictionary")
        
        if not cameras_config:
            raise ConfigValidationError("No cameras configured")
        
        validated_configs = {}
        all_errors = []
        
        for camera_id, config in cameras_config.items():
            try:
                validated_configs[camera_id] = cls.validate_camera_config(camera_id, config)
                logger.info(f"Camera '{camera_id}' configuration validated successfully")
            except ConfigValidationError as e:
                all_errors.append(str(e))
        
        if all_errors:
            error_msg = "Camera configuration validation failed:\n" + "\n".join(all_errors)
            raise ConfigValidationError(error_msg)
        
        logger.info(f"All {len(validated_configs)} camera configurations validated successfully")
        return validated_configs
    
    @classmethod
    def print_validation_summary(cls, validated_configs: Dict[str, Dict[str, Any]]) -> None:
        """Print a summary of validated configurations."""
        print("\n" + "="*60)
        print("CAMERA CONFIGURATION VALIDATION SUMMARY")
        print("="*60)
        
        for camera_id, config in validated_configs.items():
            print(f"\n📷 {camera_id} ({config.get('name', 'Unknown')})")
            print(f"   URL: {config['url']}")
            if config.get('sub_url') != config['url']:
                print(f"   SUB: {config['sub_url']}")
            print(f"   Motion: threshold={config['threshold']}, area={config['area']}")
            print(f"   Timing: timeout={config['motion_timeout']}s, pre={config['pre_record_time']}s, post={config['post_record_time']}s")
            print(f"   Video: {config['width']}x{config['height']} @ {config['fps']}fps")
            print(f"   Status: {'✅ Enabled' if config.get('enabled', True) else '❌ Disabled'}")
        
        print(f"\n✅ Total cameras validated: {len(validated_configs)}")
        print("="*60)


# Standalone validation script for testing
def main():
    """Standalone validation for testing."""
    import sys
    from core.camera_helper import CameraHelper
    
    try:
        print("🔍 Loading camera configurations...")
        cameras = CameraHelper._load_cameras()
        
        print(f"📋 Found {len(cameras)} cameras to validate")
        
        validated_configs = ConfigValidator.validate_all_cameras(cameras)
        ConfigValidator.print_validation_summary(validated_configs)
        
        print("\n✅ All configurations are valid!")
        
    except ConfigValidationError as e:
        print(f"\n❌ Validation failed:\n{e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()