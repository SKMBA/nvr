from pathlib import Path
from core.app_config import AppConfig

CONFIG_DIR = Path(__file__).resolve().parent.parent / "config"

def load_app_config():
    return AppConfig(CONFIG_DIR / "app_config.json").get_config()

def load_camera_config():
    return AppConfig(CONFIG_DIR / "cameras.json").get_config()

def load_tuning_constants():
    return AppConfig(CONFIG_DIR / "tuning_constants.json").get_config()

def load_ui_constants():
    return AppConfig(CONFIG_DIR / "ui_constants.json").get_config()
