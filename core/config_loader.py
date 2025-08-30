# core\config_loader.py
import json 
import os 
CONFIG_DIR =os .path .join (os .path .dirname (os .path .dirname (__file__ )),'config')
CONFIG_FILE =os .path .join (CONFIG_DIR ,'cameras.json')
def load_config (config_path =CONFIG_FILE ):
    if not os .path .isfile (config_path ):
        raise FileNotFoundError (f"Config file not found: {config_path }")
    with open (config_path ,'r')as f :
        try :
            config =json .load (f )
        except json .JSONDecodeError as e :
            raise ValueError (f"Invalid JSON in config file: {e }")
    return config 
def load_config_forcamera (config_path =CONFIG_FILE ):
    if not os .path .isfile (config_path ):
        raise FileNotFoundError (f"Config file not found: {config_path }")
    with open (config_path ,'r')as f :
        try :
            config =json .load (f )
        except json .JSONDecodeError as e :
            raise ValueError (f"Invalid JSON in config file: {e }")
    if not isinstance (config ,dict )or 'cameras'not in config :
        raise ValueError ("Config must contain a 'cameras' dictionary")
    cameras =config ['cameras']
    if not isinstance (cameras ,list ):
        raise ValueError ("'cameras' must be a list of camera definitions")
    try :
        return {cam ['id']:cam for cam in cameras }
    except KeyError :
        raise ValueError ("Each camera entry must have an 'id' field")
