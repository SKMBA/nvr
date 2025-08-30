# core\camera_helper.py
import os 
from core .config_loader import load_config 
import socket 
from urllib .parse import urlparse 
CONFIG_DIR =os .path .join (os .path .dirname (os .path .dirname (__file__ )),'config')
CAMERA_CONFIG_FILE =os .path .join (CONFIG_DIR ,'cameras.json')
class CameraHelper :
    @staticmethod 
    def _is_port_open (url ,timeout =1.0 ):
        parsed =urlparse (url )
        if not parsed .hostname :
            return False 
        port =parsed .port 
        if not port :
            if parsed .scheme .startswith ("rtsp"):
                port =554 
            elif parsed .scheme =="https":
                port =443 
            else :
                port =80 
        try :
            with socket .create_connection ((parsed .hostname ,port ),timeout =timeout ):
                return True 
        except (socket .timeout ,ConnectionRefusedError ,OSError ):
            return False 
    
    @staticmethod 
    def _load_cameras ():
        return load_config (CAMERA_CONFIG_FILE )
    
    @staticmethod 
    def get_camera_by_id (camera_id ):
        cameras =CameraHelper ._load_cameras ()
        return cameras .get (camera_id )
    
    @staticmethod 
    def get_camera_main_url (camera_id ,timeout =1.5):
        from core.logger_config import logger
        cam =CameraHelper .get_camera_by_id (camera_id )
        if not cam :
            logger.error (f"Camera '{camera_id }' not found")
            return None 
        main_url =cam .get ("url")
        if main_url :
            if CameraHelper ._is_port_open (main_url ,timeout ):
                logger.info (f"Using main stream for Camera {camera_id }")
                return main_url 
            else :
                logger.warning (f"Main stream unreachable for Camera {camera_id }")
        else :
            logger.error (f"Invalid main stream for Camera {camera_id }")
        return None 
    
    @staticmethod 
    def get_camera_sub_url (camera_id ,timeout =1.5):
        from core.logger_config import logger
        cam =CameraHelper .get_camera_by_id (camera_id )
        if not cam :
            logger.error (f"Camera '{camera_id }' not found")
            return None 
        sub_url =cam .get ("sub_url")

        if sub_url :
            if CameraHelper ._is_port_open (sub_url ,timeout ):
                logger.info (f"Using substream for Camera {camera_id }")
                return sub_url 
            else :
                logger.warning (f"Substream unreachable for Camera {camera_id }")
        else :
            logger.error (f"Invalid sub stream for Camera {camera_id }")
        return None 
            
    @staticmethod 
    def get_camera_url (camera_id ,timeout =1.5):
        from core.logger_config import logger
        cam =CameraHelper .get_camera_by_id (camera_id )
        if not cam :
            logger.error (f"Camera '{camera_id }' not found")
            return None 
        main_url =cam .get ("url")
        sub_url =cam .get ("sub_url")

        if main_url :
            if CameraHelper ._is_port_open (main_url ,timeout ):
                logger.info (f"Using main stream for Camera {camera_id }")
                return main_url 
            else :
                logger.warning (f"Main stream unreachable for Camera {camera_id }")
        else :
            logger.error (f"Invalid main stream for Camera {camera_id }")
            
        if sub_url :
            if CameraHelper ._is_port_open (sub_url ,timeout ):
                logger.info (f"Using substream for Camera {camera_id }")
                return sub_url 
            else :
                logger.warning (f"Substream unreachable for Camera {camera_id }")
        else :
            logger.error (f"Invalid sub stream for Camera {camera_id }")
            
        logger.critical (f"Both main and substream unreachable for Camera {camera_id }")
        return None 
    
    @staticmethod 
    def get_camera_param (camera_id ,param ):
        cam =CameraHelper .get_camera_by_id (camera_id )
        return cam .get (param )if cam else None 

    @staticmethod 
    def get_all_camera_ids ():
        return list (CameraHelper ._load_cameras ().keys ())

    @staticmethod 
    def get_all_cameras ():
        return list (CameraHelper ._load_cameras ().values ())
