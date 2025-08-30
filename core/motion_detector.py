# core\motion_detector.py
import cv2 
import time 
import datetime 
from config .tuning_constants import DETECTION_RESIZE_WIDTH 
from core .logger_config import logger 
def preprocess_frame (frame ):
    gray =cv2 .cvtColor (frame ,cv2 .COLOR_BGR2GRAY )
    resized_height =int (frame .shape [0 ]*DETECTION_RESIZE_WIDTH /frame .shape [1 ])
    gray =cv2 .resize (gray ,(DETECTION_RESIZE_WIDTH ,resized_height ))
    gray =cv2 .GaussianBlur (gray ,(21 ,21 ),0 )
    return gray 
def detect_motion (prev_frame ,current_frame_gray ,motion_threshold ,min_contour_area ):
    delta =cv2 .absdiff (prev_frame ,current_frame_gray )
    thresh =cv2 .threshold (delta ,motion_threshold ,255 ,cv2 .THRESH_BINARY )[1 ]
    thresh =cv2 .dilate (thresh ,None ,iterations =2 )
    contours ,_ =cv2 .findContours (thresh .copy (),cv2 .RETR_EXTERNAL ,cv2 .CHAIN_APPROX_SIMPLE )
    return any (cv2 .contourArea (contour )>=min_contour_area for contour in contours )
def handle_motion_detection (prev_frame ,gray ,state ,config ,frame ,current_time ,recording_manager ):
    is_motion_detected =False 
    if current_time <state ['motion_cooldown_until']:
        state ['motion_start_time']=None 
        state ['wait_expiry_time']=None 
        state ['is_waiting']=False 
    else :
        is_motion_detected =detect_motion (
        prev_frame ,gray ,config .threshold ,config .min_contour_area 
        )
        if is_motion_detected :
            if not state ['motion_confirmed']:
                if state ['motion_start_time']is None :
                    state ['motion_start_time']=current_time 
                    state ['wait_expiry_time']=current_time +config .motion_time_out 
                    if state ['debug']:
                        logger .info (
                        "Motion detected - Waiting %.1f seconds before confirming...",
                        config .motion_time_out 
                        )
                state ['is_waiting']=True 
                if check_motion_confirmation (state ,current_time ):
                    if state ['debug']:
                        logger .info (
                        "Motion confirmed after %.1f seconds - Starting recording",
                        config .motion_time_out 
                        )
                    recording_manager .start (current_time ,frame )
                    state ['is_recording']=True 
                    state ['is_recording_triggered']=True 
                    print ("<<Before RECORDING START-motion_detector.py")
                    print (f"state['motion_confirmed']: {state ['motion_confirmed']}")
                    print (f"state['last_motion_time']: {state ['last_motion_time']}")
                    print (f"state['motion_start_time']:	{state ['motion_start_time']}")
                    print (f"state['is_waiting']: {state ['is_waiting']}")
                    state ['motion_confirmed']=True 
                    state ['last_motion_time']=current_time 
                    state ['motion_start_time']=None 
                    state ['is_waiting']=False 
                    print (">>After RECORDING START-motion_detector.py")
                    print (f"state['motion_confirmed']: {state ['motion_confirmed']}")
                    print (f"state['last_motion_time']: {state ['last_motion_time']}")
                    print (f"state['motion_start_time']:	{state ['motion_start_time']}")
                    print (f"state['is_waiting']: {state ['is_waiting']}")
            else :
                state ['last_motion_time']=current_time 
                state ['motion_start_time']=None 
                state ['wait_expiry_time']=None 
                state ['is_waiting']=False 
        else :
            if state ['motion_start_time']is not None :
                if current_time <state ['wait_expiry_time']:
                    state ['is_waiting']=True 
                else :
                    state ['motion_start_time']=None 
                    state ['wait_expiry_time']=None 
                    state ['is_waiting']=False 
                    if state ['debug']:
                        logger .info (
                        "Motion interrupted before %.1f seconds confirmation.",
                        config .motion_time_out 
                        )
            else :
                state ['is_waiting']=False 
    return is_motion_detected 
def check_motion_confirmation (state ,current_time ):
    if state ['motion_start_time']is None :
        return False 
    return current_time >=state ['wait_expiry_time']
def is_motion_detectedx (prev_frame ,current_frame ,threshold =25 ,min_area =500 ):
    if len (prev_frame .shape )==3 :
        prev_gray =cv2 .cvtColor (prev_frame ,cv2 .COLOR_BGR2GRAY )
    else :
        prev_gray =prev_frame .copy ()
    if len (current_frame .shape )==3 :
        curr_gray =cv2 .cvtColor (current_frame ,cv2 .COLOR_BGR2GRAY )
    else :
        curr_gray =current_frame .copy ()
    prev_blur =cv2 .GaussianBlur (prev_gray ,(21 ,21 ),0 )
    curr_blur =cv2 .GaussianBlur (curr_gray ,(21 ,21 ),0 )
    delta =cv2 .absdiff (prev_blur ,curr_blur )
    thresh =cv2 .threshold (delta ,threshold ,255 ,cv2 .THRESH_BINARY )[1 ]
    thresh =cv2 .dilate (thresh ,None ,iterations =2 )
    contours ,_ =cv2 .findContours (thresh .copy (),cv2 .RETR_EXTERNAL ,cv2 .CHAIN_APPROX_SIMPLE )
    for contour in contours :
        if cv2 .contourArea (contour )>=min_area :
            return True 
    return False 
