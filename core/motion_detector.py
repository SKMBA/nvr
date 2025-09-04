# motion_detector.py - Simplified State Management Fix

import cv2
import time
from config.tuning_constants import DETECTION_RESIZE_WIDTH
from core.logger_config import logger

def preprocess_frame(frame):
    """Preprocess frame for motion detection - unchanged."""
    gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
    resized_height = int(frame.shape[0] * DETECTION_RESIZE_WIDTH / frame.shape[1])
    gray = cv2.resize(gray, (DETECTION_RESIZE_WIDTH, resized_height))
    gray = cv2.GaussianBlur(gray, (21, 21), 0)
    return gray

def detect_motion(prev_frame, current_frame_gray, motion_threshold, min_contour_area):
    """Detect motion between frames - unchanged."""
    delta = cv2.absdiff(prev_frame, current_frame_gray)
    thresh = cv2.threshold(delta, motion_threshold, 255, cv2.THRESH_BINARY)[1]
    thresh = cv2.dilate(thresh, None, iterations=2)
    contours, _ = cv2.findContours(thresh.copy(), cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    return any(cv2.contourArea(contour) >= min_contour_area for contour in contours)

def _reset_motion_state(state):
    """Reset motion detection state to initial values."""
    state['motion_start_time'] = None
    state['motion_confirmed'] = False
    state['is_waiting'] = False

def _is_motion_cooldown_active(state, current_time):
    """Check if motion detection is in cooldown period."""
    return current_time < state.get('motion_cooldown_until', 0)

def _should_confirm_motion(state, current_time, timeout_duration):
    """
    Check if motion should be confirmed based on duration.
    
    Args:
        state: Motion detection state dictionary
        current_time: Current timestamp
        timeout_duration: Required duration for motion confirmation
        
    Returns:
        True if motion should be confirmed
    """
    if state['motion_start_time'] is None:
        return False
    
    elapsed_time = current_time - state['motion_start_time']
    return elapsed_time >= timeout_duration

def _start_motion_timer(state, current_time, timeout_duration):
    """Start motion detection timer."""
    state['motion_start_time'] = current_time
    state['is_waiting'] = True
    state['motion_confirmed'] = False
    
    logger.info(f"Motion detected - waiting {timeout_duration:.1f}s for confirmation")

def _confirm_motion_and_start_recording(state, current_time, recording_manager, frame):
    """Confirm motion and trigger recording start."""
    logger.info("Motion confirmed - starting recording")
    
    # Start recording
    recording_manager.start(current_time, frame)
    
    # Update state
    state['motion_confirmed'] = True
    state['last_motion_time'] = current_time
    state['is_recording'] = True
    state['is_recording_triggered'] = True
    state['is_waiting'] = False
    _reset_motion_state(state)

def handle_motion_detection(prev_frame, gray, state, config, frame, current_time, recording_manager):
    """
    Simplified motion detection state management.
    
    Args:
        prev_frame: Previous frame for comparison
        gray: Current grayscale frame
        state: Motion detection state dictionary
        config: Configuration object with motion parameters
        frame: Current frame for recording
        current_time: Current timestamp
        recording_manager: Recording manager instance
        
    Returns:
        True if motion is detected, False otherwise
    """
    # Check if we're in cooldown period
    if _is_motion_cooldown_active(state, current_time):
        _reset_motion_state(state)
        return False
    
    # Detect motion in current frame
    is_motion_detected = detect_motion(
        prev_frame, 
        gray, 
        config.threshold, 
        config.min_contour_area
    )
    
    # Handle motion detection logic
    if is_motion_detected:
        # Update last motion time for ongoing motion
        if state['motion_confirmed']:
            state['last_motion_time'] = current_time
            return True
        
        # Start motion timer if not already started
        if state['motion_start_time'] is None:
            _start_motion_timer(state, current_time, config.motion_time_out)
            return True
        
        # Check if motion has been sustained long enough
        if _should_confirm_motion(state, current_time, config.motion_time_out):
            if not state['motion_confirmed']:
                _confirm_motion_and_start_recording(state, current_time, recording_manager, frame)
        
        return True
    
    else:
        # No motion detected
        if state['is_waiting']:
            # Motion was being tracked but stopped before confirmation
            if state['motion_start_time'] is not None:
                elapsed = current_time - state['motion_start_time']
                if elapsed < config.motion_time_out:
                    logger.info(f"Motion interrupted after {elapsed:.1f}s (needed {config.motion_time_out:.1f}s)")
            
        # Reset motion state when no motion is detected
        _reset_motion_state(state)
        return False

def check_motion_confirmation(state, current_time):
    """
    Check if motion has been confirmed (simplified version).
    
    Args:
        state: Motion detection state dictionary
        current_time: Current timestamp
        
    Returns:
        True if motion is confirmed, False otherwise
    """
    return state.get('motion_confirmed', False)