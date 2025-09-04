# motion_detector.py - Timer Precision Issues Fix

import cv2
import time
from config.tuning_constants import DETECTION_RESIZE_WIDTH
from core.logger_config import logger

# Timer precision constants
TIMING_TOLERANCE = 0.01  # 10ms tolerance for floating-point time comparisons
MIN_MOTION_DURATION = 0.1  # Minimum 100ms for motion detection validity

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

def _time_elapsed_since(start_time, current_time):
    """
    Calculate elapsed time with precision handling.
    
    Args:
        start_time: Start timestamp
        current_time: Current timestamp
        
    Returns:
        Elapsed time in seconds, or 0 if invalid
    """
    if start_time is None or current_time is None:
        return 0.0
    
    elapsed = current_time - start_time
    return max(0.0, elapsed)  # Ensure non-negative result

def _is_timeout_reached(start_time, current_time, timeout_duration):
    """
    Check if timeout has been reached with precision tolerance.
    
    Args:
        start_time: When the timer started
        current_time: Current timestamp
        timeout_duration: Required duration for timeout
        
    Returns:
        True if timeout has been reached (within tolerance)
    """
    if start_time is None:
        return False
    
    elapsed = _time_elapsed_since(start_time, current_time)
    
    # Use tolerance to handle floating-point precision issues
    # Timeout is reached if elapsed time is within tolerance of target duration
    return elapsed >= (timeout_duration - TIMING_TOLERANCE)

def _is_motion_duration_valid(start_time, current_time):
    """
    Check if motion has been detected for a valid minimum duration.
    
    Args:
        start_time: When motion detection started
        current_time: Current timestamp
        
    Returns:
        True if motion duration meets minimum threshold
    """
    if start_time is None:
        return False
    
    elapsed = _time_elapsed_since(start_time, current_time)
    return elapsed >= MIN_MOTION_DURATION


def _reset_motion_state(state):
    """Reset motion detection state to initial values."""
    state['motion_start_time'] = None
    state['motion_confirmed'] = False
    state['is_waiting'] = False

def _is_motion_cooldown_active(state, current_time):
    """
    Check if motion detection is in cooldown period with precision handling.
    
    Args:
        state: Motion detection state dictionary
        current_time: Current timestamp
        
    Returns:
        True if still in cooldown period
    """
    cooldown_until = state.get('motion_cooldown_until', 0)
    
    # Use tolerance to handle floating-point precision
    return current_time < (cooldown_until + TIMING_TOLERANCE)

def _should_confirm_motion(state, current_time, timeout_duration):
    """
    Check if motion should be confirmed based on duration with precision handling.
    
    Args:
        state: Motion detection state dictionary
        current_time: Current timestamp
        timeout_duration: Required duration for motion confirmation
        
    Returns:
        True if motion should be confirmed
    """
    motion_start_time = state.get('motion_start_time')
    
    if motion_start_time is None:
        return False
    
    # Check if timeout has been reached using precision-aware comparison
    return _is_timeout_reached(motion_start_time, current_time, timeout_duration)

def _start_motion_timer(state, current_time, timeout_duration):
    """
    Start motion detection timer with timestamp validation.
    
    Args:
        state: Motion detection state dictionary
        current_time: Current timestamp
        timeout_duration: Required duration for confirmation
    """
    # Validate current_time is reasonable
    if current_time <= 0:
        logger.warning("Invalid timestamp for motion timer start")
        return
    
    state['motion_start_time'] = current_time
    state['is_waiting'] = True
    state['motion_confirmed'] = False
    
    logger.info(f"Motion detected - waiting {timeout_duration:.1f}s for confirmation")

def _confirm_motion_and_start_recording(state, current_time, recording_manager, frame):
    """
    Confirm motion and trigger recording start with timing validation.
    
    Args:
        state: Motion detection state dictionary
        current_time: Current timestamp
        recording_manager: Recording manager instance
        frame: Current frame for recording
    """
    # Validate that we actually waited long enough
    motion_start_time = state.get('motion_start_time')
    if motion_start_time is not None:
        actual_elapsed = _time_elapsed_since(motion_start_time, current_time)
        logger.debug(f"Motion confirmed after {actual_elapsed:.2f}s")
    
    logger.info("Motion confirmed - starting recording")
    
    # Start recording
    recording_manager.start(current_time, frame)
    
    # Update state with validated timestamp
    state['motion_confirmed'] = True
    state['last_motion_time'] = current_time
    state['is_recording'] = True
    state['is_recording_triggered'] = True
    state['is_waiting'] = False
    _reset_motion_state(state)

def handle_motion_detection(prev_frame, gray, state, config, frame, current_time, recording_manager):
    """
    Motion detection with precision-aware timing logic.
    
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
    # Validate current_time parameter
    if current_time <= 0:
        logger.warning("Invalid timestamp in motion detection")
        return False
    
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
    
    # Handle motion detection logic with precision-aware timing
    if is_motion_detected:
        # Update last motion time for ongoing motion
        if state.get('motion_confirmed', False):
            state['last_motion_time'] = current_time
            return True
        
        # Start motion timer if not already started
        if state.get('motion_start_time') is None:
            _start_motion_timer(state, current_time, config.motion_time_out)
            return True
        
        # Check if motion has been sustained long enough (with precision handling)
        if _should_confirm_motion(state, current_time, config.motion_time_out):
            # Additional validation: ensure minimum motion duration
            if _is_motion_duration_valid(state.get('motion_start_time'), current_time):
                if not state.get('motion_confirmed', False):
                    _confirm_motion_and_start_recording(state, current_time, recording_manager, frame)
            else:
                logger.debug("Motion detected but duration too short for confirmation")
        
        return True
    
    else:
        # No motion detected - handle state cleanup
        if state.get('is_waiting', False):
            # Motion was being tracked but stopped before confirmation
            motion_start_time = state.get('motion_start_time')
            if motion_start_time is not None:
                elapsed = _time_elapsed_since(motion_start_time, current_time)
                required_time = config.motion_time_out
                
                if elapsed < (required_time - TIMING_TOLERANCE):
                    logger.info(f"Motion interrupted after {elapsed:.2f}s (needed {required_time:.1f}s)")
        
        # Reset motion state when no motion is detected
        _reset_motion_state(state)
        return False


def check_motion_confirmation(state, current_time):
    """
    Check if motion has been confirmed with timing validation.
    
    Args:
        state: Motion detection state dictionary
        current_time: Current timestamp
        
    Returns:
        True if motion is confirmed, False otherwise
    """
    # Validate timestamp
    if current_time <= 0:
        return False
        
    return state.get('motion_confirmed', False)
def set_motion_cooldown(state, current_time, cooldown_duration):
    """
    Set motion detection cooldown period with precision handling.
    
    Args:
        state: Motion detection state dictionary
        current_time: Current timestamp
        cooldown_duration: Cooldown duration in seconds
    """
    if current_time <= 0 or cooldown_duration < 0:
        logger.warning("Invalid parameters for motion cooldown")
        return
    
    # Set cooldown end time with proper precision
    state['motion_cooldown_until'] = current_time + cooldown_duration
    logger.debug(f"Motion cooldown set for {cooldown_duration:.1f}s")