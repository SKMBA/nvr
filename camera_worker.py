# camera_worker.py - Complete file with thread-safe recording state management
import threading
import time
import signal
import sys
import cv2
import os
import numpy as np
from typing import Optional, Dict, Any
from dataclasses import asdict
from datetime import datetime
import multiprocessing as mp
import queue

from core.camera_helper import CameraHelper
from core.ffmpeg_recorder import FFmpegRecorder
from core.motion_detector import detect_motion, preprocess_frame
from supervisor import HeartbeatMessage, CommandMessage

from core.logger_config import get_logger
logger = get_logger()  # Will show [camera_worker] in logs

class CameraWorker:
    """Camera worker process that handles SUB stream (preview + motion) and MAIN stream (recording)."""
    # camera_worker.py - Motion Trigger Race Condition Fix

    # 1. UPDATED __init__ METHOD - Add motion trigger state tracking
    # 1. UPDATED __init__ METHOD - Add stream health tracking for recording
    def __init__(self, camera_id: str, cmd_queue: mp.Queue, status_queue: mp.Queue):
        global logger
        logger = get_logger(f'camera_worker.{camera_id}')

        self.camera_id = camera_id
        self.cmd_queue = cmd_queue
        self.status_queue = status_queue
        
        # Camera configuration
        self.camera_config = CameraHelper.get_camera_by_id(camera_id)
        if not self.camera_config:
            raise ValueError(f"Camera {camera_id} not found in configuration")
            
        # Stream URLs
        self.sub_url = self.camera_config.get('sub_url')
        self.main_url = self.camera_config.get('url')
        
        # State
        self.running = True
        self.motion_detected = False
        self.last_fps = 0.0
        self.error_message: Optional[str] = None
        
        # Threads
        self.sub_thread: Optional[threading.Thread] = None
        self.main_thread: Optional[threading.Thread] = None
        self.heartbeat_thread: Optional[threading.Thread] = None
        self.command_thread: Optional[threading.Thread] = None
        
        # Enhanced synchronization for recording state
        self.lock = threading.RLock()  # Use RLock for nested locking
        self.recording_event = threading.Event()
        self.stop_recording_event = threading.Event()
        
        # Private recording state (always access through methods)
        self._recording = False
        self._recording_start_time = None
        
        # Motion trigger state management (NEW - prevents duplicate triggers)
        self.motion_start_time: Optional[float] = None
        self.motion_trigger_sent = False  # Track if recording event already triggered
        self.last_motion_trigger_time = 0.0  # Debounce rapid triggers
        self.motion_trigger_cooldown = 2.0  # Minimum seconds between triggers
        
        # Stream health tracking for recording control (NEW)
        self.stream_health_failures = 0  # Track consecutive stream failures during recording
        self.max_stream_failures_during_recording = 3  # Stop recording after 3 consecutive failures
        self.last_valid_stream_time = time.time()  # Track when stream was last healthy
        self.max_stream_downtime_during_recording = 30.0  # Stop recording after 30s of stream issues
        
        # Recording
        self.recorder: Optional[FFmpegRecorder] = None
        self.motion_timeout = self.camera_config.get('motion_timeout', 1.5)
        self.post_record_time = self.camera_config.get('post_record_time', 5.0)
        
        logger.info(f"CameraWorker initialized for {camera_id}")
        
    # Thread-safe recording state access methods
    def _is_recording(self) -> bool:
        """Thread-safe check if currently recording."""
        with self.lock:
            return self._recording
    
    def _set_recording_state(self, recording: bool) -> bool:
        """
        Thread-safe recording state change.
        
        Returns:
            True if state changed, False if already in that state
        """
        with self.lock:
            if self._recording == recording:
                return False  # No change needed
                
            old_state = self._recording
            self._recording = recording
            
            if recording:
                self._recording_start_time = time.time()
            else:
                self._recording_start_time = None
                
            logger.debug(f"Recording state change for {self.camera_id}: {old_state} -> {recording}")
            return True
    
    def _get_recording_duration(self) -> float:
        """Get current recording duration in seconds."""
        with self.lock:
            if self._recording and self._recording_start_time:
                return time.time() - self._recording_start_time
            return 0.0

    # Property for backward compatibility
    @property 
    def recording(self) -> bool:
        return self._is_recording()
        
    @classmethod
    def run(cls, camera_id: str, cmd_queue: mp.Queue, status_queue: mp.Queue):
        """Entry point for worker process."""
        # Set up signal handlers for graceful shutdown
        def signal_handler(signum, frame):
            logger.info(f"Worker {camera_id} received signal {signum}")
            worker.stop()
            
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        try:
            worker = cls(camera_id, cmd_queue, status_queue)
            worker.start()
        except Exception as e:
            logger.error(f"Worker {camera_id} failed to start: {e}")
            sys.exit(1)
            
    def start(self):
        """Start all worker threads."""
        logger.info(f"Starting camera worker for {self.camera_id}")
        
        try:
            # Start threads
            self.heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
            self.command_thread = threading.Thread(target=self._command_loop, daemon=True)
            self.sub_thread = threading.Thread(target=self._sub_stream_loop, daemon=True)
            self.main_thread = threading.Thread(target=self._main_stream_loop, daemon=True)
            
            self.heartbeat_thread.start()
            self.command_thread.start()
            self.sub_thread.start()
            self.main_thread.start()
            
            logger.info(f"Camera worker {self.camera_id} started successfully")
            
            # Keep main thread alive
            while self.running:
                time.sleep(1.0)
                
        except Exception as e:
            logger.error(f"Error starting camera worker {self.camera_id}: {e}")
            self.error_message = str(e)
        finally:
            self.stop()
            
    # 3. UPDATED stop METHOD - Safe queue cleanup and exception handling
  # 3. UPDATED stop METHOD - Safe queue cleanup and exception handling
    def stop(self):
        """Stop worker and all threads with safe queue cleanup."""
        """Stop worker and all threads with enhanced resource cleanup."""
        logger.info(f"Stopping camera worker {self.camera_id}")

        with self.lock:
            self.running = False
            
        # Set stop recording event to break any active recording loops
        try:
            self.stop_recording_event.set()
        except Exception as e:
            logger.warning(f"Error setting stop recording event for {self.camera_id}: {e}")
            
        # Stop recording if active
        if self.recorder:
            try:
                self.recorder.stop_recording()
            except Exception as e:
                logger.error(f"Error stopping recorder for {self.camera_id}: {e}")
                
        # Wait for threads to finish with individual timeout handling
        thread_info = [
            ("heartbeat", self.heartbeat_thread),
            ("command", self.command_thread), 
            ("sub_stream", self.sub_thread),
            ("main_stream", self.main_thread)
        ]
        
        for thread_name, thread in thread_info:
            if thread and thread.is_alive():
                try:
                    logger.debug(f"Waiting for {thread_name} thread to stop for {self.camera_id}")
                    thread.join(timeout=2.0)
                    
                    if thread.is_alive():
                        logger.warning(f"{thread_name} thread did not stop gracefully for {self.camera_id}")
                    else:
                        logger.debug(f"{thread_name} thread stopped for {self.camera_id}")
                        
                except Exception as e:
                    logger.error(f"Error stopping {thread_name} thread for {self.camera_id}: {e}")
        
        # Safe queue cleanup - drain remaining messages to prevent memory leaks
        try:
            # Drain command queue
            drained_commands = 0
            while True:
                try:
                    self.cmd_queue.get_nowait()
                    drained_commands += 1
                    if drained_commands > 100:  # Safety limit
                        break
                except queue.Empty:
                    break
                except Exception as e:
                    logger.warning(f"Error draining command queue for {self.camera_id}: {e}")
                    break
                    
            if drained_commands > 0:
                logger.debug(f"Drained {drained_commands} commands from queue for {self.camera_id}")
                
        except Exception as e:
            logger.warning(f"Error during queue cleanup for {self.camera_id}: {e}")
        
        # Clear all events after threads stop
        try:
            self.recording_event.clear()
            self.stop_recording_event.clear()
        except Exception as e:
            logger.warning(f"Error clearing events for {self.camera_id}: {e}")
            
        # Additional cleanup - force release any remaining OpenCV resources
        # This is a safety net in case the SUB thread didn't clean up properly
        try:
            # Force garbage collection to help release any unreferenced objects
            import gc
            gc.collect()
            logger.debug(f"Forced garbage collection for {self.camera_id}")
        except Exception as e:
            logger.warning(f"Error during garbage collection for {self.camera_id}: {e}")
            
        logger.info(f"Camera worker {self.camera_id} stopped with enhanced cleanup")

    def _command_loop(self):
        """Process commands from supervisor with robust queue exception handling."""
        logger.info(f"Starting command loop for {self.camera_id}")
        
        command_failures = 0
        max_command_failures = 10
        
        while self.running:
            try:
                # Enhanced command queue handling
                try:
                    cmd_data = self.cmd_queue.get(timeout=1.0)
                    command_failures = 0  # Reset failure count on successful get
                    
                    # Validate command data
                    if not isinstance(cmd_data, dict):
                        logger.warning(f"Invalid command data type for {self.camera_id}: {type(cmd_data)}")
                        continue
                    
                    self._handle_command(cmd_data)
                    
                except queue.Empty:
                    # Normal timeout - continue loop
                    continue
                    
                except (BrokenPipeError, ConnectionError, OSError) as e:
                    command_failures += 1
                    logger.error(f"Command queue connection error for worker {self.camera_id}: {e} (failure #{command_failures})")
                    
                    if command_failures >= max_command_failures:
                        logger.critical(f"Command queue persistently unavailable for {self.camera_id}, supervisor may be dead")
                        # Continue trying but with longer delays
                        time.sleep(10.0)
                    else:
                        time.sleep(2.0)  # Brief delay before retry
                        
                except Exception as e:
                    command_failures += 1
                    logger.error(f"Error getting command for worker {self.camera_id}: {e} (failure #{command_failures})")
                    
                    if command_failures >= max_command_failures:
                        logger.error(f"Too many command queue failures for {self.camera_id}")
                        time.sleep(10.0)
                    else:
                        time.sleep(1.0)
                        
            except Exception as e:
                logger.error(f"Critical error in command loop for {self.camera_id}: {e}")
                time.sleep(5.0)  # Back off on critical errors
                
        logger.info(f"Command loop stopped for {self.camera_id}")

    # 2. UPDATED _sub_stream_loop METHOD - Enhanced with frame validation and health monitoring
    # 4. UPDATED _sub_stream_loop METHOD - Enhanced error reporting for recording awareness
    def _sub_stream_loop(self):
        # """SUB stream thread - enhanced with frame validation and silent failure detection."""
        # """SUB stream thread - enhanced with recording-aware error reporting."""
        """SUB stream thread - enhanced with comprehensive resource management."""
        logger.info(f"Starting SUB stream loop for {self.camera_id} (preview and motion detection)")
        cap = None
        prev_frame = None
        frame_count = 0
        last_fps_time = time.time()

        # Connection tracking
        connection_failures = 0
        max_connection_failures = 5
        last_connection_attempt = 0
        reconnection_delay = 5.0
        max_reconnection_delay = 60.0

        # Frame health tracking
        self.frame_validation_failures = 0
        self.total_frames_processed = 0
        consecutive_frame_failures = 0
        max_consecutive_frame_failures = 10
        last_health_check = time.time()
        health_check_interval = 30.0  # Check stream health every 30 seconds

        # Frame quality tracking
        last_valid_frame_time = time.time()
        max_frame_gap = 5.0  # Maximum seconds without valid frame
        
        try:
            while self.running:
                try:
                    # Periodic stream health assessment
                    current_time = time.time()
                    if current_time - last_health_check >= health_check_interval:
                        is_healthy, health_message = self._assess_stream_health()
                        if not is_healthy:
                            # Enhanced logging for recording awareness
                            recording_status = "during recording" if self._is_recording() else "while idle"
                            logger.warning(f"Stream health issue for {self.camera_id} {recording_status}: {health_message}")
                            
                            # Force reconnection on persistent health issues - ensure cleanup
                            if cap:
                                logger.info(f"Forcing reconnection due to health issues for {self.camera_id}")
                                self._safe_release_capture(cap)
                                cap = None
                        else:
                            logger.debug(f"Stream health OK for {self.camera_id}: {health_message}")
                        last_health_check = current_time
                    
                    # Connection establishment with comprehensive resource management
                    if cap is None or not cap.isOpened():
                        current_time = time.time()
                        
                        # Rate limit reconnection attempts
                        if current_time - last_connection_attempt < reconnection_delay:
                            time.sleep(0.1)
                            continue
                            
                        last_connection_attempt = current_time
                        
                        # Ensure any existing capture is properly released
                        if cap is not None:
                            self._safe_release_capture(cap)
                            cap = None
                        
                        # Get camera URL with fallback
                        logger.info(f"Attempting to connect SUB stream for {self.camera_id}")
                        url = CameraHelper.get_camera_sub_url(self.camera_id, timeout=2.0)
                        if not url:
                            logger.warning(f"SUB stream not available, fallback to MAIN stream for {self.camera_id}")
                            url = CameraHelper.get_camera_main_url(self.camera_id, timeout=2.0)
    
                        if not url:
                            connection_failures += 1
                            # Enhanced error reporting with recording awareness
                            recording_context = " (RECORDING ACTIVE - may stop soon)" if self._is_recording() else ""
                            logger.critical(f"Both MAIN and SUB streams unreachable for Camera {self.camera_id} (failures: {connection_failures}/{max_connection_failures}){recording_context}")
                            
                            # Report error to supervisor via heartbeat
                            with self.lock:
                                self.error_message = f"Both MAIN and SUB streams unreachable (failures: {connection_failures})"
                            
                            if connection_failures >= max_connection_failures:
                                logger.critical(f"Max connection failures reached for {self.camera_id}, stopping SUB stream")
                                with self.lock:
                                    self.error_message = "Max connection failures reached"
                                break
                            
                            # Exponential backoff
                            reconnection_delay = min(reconnection_delay * 1.5, max_reconnection_delay)
                            time.sleep(reconnection_delay)
                            continue
                        
                        # Connection attempt with guaranteed resource cleanup on failure
                        temp_cap = None
                        try:
                            logger.debug(f"Creating VideoCapture for {url}")
                            temp_cap = cv2.VideoCapture(url)
                            
                            if not temp_cap.isOpened():
                                raise ValueError(f"Cannot open camera stream: {url}")
                            
                            # Test read a frame and validate it
                            ret, test_frame = temp_cap.read()
                            if not ret:
                                raise ValueError(f"Cannot read from camera stream: {url}")
                            
                            # Validate test frame
                            is_valid, validation_error = self._validate_frame(test_frame)
                            if not is_valid:
                                raise ValueError(f"Invalid test frame: {validation_error}")
                            
                            # Connection successful - assign to main variable
                            cap = temp_cap
                            temp_cap = None  # Prevent cleanup of successful connection
                            
                            logger.info(f"SUB stream connected to {url} for {self.camera_id}")
                            connection_failures = 0
                            reconnection_delay = 5.0  # Reset delay
                            
                            # Reset health tracking
                            self.frame_validation_failures = 0
                            self.total_frames_processed = 0
                            consecutive_frame_failures = 0
                            last_valid_frame_time = time.time()
                            
                            # Clear error state
                            with self.lock:
                                self.error_message = None
                            
                            # Reset frame tracking
                            prev_frame = None
                            frame_count = 0
                            last_fps_time = time.time()
                            
                        except Exception as e:
                            connection_failures += 1
                            logger.warning(f"Failed to connect to {url} for {self.camera_id}: {e} (failures: {connection_failures}/{max_connection_failures})")
                            
                            # Ensure temporary capture is released on connection failure
                            if temp_cap is not None:
                                self._safe_release_capture(temp_cap)
                                temp_cap = None
                            
                            # Report connection error
                            with self.lock:
                                self.error_message = f"Connection failed: {str(e)[:50]}..."
                            
                            # Exponential backoff
                            reconnection_delay = min(reconnection_delay * 1.5, max_reconnection_delay)
                            time.sleep(reconnection_delay)
                            continue
                    
                    # Frame reading and processing with error handling
                    try:
                        ret, frame = cap.read()
                        self.total_frames_processed += 1
                        
                        if not ret:
                            logger.warning(f"Failed to read frame from SUB stream {self.camera_id}")
                            consecutive_frame_failures += 1
                            
                            # Force reconnection after consecutive failures
                            if consecutive_frame_failures >= max_consecutive_frame_failures:
                                logger.error(f"Too many consecutive frame read failures for {self.camera_id}, forcing reconnection")
                                self._safe_release_capture(cap)
                                cap = None
                                consecutive_frame_failures = 0
                            
                            with self.lock:
                                self.error_message = "Frame read failed"
                                
                            time.sleep(1.0)  # Brief pause before reconnection
                            continue
                        
                        # Frame validation with error handling
                        try:
                            is_valid_frame, validation_error = self._validate_frame(frame)
                            if not is_valid_frame:
                                self.frame_validation_failures += 1
                                consecutive_frame_failures += 1
                                
                                logger.warning(f"Invalid frame from SUB stream {self.camera_id}: {validation_error}")
                                
                                # Force reconnection on too many validation failures
                                if consecutive_frame_failures >= max_consecutive_frame_failures:
                                    logger.error(f"Too many consecutive frame validation failures for {self.camera_id}, forcing reconnection")
                                    self._safe_release_capture(cap)
                                    cap = None
                                    consecutive_frame_failures = 0
                                
                                with self.lock:
                                    self.error_message = f"Frame validation failed: {validation_error}"
                                
                                time.sleep(0.5)  # Brief pause before next frame
                                continue
                            
                            # Frame is valid - reset failure counters
                            consecutive_frame_failures = 0
                            last_valid_frame_time = current_time
                            
                        except Exception as e:
                            logger.error(f"Error during frame validation for {self.camera_id}: {e}")
                            self._safe_release_capture(cap)
                            cap = None
                            continue
                    
                    except Exception as e:
                        logger.error(f"Error reading frame from {self.camera_id}: {e}")
                        self._safe_release_capture(cap)
                        cap = None
                        continue
                    
                    # Check for frame gap (no valid frames for too long)
                    if current_time - last_valid_frame_time > max_frame_gap:
                        logger.warning(f"No valid frames for {current_time - last_valid_frame_time:.1f}s for {self.camera_id}, forcing reconnection")
                        self._safe_release_capture(cap)
                        cap = None
                        continue
                    
                    # Frame processing with exception handling
                    try:
                        frame_count += 1
                        current_time = time.time()
                        
                        # Calculate FPS
                        if current_time - last_fps_time >= 2.0:
                            with self.lock:
                                self.last_fps = frame_count / (current_time - last_fps_time)
                            frame_count = 0
                            last_fps_time = current_time
                        
                        # Motion detection with atomic trigger logic
                        gray = preprocess_frame(frame)
                        if prev_frame is not None:
                            motion = detect_motion(
                                prev_frame, 
                                gray, 
                                self.camera_config.get('threshold', 25),
                                self.camera_config.get('area', 500)
                            )
                            
                            # Atomic motion trigger logic with proper synchronization
                            with self.lock:
                                self.motion_detected = motion
                                currently_recording = self._recording
                                current_time_for_motion = current_time
                                
                                # Handle motion recording logic
                                if motion:
                                    # Start motion timer if not started
                                    if self.motion_start_time is None:
                                        self.motion_start_time = current_time_for_motion
                                        self.motion_trigger_sent = False  # Reset trigger flag
                                        logger.debug(f"Motion started for {self.camera_id}, waiting {self.motion_timeout}s for confirmation")
                                        
                                    # Check if motion has been confirmed for required duration
                                    elif (current_time_for_motion - self.motion_start_time >= self.motion_timeout and 
                                          not self.motion_trigger_sent and 
                                          not currently_recording):
                                        
                                        # Additional cooldown check to prevent rapid triggers
                                        if current_time_for_motion - self.last_motion_trigger_time >= self.motion_trigger_cooldown:
                                            logger.info(f"Motion confirmed for {self.camera_id}, triggering recording")
                                            self.recording_event.set()
                                            self.motion_trigger_sent = True # Mark trigger as sent
                                            self.last_motion_trigger_time = current_time_for_motion
                                        else:
                                            logger.debug(f"Motion trigger cooldown active for {self.camera_id}")
                                else:
                                    # No motion detected - reset motion state
                                    if self.motion_start_time is not None:
                                        logger.debug(f"Motion ended for {self.camera_id}, resetting trigger state")
                                    self.motion_start_time = None
                                    self.motion_trigger_sent = False
                            
                        prev_frame = gray
                        
                        # Clear error state for healthy frame processing
                        if self.error_message and "Frame" in str(self.error_message):
                            with self.lock:
                                self.error_message = None
                        
                    except Exception as e:
                        logger.error(f"Error processing frame for {self.camera_id}: {e}")
                        # Don't release capture for processing errors, just continue
                        continue
                    
                    # Small delay to prevent CPU overload
                    time.sleep(0.033)  # ~30 FPS max
                    
                except Exception as e:
                    logger.error(f"Unexpected error in SUB stream inner loop for {self.camera_id}: {e}")
                    # Release capture on unexpected errors and force reconnection
                    self._safe_release_capture(cap)
                    cap = None
                    
                    with self.lock:
                        self.error_message = f"Unexpected error: {str(e)[:50]}..."
                        
                    time.sleep(5.0)  # Back off on unexpected errors
        
        except Exception as e:
            logger.error(f"Critical error in SUB stream main loop for {self.camera_id}: {e}")
            
        finally:
            # Guaranteed cleanup - ensure capture is always released
            logger.debug(f"SUB stream loop cleanup for {self.camera_id}")
            self._safe_release_capture(cap)
            cap = None
            
        logger.info(f"SUB stream loop stopped for {self.camera_id}")

 
    # camera_worker.py - OpenCV VideoCapture Resource Management Fix
    def _safe_release_capture(self, cap):
        """
        Safely release OpenCV VideoCapture with error handling.
        
        Args:
            cap: OpenCV VideoCapture object to release
        """
        if cap is not None:
            try:
                if cap.isOpened():
                    cap.release()
                logger.debug(f"VideoCapture released for {self.camera_id}")
            except Exception as e:
                logger.warning(f"Error releasing VideoCapture for {self.camera_id}: {e}")


    # 3. UPDATED _heartbeat_loop METHOD - Enhanced with frame health reporting
    # 1. UPDATED _heartbeat_loop METHOD - Comprehensive queue exception handling
    def _heartbeat_loop(self):
        """Send periodic heartbeat with comprehensive queue exception handling."""
        logger.info(f"Starting heartbeat loop for {self.camera_id}")
        
        heartbeat_failures = 0
        max_heartbeat_failures = 5
        
        while self.running:
            try:
                # Thread-safe state collection
                with self.lock:
                    current_recording = self._recording
                    current_fps = self.last_fps
                    current_error = self.error_message
                    
                # Get recorder status safely
                recorder_status = None
                if self.recorder:
                    try:
                        recorder_status = self.recorder.get_recording_status()
                    except Exception as e:
                        logger.warning(f"Error getting recorder status: {e}")
                
                # Get frame health statistics
                frame_health_info = ""
                if hasattr(self, 'total_frames_processed') and self.total_frames_processed > 0:
                    failure_rate = self.frame_validation_failures / self.total_frames_processed
                    if failure_rate > 0.05:  # More than 5% failures worth reporting
                        frame_health_info = f", frame_failures: {failure_rate:.1%}"
                
                # Determine stream state
                if current_error:
                    stream_state = "error"
                elif current_recording:
                    if recorder_status and recorder_status.get('failed'):
                        stream_state = "recording_failed"
                    else:
                        stream_state = "recording"
                elif current_fps > 0:
                    stream_state = "capturing" 
                else:
                    stream_state = "idle"
                
                # Create heartbeat message
                try:
                    heartbeat = HeartbeatMessage(
                        worker_id=self.camera_id,
                        timestamp=datetime.now().isoformat(),
                        stream_state=stream_state,
                        fps=current_fps,
                        recording=current_recording,
                        error_message=current_error
                    )
                    heartbeat_dict = asdict(heartbeat)
                except Exception as e:
                    logger.error(f"Error creating heartbeat message for {self.camera_id}: {e}")
                    continue
                
                # Send heartbeat with comprehensive exception handling
                try:
                    self.status_queue.put_nowait(heartbeat_dict)
                    heartbeat_failures = 0  # Reset failure count on success
                    
                    # Enhanced debug logging with frame health
                    if recorder_status:
                        logger.debug(f"Heartbeat {self.camera_id}: {stream_state} (fps: {current_fps:.1f}, "
                                f"duration: {self._get_recording_duration():.1f}s, "
                                f"ffmpeg_restarts: {recorder_status.get('restart_count', 0)}{frame_health_info})")
                    else:
                        logger.debug(f"Heartbeat {self.camera_id}: {stream_state} (fps: {current_fps:.1f}{frame_health_info})")
                        
                except queue.Full:
                    heartbeat_failures += 1
                    logger.warning(f"Status queue full for worker {self.camera_id} (failure #{heartbeat_failures})")
                    
                    if heartbeat_failures >= max_heartbeat_failures:
                        logger.error(f"Too many heartbeat failures for {self.camera_id}, supervisor may be unresponsive")
                        # Don't break - keep trying, but log the issue
                        
                except (BrokenPipeError, ConnectionError, OSError) as e:
                    logger.error(f"Queue connection error for worker {self.camera_id}: {e}")
                    # These indicate supervisor process issues - continue trying
                    heartbeat_failures += 1
                    
                except Exception as e:
                    heartbeat_failures += 1
                    logger.error(f"Unexpected error sending heartbeat from worker {self.camera_id}: {e}")
                    
                    if heartbeat_failures >= max_heartbeat_failures:
                        logger.critical(f"Critical heartbeat failure for {self.camera_id}, worker may be isolated")
                
                # Clear transient error messages after successful heartbeat
                if heartbeat_failures == 0 and current_error and not current_error.startswith("Max connection failures"):
                    with self.lock:
                        if self.error_message == current_error:  # Only clear if unchanged
                            self.error_message = None
                    
            except Exception as e:
                logger.error(f"Critical error in heartbeat loop for {self.camera_id}: {e}")
                time.sleep(10.0)  # Back off significantly on critical errors
                continue
                
            time.sleep(5.0)  # Heartbeat every 5 seconds

    # camera_worker.py - Event Coordination Fix - Updated Methods
    # 1. UPDATED _main_stream_loop METHOD - Enhanced event coordination
    # 3. UPDATED _main_stream_loop METHOD - Enhanced with stream health monitoring
    def _main_stream_loop(self):
        """MAIN stream thread - handles recording with stream health monitoring."""
        logger.info(f"Starting MAIN stream loop for {self.camera_id}")
        
        try:
            while self.running:
                # Wait for recording trigger with proper event handling
                if self.recording_event.wait(timeout=1.0):
                    # Clear recording event immediately to prevent duplicate processing
                    self.recording_event.clear()
                    
                    # Verify we should start recording (double-check state)
                    if self._is_recording():
                        logger.debug(f"Recording event received but already recording for {self.camera_id}")
                        continue
                    
                    logger.debug(f"Recording event triggered for {self.camera_id}")
                    
                    # Start recording (state management handled internally)
                    self._start_recording()
                    
                    # Only enter recording loop if recording actually started
                    if self._is_recording():
                        logger.debug(f"Entering recording loop for {self.camera_id}")
                        
                        # Clear stop event before starting recording loop
                        self.stop_recording_event.clear()
                        
                        # Reset stream health tracking for new recording session
                        self.stream_health_failures = 0
                        self.last_valid_stream_time = time.time()
                        
                        # Record until stop conditions met
                        recording_start_time = time.time()
                        last_health_check = time.time()
                        last_stream_health_check = time.time()
                        
                        while (self.running and 
                            self._is_recording()):
                            
                            # Check for external stop signal (non-blocking)
                            if self.stop_recording_event.is_set():
                                logger.info(f"Stop recording event received for {self.camera_id}")
                                break
                            
                            # Periodic health checks (every 2 seconds)
                            current_time = time.time()
                            if current_time - last_health_check >= 2.0:
                                # Check FFmpeg health
                                if self.recorder and not self.recorder.is_recording_healthy():
                                    logger.warning(f"FFmpeg recording failed for {self.camera_id}, stopping")
                                    break
                                
                                last_health_check = current_time
                            
                            # Stream health monitoring (every 5 seconds during recording)
                            if current_time - last_stream_health_check >= 5.0:
                                is_stream_healthy, health_reason = self._is_stream_healthy_for_recording()
                                
                                if not is_stream_healthy:
                                    logger.warning(f"Stream unhealthy during recording for {self.camera_id}: {health_reason}")
                                    self.stream_health_failures += 1
                                    
                                    # Stop recording if too many consecutive stream failures
                                    if self.stream_health_failures >= self.max_stream_failures_during_recording:
                                        logger.error(f"Stopping recording for {self.camera_id} due to persistent stream issues: {health_reason}")
                                        break
                                else:
                                    # Stream is healthy - reset failure count
                                    if self.stream_health_failures > 0:
                                        logger.info(f"Stream health recovered for {self.camera_id} during recording")
                                    self.stream_health_failures = 0
                                
                                last_stream_health_check = current_time
                            
                            # Auto-stop logic with thread-safe access
                            recording_duration = self._get_recording_duration()
                            with self.lock:
                                motion_detected = self.motion_detected
                                
                            if not motion_detected and recording_duration > self.post_record_time:
                                logger.info(f"Auto-stopping recording for {self.camera_id} (no motion, duration: {recording_duration:.1f}s)")
                                break
                            
                            # Short wait to prevent busy loop
                            time.sleep(0.5)
                            
                        logger.debug(f"Exiting recording loop for {self.camera_id}")
                        
                        # Stop recording (state management handled internally)
                        self._stop_recording()
                        
                        # Clear both events after recording ends
                        self.stop_recording_event.clear()
                        
                    else:
                        logger.warning(f"Failed to start recording for {self.camera_id} despite event trigger")
                        
        except Exception as e:
            logger.error(f"Error in MAIN stream loop for {self.camera_id}: {e}")
            with self.lock:
                self.error_message = f"MAIN stream error: {e}"
                self._set_recording_state(False)  # Ensure state is cleared
                
            # Clear events on error
            self.recording_event.clear()
            self.stop_recording_event.clear()

    # 2. UPDATED _handle_command METHOD - Improved external command handling
    # 4. UPDATED _handle_command METHOD - Enhanced exception handling for command processing
    def _handle_command(self, cmd_data: Dict[str, Any]):
        """Handle commands from supervisor with comprehensive exception handling."""
        try:
            # Validate command structure
            if not cmd_data:
                logger.warning(f"Empty command received for worker {self.camera_id}")
                return
                
            command = cmd_data.get('command')
            if not command:
                logger.warning(f"Command missing 'command' field for worker {self.camera_id}: {cmd_data}")
                return
                
            logger.info(f"Worker {self.camera_id} processing command: {command}")
            
            # Process commands with individual exception handling
            try:
                if command == "stop":
                    logger.info(f"Stop command received for worker {self.camera_id}")
                    self.stop()
                    
                elif command == "start_recording":
                    logger.info(f"Manual start recording command for {self.camera_id}")
                    if not self._is_recording():
                        try:
                            self.recording_event.set()
                            logger.debug(f"Recording event set for {self.camera_id}")
                        except Exception as e:
                            logger.error(f"Error setting recording event for {self.camera_id}: {e}")
                    else:
                        logger.info(f"Recording already active for {self.camera_id}")
                        
                elif command == "stop_recording":
                    logger.info(f"Manual stop recording command for {self.camera_id}")
                    if self._is_recording():
                        try:
                            self.stop_recording_event.set()
                            logger.debug(f"Stop recording event set for {self.camera_id}")
                        except Exception as e:
                            logger.error(f"Error setting stop recording event for {self.camera_id}: {e}")
                    else:
                        logger.info(f"Recording not active for {self.camera_id}")
                        
                elif command == "ptz_move":
                    # PTZ commands with parameter validation
                    params = cmd_data.get('params', {})
                    if not isinstance(params, dict):
                        logger.warning(f"Invalid PTZ params for {self.camera_id}: {params}")
                        return
                        
                    logger.info(f"PTZ command received for {self.camera_id}: {params}")
                    # PTZ implementation would go here
                    
                else:
                    logger.warning(f"Unknown command for {self.camera_id}: {command}")
                    
            except Exception as e:
                logger.error(f"Error executing command '{command}' for {self.camera_id}: {e}")
                
        except Exception as e:
            logger.error(f"Error handling command for {self.camera_id}: {e}, cmd_data: {cmd_data}")
            
            # Try to report error back to supervisor if possible
            try:
                with self.lock:
                    self.error_message = f"Command error: {str(e)[:50]}..."
            except:
                pass  # If even this fails, just continue


    # 3. UPDATED _start_recording METHOD - Enhanced event state management
    def _start_recording(self):
        """Start FFmpeg recording - thread-safe with enhanced event coordination."""
        try:
            # Thread-safe state change
            if not self._set_recording_state(True):
                logger.debug(f"Recording already active for {self.camera_id}")
                return
                
            # Reset motion trigger state when recording starts
            with self.lock:
                self.motion_trigger_sent = False  # Allow new triggers after recording ends
                
            logger.debug(f"Starting FFmpeg recording for {self.camera_id}")
            
            # Get main stream URL
            main_url = self.camera_config.get('url')
            if not main_url:
                raise ValueError("No main stream URL configured")
                
            # Create output filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"output/{self.camera_id}_{timestamp}.mp4"
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            
            # Initialize recorder
            self.recorder = FFmpegRecorder(
                url=main_url,
                output_file=output_file,
                pre_record_time=self.camera_config.get('pre_record_time', 5),
                fps=self.camera_config.get('fps', 15),
                frame_size=(
                    self.camera_config.get('width', 1920),
                    self.camera_config.get('height', 1080)
                )
            )
            
            self.recorder.start_recording()
            logger.info(f"Started recording for {self.camera_id}: {output_file}")
            
            # Clear any pending recording events after successful start
            try:
                self.recording_event.clear()
            except:
                pass
            
        except Exception as e:
            logger.error(f"Failed to start recording for {self.camera_id}: {e}")
            with self.lock:
                self.error_message = f"Recording start error: {e}"
                self._set_recording_state(False)  # Clear state on failure
                self.motion_trigger_sent = False  # Reset trigger state on failure
                
            # Clear recording event on failure
            try:
                self.recording_event.clear()
            except:
                pass

    # 4. UPDATED _stop_recording METHOD - Enhanced event state management  
    def _stop_recording(self):
        """Stop FFmpeg recording - thread-safe with enhanced event coordination."""
        try:
            # Thread-safe state change  
            if not self._set_recording_state(False):
                logger.debug(f"Recording already stopped for {self.camera_id}")
                return
                
            logger.debug(f"Stopping FFmpeg recording for {self.camera_id}")
            
            # Reset motion trigger state when recording stops
            with self.lock:
                self.motion_trigger_sent = False  # Allow new motion triggers
                self.motion_start_time = None  # Reset motion timer
                
            if self.recorder:
                self.recorder.stop_recording()
                self.recorder = None
                
            logger.info(f"Stopped recording for {self.camera_id}")
            
            # Clear stop recording event after successful stop
            try:
                self.stop_recording_event.clear()
            except:
                pass
            
        except Exception as e:
            logger.error(f"Failed to stop recording for {self.camera_id}: {e}")
            with self.lock:
                self.error_message = f"Recording stop error: {e}"
                self.motion_trigger_sent = False  # Reset trigger state on error
            # State already set to False by _set_recording_state above
            
            # Clear events on error
            try:
                self.stop_recording_event.clear()
            except:
                pass

    # 5. UPDATED stop METHOD - Clean event cleanup on worker shutdown
    def stop(self):
        """Stop worker and all threads with proper event cleanup."""
        logger.info(f"Stopping camera worker {self.camera_id}")
        
        with self.lock:
            self.running = False
            
        # Set stop recording event to break any active recording loops
        try:
            self.stop_recording_event.set()
        except:
            pass
            
        # Stop recording if active
        if self.recorder:
            try:
                self.recorder.stop_recording()
            except Exception as e:
                logger.error(f"Error stopping recorder: {e}")
                
        # Wait for threads to finish
        for thread in [self.heartbeat_thread, self.command_thread, self.sub_thread, self.main_thread]:
            if thread and thread.is_alive():
                thread.join(timeout=2.0)
                
        # Clear all events after threads stop
        try:
            self.recording_event.clear()
            self.stop_recording_event.clear()
        except:
            pass
            
        logger.info(f"Camera worker {self.camera_id} stopped")

    # 1. NEW HELPER METHODS - Frame validation and stream health monitoring

    def _validate_frame(self, frame) -> tuple[bool, str]:
        """
        Validate frame quality and detect corruption.
        
        Returns:
            (is_valid, error_message)
        """
        try:
            if frame is None:
                return False, "Frame is None"
            
            if not isinstance(frame, np.ndarray):
                return False, "Frame is not numpy array"
            
            if frame.size == 0:
                return False, "Frame is empty"
            
            # Check frame dimensions
            if len(frame.shape) < 2:
                return False, "Invalid frame dimensions"
            
            height, width = frame.shape[:2]
            
            # Reasonable dimension checks
            if height < 100 or width < 100:
                return False, f"Frame too small: {width}x{height}"
            
            if height > 4096 or width > 4096:
                return False, f"Frame too large: {width}x{height}"
            
            # Check for all-black or all-white frames (common corruption)
            if len(frame.shape) == 3:  # Color frame
                frame_gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
            else:
                frame_gray = frame
            
            mean_brightness = np.mean(frame_gray)
            std_brightness = np.std(frame_gray)
            
            # Detect completely black frames
            if mean_brightness < 5 and std_brightness < 5:
                return False, "Frame appears to be completely black"
            
            # Detect completely white/overexposed frames
            if mean_brightness > 250 and std_brightness < 5:
                return False, "Frame appears to be completely white/overexposed"
            
            # Check for reasonable data variance (not frozen frame)
            if std_brightness < 0.1:
                return False, "Frame appears frozen (no variance)"
            
            return True, ""
            
        except Exception as e:
            return False, f"Frame validation error: {str(e)[:50]}"

    def _assess_stream_health(self) -> tuple[bool, str]:
        """
        Assess overall stream health based on recent metrics.
        
        Returns:
            (is_healthy, status_message)
        """
        try:
            with self.lock:
                current_fps = self.last_fps
                
            # FPS health check
            expected_fps = self.camera_config.get('fps', 15)
            min_acceptable_fps = expected_fps * 0.3  # 30% of expected FPS
            
            if current_fps < min_acceptable_fps:
                return False, f"Low FPS: {current_fps:.1f} (expected: {expected_fps})"
            
            # Check frame validation failure rate
            if hasattr(self, 'frame_validation_failures'):
                failure_rate = self.frame_validation_failures / max(self.total_frames_processed, 1)
                if failure_rate > 0.1:  # More than 10% failures
                    return False, f"High frame validation failure rate: {failure_rate:.1%}"
            
            return True, "Stream healthy"
            
        except Exception as e:
            return False, f"Health assessment error: {str(e)[:50]}"

    # 2. NEW HELPER METHOD - Check if SUB stream is healthy enough to continue recording
    def _is_stream_healthy_for_recording(self) -> tuple[bool, str]:
        """
        Check if SUB stream is healthy enough to continue recording.
        
        Returns:
            (is_healthy, reason_if_unhealthy)
        """
        try:
            current_time = time.time()
            
            # Check if we have recent valid frames (FPS > 0)
            with self.lock:
                current_fps = self.last_fps
                current_error = self.error_message
            
            # Check for stream errors
            if current_error:
                # Critical errors that should stop recording immediately
                critical_errors = [
                    "Max connection failures reached",
                    "Both MAIN and SUB streams unreachable"
                ]
                
                for critical_error in critical_errors:
                    if critical_error in current_error:
                        return False, f"Critical stream error: {current_error}"
            
            # Check FPS health
            expected_fps = self.camera_config.get('fps', 15)
            min_recording_fps = expected_fps * 0.2  # 20% of expected FPS minimum for recording
            
            if current_fps < min_recording_fps:
                stream_downtime = current_time - self.last_valid_stream_time
                
                # Update last valid stream time if FPS is acceptable
                if current_fps >= min_recording_fps:
                    self.last_valid_stream_time = current_time
                    self.stream_health_failures = 0  # Reset failure count
                
                # Check if stream has been down too long
                if stream_downtime > self.max_stream_downtime_during_recording:
                    return False, f"Stream down for {stream_downtime:.1f}s (max: {self.max_stream_downtime_during_recording}s)"
            else:
                # Stream is healthy - reset counters
                self.last_valid_stream_time = current_time
                self.stream_health_failures = 0
            
            # Check consecutive failure count
            if hasattr(self, 'total_frames_processed') and hasattr(self, 'frame_validation_failures'):
                if self.total_frames_processed > 100:  # Only check after some frames processed
                    recent_failure_rate = self.frame_validation_failures / self.total_frames_processed
                    if recent_failure_rate > 0.5:  # More than 50% failures
                        return False, f"High frame failure rate: {recent_failure_rate:.1%}"
            
            return True, "Stream healthy"
            
        except Exception as e:
            return False, f"Health check error: {str(e)[:50]}"
        
# For testing/debugging - can run worker standalone
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python camera_worker.py <camera_id>")
        sys.exit(1)
        
    camera_id = sys.argv[1]
    
    # Create mock queues for standalone testing
    cmd_queue = mp.Queue()
    status_queue = mp.Queue()
    
    worker = CameraWorker(camera_id, cmd_queue, status_queue)
    
    try:
        worker.start()
    except KeyboardInterrupt:
        logger.info(f"Worker {camera_id} interrupted")
    finally:
        worker.stop()