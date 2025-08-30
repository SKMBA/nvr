# camera_worker.py
import threading
import time
import signal
import sys
import cv2
import os
from typing import Optional, Dict, Any
from dataclasses import asdict
from datetime import datetime
import multiprocessing as mp
import queue

from core.logger_config import logger
from core.camera_helper import CameraHelper
from core.ffmpeg_recorder import FFmpegRecorder
from core.motion_detector import detect_motion, preprocess_frame
from supervisor import HeartbeatMessage, CommandMessage


class CameraWorker:
    """Camera worker process that handles SUB stream (preview + motion) and MAIN stream (recording)."""
    
    def __init__(self, camera_id: str, cmd_queue: mp.Queue, status_queue: mp.Queue):
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
        self.recording = False
        self.motion_detected = False
        self.last_fps = 0.0
        self.error_message: Optional[str] = None
        
        # Threads
        self.sub_thread: Optional[threading.Thread] = None
        self.main_thread: Optional[threading.Thread] = None
        self.heartbeat_thread: Optional[threading.Thread] = None
        self.command_thread: Optional[threading.Thread] = None
        
        # Synchronization
        self.lock = threading.Lock()
        self.recording_event = threading.Event()
        self.stop_recording_event = threading.Event()
        
        # Recording
        self.recorder: Optional[FFmpegRecorder] = None
        self.motion_start_time: Optional[float] = None
        self.motion_timeout = self.camera_config.get('motion_timeout', 1.5)
        self.post_record_time = self.camera_config.get('post_record_time', 5.0)
        
        logger.info(f"CameraWorker initialized for {camera_id}")
        
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
            
    def stop(self):
        """Stop worker and all threads."""
        logger.info(f"Stopping camera worker {self.camera_id}")
        
        with self.lock:
            self.running = False
            self.stop_recording_event.set()
            
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
                
        logger.info(f"Camera worker {self.camera_id} stopped")
        
    def _heartbeat_loop(self):
        """Send periodic heartbeat messages to supervisor."""
        logger.info(f"Starting heartbeat loop for {self.camera_id}")
        
        while self.running:
            try:
                with self.lock:
                    stream_state = "error" if self.error_message else ("recording" if self.recording else "capturing")
                    
                heartbeat = HeartbeatMessage(
                    worker_id=self.camera_id,
                    timestamp=datetime.now().isoformat(),
                    stream_state=stream_state,
                    fps=self.last_fps,
                    recording=self.recording,
                    error_message=self.error_message
                )
                
                self.status_queue.put_nowait(asdict(heartbeat))
                
                # Clear error message after sending
                if self.error_message:
                    self.error_message = None
                    
            except Exception as e:
                logger.error(f"Error sending heartbeat from worker {self.camera_id}: {e}")
                
            time.sleep(5.0)  # Heartbeat every 5 seconds
            
    def _command_loop(self):
        """Process commands from supervisor."""
        logger.info(f"Starting command loop for {self.camera_id}")
        
        while self.running:
            try:
                # Non-blocking check for commands
                try:
                    cmd_data = self.cmd_queue.get(timeout=1.0)
                    self._handle_command(cmd_data)
                except queue.Empty:
                    continue
                    
            except Exception as e:
                logger.error(f"Error processing command in worker {self.camera_id}: {e}")
                
    def _handle_command(self, cmd_data: Dict[str, Any]):
        """Handle a command from the supervisor."""
        try:
            command = cmd_data.get('command')
            logger.info(f"Worker {self.camera_id} received command: {command}")
            
            if command == "stop":
                self.stop()
            elif command == "start_recording":
                self._start_recording()
            elif command == "stop_recording":
                self._stop_recording()
            elif command == "ptz_move":
                # PTZ commands would be handled here
                logger.info(f"PTZ command received: {cmd_data.get('params', {})}")
            else:
                logger.warning(f"Unknown command: {command}")
                
        except Exception as e:
            logger.error(f"Error handling command {cmd_data}: {e}")
            
    def _sub_stream_loop(self):
        """SUB stream thread - handles preview and motion detection."""
        logger.info(f"Starting SUB stream loop for {self.camera_id} (preview and motion detection)")
        
        cap = None
        prev_frame = None
        frame_count = 0
        last_fps_time = time.time()
        
        try:
            # Get camera URL - prefer sub_url, fallback to main
            # url = CameraHelper.get_camera_url(self.camera_id, timeout=2.0)
            logger.debug(f"Retrieve SUB stream for {self.camera_id}")
            url = CameraHelper.get_camera_sub_url(self.camera_id, timeout=2.0)
            if url == None:
                logger.warning(f"SUB stream not available, fallback to MAIN stream for {self.camera_id}")
                logger.debug(f"Retrieve MAIN stream for {self.camera_id}")
                url = CameraHelper.get_camera_main_url(self.camera_id, timeout=2.0)

            if not url:
                logger.critical (f"Both MAIN and SUB streamS unreachable for Camera {camera_id }")
                raise ValueError(f"No valid camera URL found for {self.camera_id}")
                
            cap = cv2.VideoCapture(url)
            if not cap.isOpened():
                raise ValueError(f"Cannot open camera stream: {url}")
                
            logger.info(f"SUB stream connected to {url}")
            
            while self.running:
                ret, frame = cap.read()
                if not ret:
                    logger.warning(f"Failed to read frame from SUB stream {self.camera_id}")
                    time.sleep(0.1)
                    continue
                    
                frame_count += 1
                current_time = time.time()
                
                # Calculate FPS
                if current_time - last_fps_time >= 2.0:
                    with self.lock:
                        self.last_fps = frame_count / (current_time - last_fps_time)
                    frame_count = 0
                    last_fps_time = current_time
                    
                # Motion detection
                gray = preprocess_frame(frame)
                if prev_frame is not None:
                    motion = detect_motion(
                        prev_frame, 
                        gray, 
                        self.camera_config.get('threshold', 25),
                        self.camera_config.get('area', 500)
                    )
                    
                    with self.lock:
                        self.motion_detected = motion
                        
                    # Handle motion recording logic
                    if motion:
                        if not self.motion_start_time:
                            self.motion_start_time = current_time
                        elif current_time - self.motion_start_time >= self.motion_timeout:
                            if not self.recording:
                                logger.info(f"Motion confirmed for {self.camera_id}, starting recording")
                                self.recording_event.set()
                    else:
                        self.motion_start_time = None
                        
                prev_frame = gray
                
                # Small delay to prevent CPU overload
                time.sleep(0.033)  # ~30 FPS max
                
        except Exception as e:
            logger.error(f"Error in SUB stream loop for {self.camera_id}: {e}")
            with self.lock:
                self.error_message = f"SUB stream error: {e}"
        finally:
            if cap:
                cap.release()
                
    def _main_stream_loop(self):
        """MAIN stream thread - handles high-quality recording."""
        logger.info(f"Starting MAIN stream loop for {self.camera_id}")
        
        try:
            while self.running:
                # Wait for recording trigger
                if self.recording_event.wait(timeout=1.0):
                    self.recording_event.clear()
                    self._start_recording()
                    
                    # Record until stop signal or timeout
                    start_time = time.time()
                    while (self.running and 
                           self.recording and 
                           not self.stop_recording_event.wait(timeout=1.0)):
                        
                        # Auto-stop recording after post_record_time if no motion
                        if (not self.motion_detected and 
                            time.time() - start_time > self.post_record_time):
                            logger.info(f"Auto-stopping recording for {self.camera_id} (no motion)")
                            break
                            
                    self._stop_recording()
                    self.stop_recording_event.clear()
                    
        except Exception as e:
            logger.error(f"Error in MAIN stream loop for {self.camera_id}: {e}")
            with self.lock:
                self.error_message = f"MAIN stream error: {e}"
                
    def _start_recording(self):
        """Start FFmpeg recording on MAIN stream."""
        try:
            if self.recording:
                return
                
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
            
            with self.lock:
                self.recording = True
                
            logger.info(f"Started recording for {self.camera_id}: {output_file}")
            
        except Exception as e:
            logger.error(f"Failed to start recording for {self.camera_id}: {e}")
            with self.lock:
                self.error_message = f"Recording start error: {e}"
                
    def _stop_recording(self):
        """Stop FFmpeg recording."""
        try:
            if not self.recording:
                return
                
            if self.recorder:
                self.recorder.stop_recording()
                self.recorder = None
                
            with self.lock:
                self.recording = False
                
            logger.info(f"Stopped recording for {self.camera_id}")
            
        except Exception as e:
            logger.error(f"Failed to stop recording for {self.camera_id}: {e}")
            with self.lock:
                self.error_message = f"Recording stop error: {e}"


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