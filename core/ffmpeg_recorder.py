# core/ffmpeg_recorder.py - Complete file with application-level resilience
import subprocess
import threading
import collections
import queue
import cv2
import time
import os
from datetime import datetime
from config.ui_constants import FFMPEG_LOG_FILE
from core.logger_config import logger


class FFmpegRecorder:
    def __init__(self, url, output_file, pre_record_time, fps, frame_size):
        self.url = url
        self.output_file = output_file
        self.pre_record_time = pre_record_time
        self.fps = fps
        self.frame_size = frame_size
        self.frame_queue = collections.deque(maxlen=self.pre_record_time * self.fps)
        self.write_queue = queue.Queue(maxsize=1000)
        self.process = None
        self.recording = False
        self.lock = threading.Lock()
        self.writer_thread = None
        self.stop_writer_event = threading.Event()
        self.dropped_frames = 0
        
        # Process monitoring
        self.monitor_thread = None
        self.stop_monitor_event = threading.Event()
        self.restart_count = 0
        self.max_restarts = 3
        
        logger.info(f"[FFmpegRecorder] Initialized with frame_size={self.frame_size}")

    def add_frame(self, frame):
        resized = cv2.resize(frame, self.frame_size)
        with self.lock:
            self.frame_queue.append(resized)
            if self.recording:
                try:
                    self.write_queue.put_nowait(resized)
                except queue.Full:
                    self.dropped_frames += 1
                    logger.warning(f"[FFmpegRecorder] Write queue full! Dropping frame #{self.dropped_frames}")

    def start_recording(self):
        with self.lock:
            if self.recording:
                return
                
            logger.info(f"[Recorder] Starting recording @{self.fps} fps...")
            
            # Create output directory
            os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
            
            # Start FFmpeg process
            success = self._start_ffmpeg_process()
            if not success:
                logger.error("[Recorder] Failed to start FFmpeg process")
                return
            
            # Start writer thread
            self.stop_writer_event.clear()
            self.writer_thread = threading.Thread(target=self._writer_loop, daemon=True)
            self.writer_thread.start()
            
            # Start process monitoring
            self.stop_monitor_event.clear()
            self.monitor_thread = threading.Thread(target=self._monitor_ffmpeg, daemon=True)
            self.monitor_thread.start()
            
            self.recording = True
            self.restart_count = 0
            
            logger.info(f"[Recorder] Recording started successfully (PID: {self.process.pid})")

    def _start_ffmpeg_process(self):
        """Start the FFmpeg process."""
        try:
            # Simple, reliable FFmpeg command for Windows
            cmd = [
                "ffmpeg",
                "-y",  # Overwrite output
                "-rtsp_transport", "tcp",  # Use TCP for RTSP
                "-i", self.url,
                "-an",  # No audio
                "-c:v", "libx264",
                "-preset", "ultrafast", 
                "-pix_fmt", "yuv420p",
                self.output_file,
            ]
            
            logger.debug(f"[FFmpeg] Starting: {' '.join(cmd)}")
            
            self.process = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=open(FFMPEG_LOG_FILE, "a"),
                stderr=subprocess.STDOUT,
                bufsize=0  # Unbuffered
            )
            
            # Add pre-record frames
            for frame in self.frame_queue:
                try:
                    self.write_queue.put_nowait(frame)
                except queue.Full:
                    logger.warning("[FFmpegRecorder] Write queue full during prebuffer")
            
            return True
            
        except Exception as e:
            logger.error(f"[FFmpeg] Failed to start process: {e}")
            return False

    def _writer_loop(self):
        """Writer thread - sends frames to FFmpeg."""
        logger.info("[Writer] Writer thread started")
        
        while not self.stop_writer_event.is_set():
            try:
                frame = self.write_queue.get(timeout=0.1)
                
                if self.process and self.process.stdin:
                    try:
                        self.process.stdin.write(frame.tobytes())
                        self.process.stdin.flush()
                    except BrokenPipeError:
                        logger.warning("[Writer] Broken pipe - FFmpeg process ended")
                        break
                    except Exception as e:
                        logger.error(f"[Writer] Error writing frame: {e}")
                        break
                        
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"[Writer] Writer loop error: {e}")
                break
                
        logger.info("[Writer] Writer thread stopped")

    def _monitor_ffmpeg(self):
        """Monitor FFmpeg process and restart if needed."""
        logger.info("[Monitor] FFmpeg process monitoring started")
        
        while not self.stop_monitor_event.wait(2.0):  # Check every 2 seconds
            try:
                if not self.process:
                    logger.warning("[Monitor] No FFmpeg process to monitor")
                    break
                
                # Check process status
                exit_code = self.process.poll()
                
                if exit_code is not None:
                    # Process has exited
                    if exit_code == 0:
                        logger.info(f"[Monitor] FFmpeg process exited normally (code: {exit_code})")
                        break  # Normal exit, don't restart
                    else:
                        logger.warning(f"[Monitor] FFmpeg process failed (exit code: {exit_code})")
                        
                        # Attempt restart if under limit
                        if self.restart_count < self.max_restarts:
                            logger.info(f"[Monitor] Attempting restart ({self.restart_count + 1}/{self.max_restarts})")
                            
                            if self._restart_ffmpeg():
                                self.restart_count += 1
                                continue  # Continue monitoring new process
                            else:
                                logger.error("[Monitor] Restart failed")
                                break
                        else:
                            logger.error(f"[Monitor] Max restarts ({self.max_restarts}) reached, giving up")
                            break
                            
            except Exception as e:
                logger.error(f"[Monitor] Monitor error: {e}")
                time.sleep(5.0)  # Back off on errors
                
        # Recording failed/ended
        with self.lock:
            if self.recording:
                logger.warning("[Monitor] Marking recording as stopped due to monitoring exit")
                self.recording = False
                
        logger.info("[Monitor] FFmpeg process monitoring stopped")

    def _restart_ffmpeg(self):
        """Restart the FFmpeg process."""
        try:
            logger.info("[Restart] Restarting FFmpeg process...")
            
            # Kill old process
            if self.process:
                try:
                    self.process.kill()
                    self.process.wait(timeout=3.0)
                except:
                    pass
                    
            # Create new output file to avoid corruption
            base_name = os.path.splitext(self.output_file)[0]
            extension = os.path.splitext(self.output_file)[1] 
            timestamp = datetime.now().strftime("%H%M%S")
            new_output = f"{base_name}_restart_{timestamp}{extension}"
            
            # Update output file
            old_output = self.output_file
            self.output_file = new_output
            
            # Start new process
            if self._start_ffmpeg_process():
                logger.info(f"[Restart] FFmpeg restarted successfully: {new_output}")
                return True
            else:
                logger.error("[Restart] Failed to restart FFmpeg")
                self.output_file = old_output  # Restore original name
                return False
                
        except Exception as e:
            logger.error(f"[Restart] Restart error: {e}")
            return False

    def stop_recording(self):
        """Stop FFmpeg recording with graceful shutdown."""
        logger.info("[Recorder] Stopping recording...")
        
        with self.lock:
            if not self.recording:
                return
            self.recording = False
            
        # Stop monitoring
        if self.monitor_thread:
            self.stop_monitor_event.set()
            self.monitor_thread.join(timeout=3.0)
            self.monitor_thread = None
            
        # Stop writer thread
        if self.writer_thread:
            self.stop_writer_event.set()
            self.writer_thread.join(timeout=2.0)
            self.writer_thread = None
            
        # Graceful FFmpeg shutdown
        try:
            if self.process and self.process.poll() is None:
                logger.info("[Recorder] Sending 'q' command to FFmpeg for graceful shutdown...")
                
                try:
                    # Send quit command
                    self.process.stdin.write(b'q\n')
                    self.process.stdin.flush()
                    
                    # Wait for graceful shutdown
                    exit_code = self.process.wait(timeout=10)
                    logger.info(f"[Recorder] FFmpeg finished gracefully (exit code: {exit_code})")
                    
                except subprocess.TimeoutExpired:
                    logger.warning("[Recorder] Graceful shutdown timeout, terminating...")
                    self.process.terminate()
                    
                    try:
                        exit_code = self.process.wait(timeout=5)
                        logger.info(f"[Recorder] FFmpeg terminated (exit code: {exit_code})")
                    except subprocess.TimeoutExpired:
                        logger.warning("[Recorder] Termination timeout, force killing...")
                        self.process.kill()
                        self.process.wait()
                        
                except (BrokenPipeError, OSError):
                    # FFmpeg already closed
                    logger.info("[Recorder] FFmpeg already closed")
                    self.process.wait(timeout=5)
                    
                except Exception as e:
                    logger.error(f"[Recorder] Error during graceful shutdown: {e}")
                    try:
                        self.process.terminate()
                        self.process.wait(timeout=5)
                    except:
                        self.process.kill()
                        self.process.wait()
                        
                finally:
                    # Close stdin
                    try:
                        if self.process and self.process.stdin:
                            self.process.stdin.close()
                    except:
                        pass
                        
        except Exception as e:
            logger.error(f"[Recorder] Error stopping FFmpeg: {e}")
            
        finally:
            self.process = None
            logger.info(f"[Recorder] Recording stopped. Total dropped frames: {self.dropped_frames}")

    def get_dropped_frames(self):
        return self.dropped_frames

    def reset_dropped_frames(self):
        self.dropped_frames = 0

    def get_dropped_frame_count(self):
        return self.dropped_frames