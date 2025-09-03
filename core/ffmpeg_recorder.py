# ffmpeg_recorder.py - Add process monitoring capabilities

import subprocess
import threading
import collections
import queue
import cv2
import time
import os
from datetime import datetime
from config.ui_constants import FFMPEG_LOG_FILE
from core.logger_config import get_logger

logger = get_logger(__name__)  # __name__ will be the module’s name

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
        self.recording_failed = False  # Flag to indicate recording failure
        
        logger.debug(f"[FFmpegRecorder] Initialized with frame_size={self.frame_size}")

    def add_frame(self, frame):
        resized = cv2.resize(frame, self.frame_size)
        with self.lock:
            self.frame_queue.append(resized)
            if self.recording and not self.recording_failed:
                try:
                    self.write_queue.put_nowait(resized)
                except queue.Full:
                    self.dropped_frames += 1
                    logger.warning(f"[FFmpegRecorder] Write queue full! Dropping frame #{self.dropped_frames}")

    def start_recording(self):
        with self.lock:
            if self.recording:
                return
                
            logger.debug(f"[Recorder] Starting recording @{self.fps} fps...")
            
            # Reset state
            self.recording_failed = False
            self.restart_count = 0
            
            # Create output directory
            os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
            
            # Start FFmpeg process
            if not self._start_ffmpeg_process():
                logger.error("[Recorder] Failed to start initial FFmpeg process")
                return
            
            # Start writer thread
            self.stop_writer_event.clear()
            self.writer_thread = threading.Thread(target=self._writer_loop, daemon=True)
            self.writer_thread.start()
            
            # Start process monitoring
            self.stop_monitor_event.clear()
            self.monitor_thread = threading.Thread(target=self._monitor_ffmpeg_process, daemon=True)
            self.monitor_thread.start()
            
            self.recording = True
            logger.debug(f"[Recorder] Recording started with process monitoring (PID: {self.process.pid})")

    def _start_ffmpeg_process(self):
        """Start FFmpeg process with error handling."""
        try:
            # Simple, reliable command for Windows
            cmd = [
                "ffmpeg",
                "-y",
                # "-rtsp_transport", "tcp",
                "-rtsp_transport", "udp",
                # "-fflags", "low_delay",
                "-probesize", "500000",
                "-analyzeduration", "5000000",
                # "-framedrop",
                "-max_delay", "5000000",
                # "-bufsize", "1M",
                "-i", self.url,
                "-an",
                "-c:v", "libx264", 
                "-preset", "ultrafast",
                "-pix_fmt", "yuv420p",
                self.output_file,
            ]
            
            logger.debug(f"[FFmpeg] Command: {' '.join(cmd)}")
            
            self.process = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=open(FFMPEG_LOG_FILE, "a"),
                stderr=subprocess.STDOUT,
                bufsize=0
            )
            
            # Add pre-record frames to queue
            for frame in self.frame_queue:
                try:
                    self.write_queue.put_nowait(frame)
                except queue.Full:
                    logger.warning("[FFmpegRecorder] Write queue full during prebuffer")
            
            logger.debug(f"[FFmpeg] Process started successfully (PID: {self.process.pid})")
            return True
            
        except Exception as e:
            logger.error(f"[FFmpeg] Failed to start process: {e}")
            return False

    def _monitor_ffmpeg_process(self):
        """Monitor FFmpeg process health and handle failures."""
        logger.debug(f"[FFmpeg Monitor] Starting process monitoring for PID: {self.process.pid}")
        
        while not self.stop_monitor_event.wait(2.0):  # Check every 2 seconds
            try:
                if not self.process:
                    logger.warning("[FFmpeg Monitor] No process to monitor")
                    break
                
                # Check if process is still running
                exit_code = self.process.poll()
                
                if exit_code is not None:
                    # Process has exited
                    if exit_code == 0:
                        logger.info(f"[FFmpeg Monitor] Process completed normally (exit code: {exit_code})")
                        break  # Normal completion
                    else:
                        # Process failed
                        logger.warning(f"[FFmpeg Monitor] Process failed (exit code: {exit_code}, restart: {self.restart_count}/{self.max_restarts})")
                        
                        # Attempt restart if under limit
                        if self.restart_count < self.max_restarts and not self.stop_monitor_event.is_set():
                            if self._restart_ffmpeg_process():
                                self.restart_count += 1
                                logger.info(f"[FFmpeg Monitor] Restart successful (attempt {self.restart_count})")
                                continue  # Continue monitoring new process
                            else:
                                logger.error("[FFmpeg Monitor] Restart failed")
                                break
                        else:
                            logger.error(f"[FFmpeg Monitor] Max restarts ({self.max_restarts}) reached or stopping")
                            break
                            
            except Exception as e:
                logger.error(f"[FFmpeg Monitor] Monitoring error: {e}")
                time.sleep(5.0)  # Back off on errors
        
        # Recording has failed or ended
        with self.lock:
            if self.recording:
                logger.warning("[FFmpeg Monitor] Marking recording as failed due to process exit")
                self.recording_failed = True
                
        logger.debug("[FFmpeg Monitor] Process monitoring stopped")

    def _restart_ffmpeg_process(self):
        """Restart FFmpeg process after failure."""
        try:
            logger.info("[FFmpeg Restart] Attempting to restart FFmpeg process...")
            
            # Clean up old process
            if self.process:
                try:
                    self.process.kill()
                    self.process.wait(timeout=3.0)
                except Exception as e:
                    logger.warning(f"[FFmpeg Restart] Error cleaning up old process: {e}")
            
            # Create new output filename to avoid file conflicts
            base_name = os.path.splitext(self.output_file)[0]
            extension = os.path.splitext(self.output_file)[1]
            timestamp = datetime.now().strftime("%H%M%S")
            new_output = f"{base_name}_part{self.restart_count + 1}_{timestamp}{extension}"
            
            # Update output file
            old_output = self.output_file
            self.output_file = new_output
            
            # Start new FFmpeg process
            if self._start_ffmpeg_process():
                logger.info(f"[FFmpeg Restart] Process restarted successfully: {new_output}")
                return True
            else:
                logger.error("[FFmpeg Restart] Failed to start new process")
                self.output_file = old_output  # Restore original name
                return False
                
        except Exception as e:
            logger.error(f"[FFmpeg Restart] Restart error: {e}")
            return False

    def is_recording_healthy(self):
        """Check if recording is healthy (process alive and no failures)."""
        with self.lock:
            if not self.recording:
                return False
            if self.recording_failed:
                return False
            if not self.process:
                return False
            return self.process.poll() is None  # Process still running

    def get_recording_status(self):
        """Get detailed recording status for monitoring."""
        with self.lock:
            return {
                'recording': self.recording,
                'failed': self.recording_failed,
                'restart_count': self.restart_count,
                'dropped_frames': self.dropped_frames,
                'process_alive': self.process.poll() is None if self.process else False,
                'process_pid': self.process.pid if self.process else None
            }

    # Keep existing _writer_loop and stop_recording methods unchanged
    def _writer_loop(self):
        """Writer thread - sends frames to FFmpeg."""
        while not self.stop_writer_event.is_set():
            try:
                frame = self.write_queue.get(timeout=0.1)
                
                if self.process and self.process.stdin and not self.recording_failed:
                    try:
                        self.process.stdin.write(frame.tobytes())
                    except BrokenPipeError:
                        logger.warning("[Writer] Broken pipe - FFmpeg process ended")
                        with self.lock:
                            self.recording_failed = True
                        break
                    except Exception as e:
                        logger.error(f"[Writer] Error writing frame: {e}")
                        with self.lock:
                            self.recording_failed = True
                        break
                        
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"[Writer] Writer loop error: {e}")
                break

    # ffmpeg_recorder.py - Enhanced FFmpeg Shutdown for Stream Disconnection Fix

    def stop_recording(self, force_immediate=False):
        """Stop FFmpeg recording with enhanced shutdown logic for stream disconnections."""
        logger.debug("[Recorder] Stopping recording...")
        
        with self.lock:
            if not self.recording:
                return
            self.recording = False
            
        # Stop monitoring first
        if self.monitor_thread:
            self.stop_monitor_event.set()
            self.monitor_thread.join(timeout=3.0)
            self.monitor_thread = None
            
        # Stop writer thread
        if self.writer_thread:
            self.stop_writer_event.set()
            self.writer_thread.join(timeout=2.0)
            self.writer_thread = None
            
        # Enhanced FFmpeg shutdown with stream disconnection awareness
        try:
            if self.process and self.process.poll() is None:
                # Check if we should attempt graceful shutdown or force immediate termination
                should_force_immediate = force_immediate or self._should_force_immediate_shutdown()
                
                if should_force_immediate:
                    logger.warning("[Recorder] Stream likely disconnected, forcing immediate FFmpeg termination")
                    self._force_immediate_shutdown()
                else:
                    logger.debug("[Recorder] Attempting graceful FFmpeg shutdown...")
                    self._attempt_graceful_shutdown()
                    
            else:
                logger.debug("[Recorder] FFmpeg process already terminated")
                
        except Exception as e:
            logger.error(f"[Recorder] Error during FFmpeg shutdown: {e}")
            # Always ensure process is terminated
            self._force_immediate_shutdown()
            
        finally:
            self.process = None
            logger.debug(f"[Recorder] Recording stopped. Restarts: {self.restart_count}, Dropped frames: {self.dropped_frames}")


    def _should_force_immediate_shutdown(self) -> bool:
        """
        Determine if FFmpeg shutdown should be immediate rather than graceful.
        
        Returns:
            True if immediate shutdown is recommended
        """
        try:
            # Check if process is responsive by testing stdin
            if not self.process or not self.process.stdin:
                return True
                
            # If we have high restart count, FFmpeg may be in bad state
            if self.restart_count > 0:
                logger.warning("[Recorder] High restart count detected, using immediate shutdown")
                return True
            
            # Check if writer thread had recent errors (broken pipe indicates stream issues)
            # This would be detected by checking if write_queue has been backing up
            try:

                queue_size = self.write_queue.qsize()
                if queue_size > 50:  # If queue is backing up significantly
                    logger.warning(f"[Recorder] Write queue backed up ({queue_size} frames), using immediate shutdown")
                    return True
            except:
                pass
            
            # Test if stdin is still writable (quick test)
            try:

                if os.name is "posix": #and os.name is not "nt":
                    import select
                    import sys

                    if hasattr(select, 'select'):  # Unix-like systems
                        ready, _, _ = select.select([], [self.process.stdin], [], 0)
                        if not ready:  # stdin not ready for writing
                            logger.warning("[Recorder] FFmpeg stdin not ready, using immediate shutdown")
                            return True

                # On Windows, we'll use a timeout-based approach instead
                if os.name is "nt":
                    # Windows: try a non-blocking write to check
                    try:
                        self.process.stdin.write(b"")
                        self.process.stdin.flush()

                    except Exception as e:
                        logger.error(f"[Recorder] Windows stdin not writable: {e}")
                        return True

            except:
                # If we can't test stdin, be safe and use immediate shutdown
                return True
            
            return False
            
        except Exception as e:
            logger.critical(f"[Recorder] Error checking shutdown method: {e}")
            return True  # Default to immediate shutdown on errors

    def _attempt_graceful_shutdown(self):
        """Attempt graceful FFmpeg shutdown with shorter timeout for stream disconnections."""
        try:
            # Send 'q' command for graceful shutdown
            logger.debug("[Recorder] Sending 'q' command to FFmpeg...")
            self.process.stdin.write(b'q\n')
            self.process.stdin.flush()
            
            # Use shorter timeout for graceful shutdown (3 seconds instead of 10)
            # This prevents long waits when stream is disconnected
            logger.debug("[Recorder] Waiting for graceful shutdown (3s timeout)...")
            exit_code = self.process.wait(timeout=3.0)
            logger.debug(f"[Recorder] FFmpeg finished gracefully (exit code: {exit_code})")
        except subprocess.TimeoutExpired:
            logger.warning("[Recorder] Graceful shutdown timeout (3s), terminating...")
            self._terminate_with_timeout()
            
        except (BrokenPipeError, OSError) as e:
            # These errors indicate the stream/process is already in bad state
            logger.warning(f"[Recorder] Process already unresponsive ({e}), terminating immediately")
            self._terminate_with_timeout()
            
        except Exception as e:
            logger.error(f"[Recorder] Error during graceful shutdown: {e}")
            self._terminate_with_timeout()
            
        finally:
            # Always close stdin to prevent resource leaks
            try:
                if self.process and self.process.stdin:
                    self.process.stdin.close()
            except:
                pass

    def _force_immediate_shutdown(self):
        """Force immediate FFmpeg termination without attempting graceful shutdown."""
        try:
            if not self.process:
                return
                
            logger.debug("[Recorder] Forcing immediate FFmpeg termination...")
            
            # Close stdin first to break any blocking operations
            try:
                if self.process.stdin:
                    self.process.stdin.close()
            except:
                pass
                
            # Terminate immediately
            self.process.terminate()
            
            # Wait briefly for termination
            try:
                exit_code = self.process.wait(timeout=2.0)
                logger.info(f"[Recorder] FFmpeg terminated immediately (exit code: {exit_code})")
            except subprocess.TimeoutExpired:
                logger.warning("[Recorder] Termination timeout, force killing...")
                self.process.kill()
                self.process.wait()  # This should not timeout after kill
                logger.info("[Recorder] FFmpeg force killed")
                
        except Exception as e:
            logger.error(f"[Recorder] Error during immediate shutdown: {e}")
            # Last resort - try kill
            try:
                if self.process:
                    self.process.kill()
                    self.process.wait()
            except:
                pass

    def _terminate_with_timeout(self):
        """Terminate FFmpeg with timeout and fallback to kill."""
        try:
            self.process.terminate()
            
            # Wait for termination with short timeout
            exit_code = self.process.wait(timeout=2.0)
            logger.info(f"[Recorder] FFmpeg terminated (exit code: {exit_code})")
            
        except subprocess.TimeoutExpired:
            logger.warning("[Recorder] Termination timeout, force killing...")
            try:
                self.process.kill()
                self.process.wait()  # This should not timeout after kill
                logger.info("[Recorder] FFmpeg force killed")
            except Exception as e:
                logger.error(f"[Recorder] Error during force kill: {e}")

    # Enhanced method for camera worker integration
    def stop_recording_immediate(self):
        """Stop recording with immediate termination - for use during stream failures."""
        self.stop_recording(force_immediate=True)        


    # Keep existing utility methods
    def get_dropped_frames(self):
        return self.dropped_frames

    def reset_dropped_frames(self):
        self.dropped_frames = 0

    def get_dropped_frame_count(self):
        return self.dropped_frames

        