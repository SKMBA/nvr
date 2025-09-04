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
    # ffmpeg_recorder.py - Frame Queue Memory Leak Fix
    
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
        
        # Frame queue memory management (NEW)
        self.queue_high_watermark = 800  # Alert when queue grows beyond this
        self.queue_critical_threshold = 950  # Force cleanup when queue reaches this
        self.last_queue_cleanup = time.time()
        self.queue_cleanup_interval = 5.0  # Cleanup check every 5 seconds
        logger.debug(f"[FFmpegRecorder] Initialized with frame_size={self.frame_size}")
        
    # ffmpeg_recorder.py - Frame Queue Memory Leak Fix
    def add_frame(self, frame):
        """Add frame to recording with memory leak prevention."""
        try:
            resized = cv2.resize(frame, self.frame_size)
        except Exception as e:
            logger.warning(f"[FFmpegRecorder] Error resizing frame: {e}")
            return
        
        with self.lock:
            # Always add to pre-record buffer (circular buffer with maxlen)
            self.frame_queue.append(resized)
            
            # Only add to write queue if recording is active and healthy
            if self.recording and not self.recording_failed:
                # Check queue health before adding frame
                if self._should_add_frame_to_queue():
                    try:
                        self.write_queue.put_nowait(resized)
                    except queue.Full:
                        self.dropped_frames += 1
                        
                        # Check if queue is critically full
                        current_queue_size = self._get_queue_size()
                        if current_queue_size >= self.queue_critical_threshold:
                            logger.error(f"[FFmpegRecorder] Write queue critically full ({current_queue_size} frames), forcing cleanup")
                            self._emergency_queue_cleanup()
                        else:
                            logger.warning(f"[FFmpegRecorder] Write queue full! Dropping frame #{self.dropped_frames} (queue size: {current_queue_size})")
                else:
                    # Recording is unhealthy, don't add more frames
                    logger.warning("[FFmpegRecorder] Skipping frame addition - recording unhealthy")
    
    def _should_add_frame_to_queue(self) -> bool:
        """
        Determine if frame should be added to write queue based on system health.
        
        Returns:
            True if frame should be added, False otherwise
        """
        try:
            # Check if recording has failed
            if self.recording_failed:
                return False
            
            # Check if FFmpeg process is still alive
            if not self.process or self.process.poll() is not None:
                logger.debug("[FFmpegRecorder] FFmpeg process not running, skipping frame")
                return False
            
            # Check queue size and health
            current_queue_size = self._get_queue_size()
            
            # Don't add frames if queue is backing up significantly
            if current_queue_size >= self.queue_high_watermark:
                logger.warning(f"[FFmpegRecorder] Queue backing up ({current_queue_size} frames), skipping frame addition")
                return False
            
            # Periodic queue cleanup check
            current_time = time.time()
            if current_time - self.last_queue_cleanup >= self.queue_cleanup_interval:
                self._periodic_queue_maintenance()
                self.last_queue_cleanup = current_time
            
            return True
            
        except Exception as e:
            logger.error(f"[FFmpegRecorder] Error checking queue health: {e}")
            return False

    def _get_queue_size(self) -> int:
        """
        Safely get current write queue size.
        
        Returns:
            Current queue size, or 0 if error
        """
        try:
            return self.write_queue.qsize()
        except Exception as e:
            logger.warning(f"[FFmpegRecorder] Error getting queue size: {e}")
            return 0

    def _periodic_queue_maintenance(self):
        """Perform periodic queue health checks and maintenance."""
        try:
            queue_size = self._get_queue_size()
            
            # Log queue health for monitoring
            if queue_size > self.queue_high_watermark:
                logger.warning(f"[FFmpegRecorder] Queue size high: {queue_size} frames")
            elif queue_size > self.queue_high_watermark // 2:
                logger.debug(f"[FFmpegRecorder] Queue size moderate: {queue_size} frames")
            
            # Check if writer thread is still alive
            if self.writer_thread and not self.writer_thread.is_alive():
                logger.error("[FFmpegRecorder] Writer thread died, marking recording as failed")
                with self.lock:
                    self.recording_failed = True
                    
        except Exception as e:
            logger.error(f"[FFmpegRecorder] Error in periodic queue maintenance: {e}")

    def _emergency_queue_cleanup(self):
        """Emergency queue cleanup when memory usage is critical."""
        try:
            logger.warning("[FFmpegRecorder] Performing emergency queue cleanup")
            
            # Mark recording as failed to stop new frames
            self.recording_failed = True
            
            # Try to drain some frames from the queue
            drained = 0
            max_drain = 200  # Don't drain everything, just reduce pressure
            
            while drained < max_drain:
                try:
                    self.write_queue.get_nowait()
                    drained += 1
                except queue.Empty:
                    break
                    
            if drained > 0:
                logger.info(f"[FFmpegRecorder] Emergency cleanup drained {drained} frames")
                self.dropped_frames += drained
            
        except Exception as e:
            logger.error(f"[FFmpegRecorder] Error during emergency queue cleanup: {e}")

    def _drain_write_queue(self):
        """Drain all remaining frames from write queue to prevent memory leaks."""
        try:
            drained_frames = 0
            
            while True:
                try:
                    self.write_queue.get_nowait()
                    drained_frames += 1
                    
                    # Safety limit to prevent infinite loop
                    if drained_frames > 2000:
                        logger.warning("[FFmpegRecorder] Queue drain safety limit reached")
                        break
                        
                except queue.Empty:
                    break
                    
            if drained_frames > 0:
                logger.info(f"[FFmpegRecorder] Drained {drained_frames} frames from write queue")
                
            return drained_frames
            
        except Exception as e:
            logger.error(f"[FFmpegRecorder] Error draining write queue: {e}")
            return 0


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
    # ffmpeg_recorder.py - Frame Queue Memory Leak Fix
    def _writer_loop(self):
        """Writer thread with enhanced error handling and memory management."""
        logger.info("[Writer] Writer thread started")
        frame_write_errors = 0
        max_write_errors = 10
        
        while not self.stop_writer_event.is_set():
            try:
                # Use timeout to allow periodic checks
                try:
                    frame = self.write_queue.get(timeout=0.5)
                except queue.Empty:
                    # Check if we should continue running
                    continue
                
                # Check if recording has failed
                with self.lock:
                    if self.recording_failed:
                        logger.info("[Writer] Recording marked as failed, stopping writer")
                        break
                
                # Write frame to FFmpeg process
                if self.process and self.process.stdin:
                    try:
                        self.process.stdin.write(frame.tobytes())
                        self.process.stdin.flush()
                        frame_write_errors = 0  # Reset error count on success
                        
                    except BrokenPipeError:
                        logger.warning("[Writer] Broken pipe - FFmpeg process ended")
                        with self.lock:
                            self.recording_failed = True
                        break
                        
                    except Exception as e:
                        frame_write_errors += 1
                        logger.error(f"[Writer] Error writing frame: {e} (error #{frame_write_errors})")
                        
                        # Stop writing after too many errors
                        if frame_write_errors >= max_write_errors:
                            logger.error("[Writer] Too many write errors, stopping writer")
                            with self.lock:
                                self.recording_failed = True
                            break
                        
                        # Brief pause before retry
                        time.sleep(0.1)
                else:
                    logger.warning("[Writer] No valid FFmpeg process or stdin")
                    with self.lock:
                        self.recording_failed = True
                    break
                    
            except Exception as e:
                logger.error(f"[Writer] Writer loop error: {e}")
                with self.lock:
                    self.recording_failed = True
                break
        
        # Cleanup: drain any remaining frames to prevent memory leaks
        logger.info("[Writer] Writer thread stopping, draining remaining frames")
        drained = self._drain_write_queue()
        
        if drained > 0:
            logger.info(f"[Writer] Writer thread drained {drained} frames during cleanup")
        
        logger.info("[Writer] Writer thread stopped")

    

    # ffmpeg_recorder.py - Enhanced FFmpeg Shutdown for Stream Disconnection Fix
    # ffmpeg_recorder.py - Frame Queue Memory Leak Fix
    def stop_recording(self, force_immediate=False):
        """Stop FFmpeg recording with enhanced queue cleanup."""
        logger.info("[Recorder] Stopping recording...")
        
        with self.lock:
            if not self.recording:
                return
            self.recording = False
            
        # Stop monitoring first
        if self.monitor_thread:
            self.stop_monitor_event.set()
            self.monitor_thread.join(timeout=3.0)
            self.monitor_thread = None
            
        # Signal writer thread to stop and wait for cleanup
        if self.writer_thread:
            self.stop_writer_event.set()
            logger.debug("[Recorder] Waiting for writer thread to complete cleanup...")
            self.writer_thread.join(timeout=5.0)  # Longer timeout for queue cleanup
            
            if self.writer_thread.is_alive():
                logger.warning("[Recorder] Writer thread did not stop gracefully")
            
            self.writer_thread = None
            
        # Additional queue cleanup after writer thread stops
        remaining_frames = self._drain_write_queue()
        if remaining_frames > 0:
            logger.info(f"[Recorder] Final cleanup drained additional {remaining_frames} frames")
            
        # Enhanced FFmpeg shutdown with stream disconnection awareness
        try:
            if self.process and self.process.poll() is None:
                should_force_immediate = force_immediate or self._should_force_immediate_shutdown()
                
                if should_force_immediate:
                    logger.warning("[Recorder] Stream likely disconnected, forcing immediate FFmpeg termination")
                    self._force_immediate_shutdown()
                else:
                    logger.warning("[Recorder] Attempting graceful FFmpeg shutdown...")
                    self._attempt_graceful_shutdown()
                    
            else:
                logger.debug("[Recorder] FFmpeg process already terminated")
                
        except Exception as e:
            logger.error(f"[Recorder] Error during FFmpeg shutdown: {e}")
            self._force_immediate_shutdown()
            
        finally:
            self.process = None
            
            # Reset state and cleanup
            with self.lock:
                self.recording_failed = False
                
            # Clear any remaining frames in pre-record buffer if needed
            # (though this shouldn't be necessary due to maxlen)
            self.frame_queue.clear()
            
            logger.info(f"[Recorder] Recording stopped with enhanced cleanup. "
                       f"Restarts: {self.restart_count}, Dropped frames: {self.dropped_frames}")

    # ffmpeg_recorder.py - Frame Queue Memory Leak Fix
    def get_queue_stats(self) -> dict:
        """
        Get current queue statistics for monitoring.
        
        Returns:
            Dictionary with queue health statistics
        """
        try:
            with self.lock:
                return {
                    'write_queue_size': self._get_queue_size(),
                    'pre_record_queue_size': len(self.frame_queue),
                    'recording': self.recording,
                    'recording_failed': self.recording_failed,
                    'dropped_frames': self.dropped_frames,
                    'queue_high_watermark': self.queue_high_watermark,
                    'queue_critical_threshold': self.queue_critical_threshold
                }
        except Exception as e:
            logger.error(f"[FFmpegRecorder] Error getting queue stats: {e}")
            return {}
        
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

        