# supervisor.py
import multiprocessing as mp
import time
import json
import threading
from typing import Dict, Optional, List
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from enum import Enum
import signal
import sys
import os

from core.camera_helper import CameraHelper
from core.config_validator import ConfigValidator, ConfigValidationError

from core.logger_config import get_logger
logger = get_logger()  # Will show [supervisor] in logs

class WorkerState(Enum):
    STARTING = "starting"
    RUNNING = "running"  
    UNHEALTHY = "unhealthy"
    CRASHED = "crashed"
    STOPPING = "stopping"


@dataclass
class WorkerInfo:
    camera_id: str
    process: Optional[mp.Process]
    state: WorkerState
    last_heartbeat: Optional[datetime]
    restart_count: int
    next_restart: Optional[datetime]
    cmd_queue: mp.Queue
    status_queue: mp.Queue


@dataclass
class HeartbeatMessage:
    schema_version: str = "1.0"
    worker_id: str = ""
    timestamp: str = ""
    stream_state: str = "idle"  # idle, capturing, recording, error
    fps: float = 0.0
    recording: bool = False
    error_message: Optional[str] = None


@dataclass  
class CommandMessage:
    schema_version: str = "1.0"
    command: str = ""  # start, stop, restart, ptz_move
    camera_id: str = ""
    params: Dict = None


class WorkerManager:
    """Manages camera worker processes with heartbeat monitoring and restart logic."""
    
    def __init__(self, heartbeat_timeout: float = 15.0, max_restart_delay: float = 60.0):
        self.workers: Dict[str, WorkerInfo] = {}
        self.heartbeat_timeout = heartbeat_timeout
        self.max_restart_delay = max_restart_delay
        self.running = True
        self.monitor_thread: Optional[threading.Thread] = None
        
    def start(self):
        """Start the worker manager and monitoring thread."""
        logger.info("Starting WorkerManager...")
        
        # # Load camera configurations
        # camera_ids = CameraHelper.get_all_camera_ids()
        # logger.info(f"Found {len(camera_ids)} cameras: {camera_ids}")
        
        try:
            # Load and validate camera configurations
            from core.camera_helper import CameraHelper
            cameras = CameraHelper._load_cameras()
            
            # NEW: Validate all camera configurations
            logger.info("Validating camera configurations...")
            validated_cameras = ConfigValidator.validate_all_cameras(cameras)
            ConfigValidator.print_validation_summary(validated_cameras)
            
            # Filter out enabled cameras
            enabled_cameras = {
                    cam_id: cfg
                    for cam_id, cfg in validated_cameras.items()
                    if cfg.get("enabled", True)
                }
            
            # Use validated enabled camera IDs
            camera_ids = list(enabled_cameras.keys())
            logger.info(f"Found {len(camera_ids)} valid enabled cameras: {camera_ids}")
            
        except ConfigValidationError as e:
            logger.error(f"Camera configuration validation failed: {e}")
            raise  # Stop startup if configuration is invalid
        except Exception as e:
            logger.error(f"Error loading camera configurations: {e}")
            raise
        
        # Initialize workers for each camera
        for camera_id in camera_ids:
            self._init_worker(camera_id)
            
        # Start monitoring thread
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()
        
        logger.info(f"WorkerManager started with {len(self.workers)} workers")
        
    def stop(self):
        """Stop all workers and monitoring."""
        logger.info("Stopping WorkerManager...")
        self.running = False
        
        # Stop all workers
        for worker_info in self.workers.values():
            self._stop_worker(worker_info)
            
        # Wait for monitor thread
        if self.monitor_thread and self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=5.0)
            
        logger.info("WorkerManager stopped")
        
    def _init_worker(self, camera_id: str):
        """Initialize a worker for the given camera."""
        logger.info(f"Initializing worker for camera {camera_id}")
        
        cmd_queue = mp.Queue(maxsize=100)
        status_queue = mp.Queue(maxsize=1000)
        
        worker_info = WorkerInfo(
            camera_id=camera_id,
            process=None,
            state=WorkerState.STARTING,
            last_heartbeat=None,
            restart_count=0,
            next_restart=None,
            cmd_queue=cmd_queue,
            status_queue=status_queue
        )
        
        self.workers[camera_id] = worker_info
        self._start_worker_process(worker_info)
        
    def _start_worker_process(self, worker_info: WorkerInfo):
        """Start the actual worker process."""
        try:
            from camera_worker import CameraWorker  # Import here to avoid circular deps
            
            process = mp.Process(
                target=CameraWorker.run,
                args=(worker_info.camera_id, worker_info.cmd_queue, worker_info.status_queue),
                name=f"worker-{worker_info.camera_id}"
            )
            process.start()
            
            worker_info.process = process
            worker_info.state = WorkerState.RUNNING
            worker_info.last_heartbeat = datetime.now()
            
            logger.info(f"Started worker process {process.pid} for camera {worker_info.camera_id}")
            
        except Exception as e:
            logger.error(f"Failed to start worker for camera {worker_info.camera_id}: {e}")
            worker_info.state = WorkerState.CRASHED
            self._schedule_restart(worker_info)
            
    def _stop_worker(self, worker_info: WorkerInfo):
        """Stop a worker process gracefully."""
        if worker_info.process and worker_info.process.is_alive():
            logger.info(f"Stopping worker {worker_info.camera_id} (PID: {worker_info.process.pid})")
            
            # Send stop command
            try:
                cmd = CommandMessage(command="stop", camera_id=worker_info.camera_id)
                worker_info.cmd_queue.put_nowait(asdict(cmd))
            except:
                pass  # Queue might be full
                
            # Wait for graceful shutdown
            worker_info.process.join(timeout=5.0)
            
            # Force terminate if still alive
            if worker_info.process.is_alive():
                logger.warning(f"Force terminating worker {worker_info.camera_id}")
                worker_info.process.terminate()
                worker_info.process.join(timeout=2.0)
                
            if worker_info.process.is_alive():
                logger.error(f"Failed to stop worker {worker_info.camera_id}")
                
        worker_info.state = WorkerState.STOPPING
        
    def _schedule_restart(self, worker_info: WorkerInfo):
        """Schedule a worker restart with exponential backoff."""
        delay = min(2 ** worker_info.restart_count, self.max_restart_delay)
        worker_info.next_restart = datetime.now() + timedelta(seconds=delay)
        worker_info.restart_count += 1
        
        logger.warning(f"Scheduled restart for worker {worker_info.camera_id} in {delay}s (attempt #{worker_info.restart_count})")
        
    def _monitor_loop(self):
        """Main monitoring loop - runs in background thread."""
        logger.info("Starting worker monitor loop")
        
        while self.running:
            try:
                current_time = datetime.now()
                
                # Process status messages from all workers
                self._process_status_messages()
                
                # Check worker health and restart if needed
                for worker_info in self.workers.values():
                    self._check_worker_health(worker_info, current_time)
                    
                time.sleep(2.0)  # Monitor every 2 seconds
                
            except Exception as e:
                logger.error(f"Error in monitor loop: {e}")
                time.sleep(5.0)
                
        logger.info("Worker monitor loop stopped")

    # supervisor.py - Replace the _process_status_messages method

    def _process_status_messages(self):
        """Process heartbeat and status messages from workers with enhanced error handling."""
        for worker_info in self.workers.values():
            try:
                while not worker_info.status_queue.empty():
                    msg_data = worker_info.status_queue.get_nowait()
                    
                    # Parse heartbeat message
                    if isinstance(msg_data, dict) and msg_data.get('timestamp'):
                        worker_info.last_heartbeat = datetime.now()
                        
                        # Extract detailed status
                        state = msg_data.get('stream_state', 'unknown')
                        fps = msg_data.get('fps', 0.0)
                        recording = msg_data.get('recording', False)
                        error_msg = msg_data.get('error_message')
                        
                        # Assess worker health based on stream state and errors
                        if error_msg:
                            if "Max connection failures" in error_msg or "No valid camera URL" in error_msg:
                                # Critical errors - mark as unhealthy
                                if worker_info.state == WorkerState.RUNNING:
                                    logger.error(f"Worker {worker_info.camera_id} critical error: {error_msg}")
                                    worker_info.state = WorkerState.UNHEALTHY
                            else:
                                # Transient errors - log but don't change state immediately
                                logger.warning(f"Worker {worker_info.camera_id} transient error: {error_msg}")
                        else:
                            # No errors - check if recovering from unhealthy state
                            if worker_info.state == WorkerState.UNHEALTHY and fps > 0:
                                logger.info(f"Worker {worker_info.camera_id} recovered (state: {state}, fps: {fps:.1f})")
                                worker_info.state = WorkerState.RUNNING
                                worker_info.restart_count = 0  # Reset restart count on recovery
                            elif worker_info.state == WorkerState.RUNNING:
                                # Normal operation - just log at debug level
                                logger.debug(f"Worker {worker_info.camera_id} healthy: {state} (fps: {fps:.1f}, recording: {recording})")
                        
            except Exception as e:
                logger.error(f"Error processing status from worker {worker_info.camera_id}: {e}")

    # supervisor.py - Also replace the _check_worker_health method

    def _check_worker_health(self, worker_info: WorkerInfo, current_time: datetime):
        """Check if a worker is healthy and restart if needed with enhanced criteria."""
        
        # Check if process is alive
        if worker_info.process and not worker_info.process.is_alive():
            exit_code = worker_info.process.exitcode
            logger.error(f"Worker {worker_info.camera_id} process died (exit code: {exit_code})")
            worker_info.state = WorkerState.CRASHED
            self._schedule_restart(worker_info)
            return
            
        # Check heartbeat timeout
        if (worker_info.last_heartbeat and 
            current_time - worker_info.last_heartbeat > timedelta(seconds=self.heartbeat_timeout)):
            
            if worker_info.state == WorkerState.RUNNING:
                logger.warning(f"Worker {worker_info.camera_id} missed heartbeat (last: {worker_info.last_heartbeat})")
                worker_info.state = WorkerState.UNHEALTHY
                
            # If unhealthy for too long, restart
            elif (worker_info.state == WorkerState.UNHEALTHY and 
                current_time - worker_info.last_heartbeat > timedelta(seconds=self.heartbeat_timeout * 2)):
                logger.error(f"Worker {worker_info.camera_id} unresponsive, restarting...")
                self._stop_worker(worker_info)
                worker_info.state = WorkerState.CRASHED
                self._schedule_restart(worker_info)
        
        # Check for persistent unhealthy state (new check)
        elif (worker_info.state == WorkerState.UNHEALTHY and 
            worker_info.last_heartbeat and
            current_time - worker_info.last_heartbeat > timedelta(seconds=self.heartbeat_timeout * 3)):
            # Worker is sending heartbeats but reporting persistent errors
            logger.error(f"Worker {worker_info.camera_id} persistently unhealthy, restarting...")
            self._stop_worker(worker_info)
            worker_info.state = WorkerState.CRASHED
            self._schedule_restart(worker_info)
            
        # Handle scheduled restarts
        if (worker_info.state == WorkerState.CRASHED and 
            worker_info.next_restart and 
            current_time >= worker_info.next_restart):
            
            logger.info(f"Restarting worker {worker_info.camera_id} (attempt #{worker_info.restart_count})")
            self._start_worker_process(worker_info)


    def get_status(self) -> Dict:
        """Get status of all workers for health endpoint."""
        status = {
            'supervisor': {
                'running': self.running,
                'timestamp': datetime.now().isoformat(),
                'worker_count': len(self.workers)
            },
            'workers': {}
        }
        
        for camera_id, worker_info in self.workers.items():
            status['workers'][camera_id] = {
                'state': worker_info.state.value,
                'last_heartbeat': worker_info.last_heartbeat.isoformat() if worker_info.last_heartbeat else None,
                'restart_count': worker_info.restart_count,
                'process_alive': worker_info.process.is_alive() if worker_info.process else False,
                'next_restart': worker_info.next_restart.isoformat() if worker_info.next_restart else None
            }
            
        return status


def signal_handler(signum, frame):
    """Handle shutdown signals."""
    logger.info(f"Received signal {signum}, shutting down...")
    global worker_manager
    if worker_manager:
        worker_manager.stop()
    sys.exit(0)


def main():
    """Main supervisor entry point."""
    logger.info("Starting NVR Supervisor")
    
    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    global worker_manager
    worker_manager = WorkerManager()
    
    try:
        worker_manager.start()
        
        # Keep supervisor alive
        while worker_manager.running:
            time.sleep(5.0)
            
            # Periodic status log
            status = worker_manager.get_status()
            running_workers = sum(1 for w in status['workers'].values() if w['state'] == 'running')
            logger.info(f"Supervisor status: {running_workers}/{len(status['workers'])} workers running")
            
    except KeyboardInterrupt:
        logger.info("Supervisor interrupted by user")
    except Exception as e:
        logger.error(f"Supervisor error: {e}")
    finally:
        worker_manager.stop()
        logger.info("Supervisor shutdown complete")


if __name__ == "__main__":
    main()