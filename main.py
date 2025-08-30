# main.py - Integrated NVR Entry Point
import os
import sys
import time
import signal
import multiprocessing as mp
from pathlib import Path

from core.logger_config import logger
from supervisor import WorkerManager
from health_api import HealthAPI


def get_runtime_path():
    """Get the runtime path for the application."""
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))


class NVRApplication:
    """Main NVR application orchestrator."""
    
    def __init__(self):
        from typing import Optional
        self.worker_manager: Optional[WorkerManager] = None
        self.health_api: Optional[HealthAPI] = None
        self.running = True
        
        # Ensure output directory exists
        self.output_dir = Path("output")
        self.output_dir.mkdir(exist_ok=True)
        
    def start(self):
        """Start the NVR application."""
        logger.info("=" * 60)
        logger.info("Starting NVR Application")
        logger.info("=" * 60)
        
        try:
            # Initialize worker manager
            self.worker_manager = WorkerManager(
                heartbeat_timeout=15.0,    # 15 second heartbeat timeout
                max_restart_delay=60.0     # Max 60 second restart delay
            )
            
            # Start health API
            self.health_api = HealthAPI(self.worker_manager, port=5001)
            self.health_api.start()
            
            # Start worker manager
            self.worker_manager.start()
            
            logger.info("NVR Application started successfully")
            logger.info("Health API available at: http://localhost:5001/health")
            logger.info("Detailed status at: http://localhost:5001/status")
            logger.info("Press Ctrl+C to stop")
            
            # Main application loop
            self._main_loop()
            
        except Exception as e:
            logger.error(f"Failed to start NVR application: {e}")
            self.stop()
            raise
            
    def _main_loop(self):
        """Main application loop with periodic status logging."""
        last_status_log = time.time()
        status_log_interval = 60.0  # Log status every minute
        
        while self.running:
            try:
                current_time = time.time()
                
                # Periodic status logging
                if current_time - last_status_log >= status_log_interval:
                    self._log_system_status()
                    last_status_log = current_time
                    
                time.sleep(5.0)  # Main loop runs every 5 seconds
                
            except KeyboardInterrupt:
                logger.info("Application interrupted by user")
                break
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                time.sleep(10.0)  # Back off on errors
                
    def _log_system_status(self):
        """Log periodic system status."""
        if not self.worker_manager:
            return
            
        try:
            status = self.worker_manager.get_status()
            
            running_workers = sum(1 for w in status['workers'].values() if w['state'] == 'running')
            total_workers = len(status['workers'])
            
            logger.info(f"System Status: {running_workers}/{total_workers} workers running")
            
            # Log any unhealthy workers
            for camera_id, worker_status in status['workers'].items():
                if worker_status['state'] != 'running':
                    logger.warning(f"Worker {camera_id}: {worker_status['state']} (restarts: {worker_status['restart_count']})")
                    
        except Exception as e:
            logger.error(f"Error logging system status: {e}")
            
    def stop(self):
        """Stop the NVR application."""
        logger.info("Stopping NVR Application...")
        
        self.running = False
        
        # Stop worker manager
        if self.worker_manager:
            self.worker_manager.stop()
            
        # Stop health API
        if self.health_api:
            self.health_api.stop()
            
        logger.info("NVR Application stopped")
        
    def signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}, initiating shutdown...")
        self.stop()
        sys.exit(0)


def main():
    """Main entry point."""
    # Configure multiprocessing for Windows
    if sys.platform == "win32":
        mp.set_start_method('spawn', force=True)
        
    # Create application
    app = NVRApplication()
    
    # Set up signal handlers
    signal.signal(signal.SIGINT, app.signal_handler)
    signal.signal(signal.SIGTERM, app.signal_handler)
    
    # Start application
    try:
        app.start()
    except Exception as e:
        logger.error(f"Application failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()