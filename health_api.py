# health_api.py
from flask import Flask, jsonify
import threading
from typing import Optional

from core.logger_config import get_logger
logger = get_logger()  

class HealthAPI:
    """Simple Flask API to expose supervisor and worker health status."""
    
    def __init__(self, worker_manager, port: int = 5001):
        self.worker_manager = worker_manager
        self.port = port
        self.app = Flask(__name__)
        self.server_thread: Optional[threading.Thread] = None
        
        # Register routes
        self._setup_routes()
        
    def _setup_routes(self):
        """Set up Flask routes."""
        
        @self.app.route('/health')
        def health():
            """Overall system health check."""
            try:
                status = self.worker_manager.get_status()
                
                # Determine overall health
                healthy_workers = sum(1 for w in status['workers'].values() 
                                    if w['state'] == 'running')
                total_workers = len(status['workers'])
                
                overall_health = "healthy" if healthy_workers == total_workers else "degraded"
                if healthy_workers == 0:
                    overall_health = "critical"
                    
                return jsonify({
                    'status': overall_health,
                    'timestamp': status['supervisor']['timestamp'],
                    'workers': {
                        'healthy': healthy_workers,
                        'total': total_workers,
                        'percentage': round(healthy_workers / total_workers * 100, 1) if total_workers > 0 else 0
                    }
                })
                
            except Exception as e:
                logger.error(f"Health check error: {e}")
                return jsonify({'status': 'error', 'message': str(e)}), 500
                
        @self.app.route('/status')
        def detailed_status():
            """Detailed status of all components."""
            try:
                return jsonify(self.worker_manager.get_status())
            except Exception as e:
                logger.error(f"Status check error: {e}")
                return jsonify({'error': str(e)}), 500
                
        @self.app.route('/workers')
        def workers():
            """List all workers and their states."""
            try:
                status = self.worker_manager.get_status()
                return jsonify(status['workers'])
            except Exception as e:
                logger.error(f"Workers check error: {e}")
                return jsonify({'error': str(e)}), 500
                
        @self.app.route('/workers/<camera_id>')
        def worker_detail(camera_id):
            """Get detailed status for a specific worker."""
            try:
                status = self.worker_manager.get_status()
                worker_status = status['workers'].get(camera_id)
                
                if not worker_status:
                    return jsonify({'error': f'Worker {camera_id} not found'}), 404
                    
                return jsonify(worker_status)
            except Exception as e:
                logger.error(f"Worker detail error: {e}")
                return jsonify({'error': str(e)}), 500
        
    def start(self):
        """Start the health API server in a background thread."""
        logger.info(f"Starting health API on port {self.port}")
        
        self.server_thread = threading.Thread(
            target=lambda: self.app.run(host='0.0.0.0', port=self.port, debug=False),
            daemon=True
        )
        self.server_thread.start()
        
        logger.info(f"Health API started at http://localhost:{self.port}/health")
        
    def stop(self):
        """Stop the health API server."""
        # Flask doesn't have a clean shutdown method when running in a thread
        # In production, you'd use a proper WSGI server like gunicorn
        logger.info("Health API stopping...")