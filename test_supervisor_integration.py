# test_supervisor_integration.py
import pytest
import time
import multiprocessing as mp
import json
import tempfile
import os
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta

from supervisor import WorkerManager, WorkerState, HeartbeatMessage
from camera_worker import CameraWorker


class TestSupervisorIntegration:
    """Integration tests for supervisor-worker communication."""
    
    @pytest.fixture
    def mock_camera_config(self):
        """Create a temporary camera config for testing."""
        config = {
            "test_cam": {
                "name": "Test Camera",
                "url": "rtsp://test:test@192.168.1.100:554/stream1",
                "sub_url": "rtsp://test:test@192.168.1.100:554/stream2",
                "width": 1920,
                "height": 1080,
                "fps": 15,
                "threshold": 25,
                "area": 500,
                "motion_timeout": 1.5,
                "post_record_time": 5.0
            }
        }
        
        # Create temporary config file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(config, f)
            config_file = f.name
            
        yield config_file, config
        
        # Cleanup
        os.unlink(config_file)
        
    @patch('core.camera_helper.CameraHelper.get_all_camera_ids')
    @patch('core.camera_helper.CameraHelper.get_camera_by_id')
    def test_supervisor_starts_workers(self, mock_get_camera, mock_get_ids, mock_camera_config):
        """Test that supervisor correctly starts worker processes."""
        config_file, config = mock_camera_config
        
        mock_get_ids.return_value = ['test_cam']
        mock_get_camera.return_value = config['test_cam']
        
        # Use shorter timeouts for testing
        manager = WorkerManager(heartbeat_timeout=5.0, max_restart_delay=10.0)
        
        try:
            manager.start()
            
            # Verify worker was created
            assert 'test_cam' in manager.workers
            worker_info = manager.workers['test_cam']
            
            # Give some time for process to start
            time.sleep(2.0)
            
            # Check initial state
            assert worker_info.state in [WorkerState.STARTING, WorkerState.RUNNING]
            assert worker_info.restart_count == 0
            
        finally:
            manager.stop()
            
    def test_heartbeat_message_format(self):
        """Test heartbeat message serialization."""
        heartbeat = HeartbeatMessage(
            worker_id="test_cam",
            timestamp=datetime.now().isoformat(),
            stream_state="capturing",
            fps=15.5,
            recording=False,
            error_message=None
        )
        
        # Should be JSON serializable
        data = json.dumps(heartbeat.__dict__)
        parsed = json.loads(data)
        
        assert parsed['worker_id'] == "test_cam"
        assert parsed['schema_version'] == "1.0"
        assert parsed['fps'] == 15.5
        assert parsed['recording'] is False
        
    @patch('cv2.VideoCapture')
    def test_worker_mock_capture(self, mock_cv2):
        """Test worker with mocked video capture."""
        # Mock OpenCV VideoCapture
        mock_cap = MagicMock()
        mock_cap.isOpened.return_value = True
        mock_cap.read.return_value = (True, MagicMock())  # (success, frame)
        mock_cv2.return_value = mock_cap
        
        cmd_queue = mp.Queue()
        status_queue = mp.Queue()
        
        with patch('core.camera_helper.CameraHelper.get_camera_by_id') as mock_get_camera:
            mock_get_camera.return_value = {
                'url': 'rtsp://test/stream1',
                'sub_url': 'rtsp://test/stream2',
                'fps': 15,
                'threshold': 25,
                'area': 500
            }
            
            worker = CameraWorker('test_cam', cmd_queue, status_queue)
            
            # Start worker in separate process for realistic test
            def run_worker():
                worker.start()
                
            worker_process = mp.Process(target=run_worker)
            worker_process.start()
            
            try:
                # Wait for heartbeat messages
                start_time = time.time()
                heartbeat_received = False
                
                while time.time() - start_time < 10.0:  # 10 second timeout
                    try:
                        msg = status_queue.get(timeout=1.0)
                        if msg.get('worker_id') == 'test_cam':
                            heartbeat_received = True
                            break
                    except:
                        continue
                        
                assert heartbeat_received, "No heartbeat received from worker"
                
            finally:
                worker_process.terminate()
                worker_process.join(timeout=5.0)
                
    def test_worker_restart_logic(self, mock_camera_config):
        """Test supervisor restart logic when worker crashes."""
        config_file, config = mock_camera_config
        
        with patch('core.camera_helper.CameraHelper.get_all_camera_ids') as mock_get_ids:
            with patch('core.camera_helper.CameraHelper.get_camera_by_id') as mock_get_camera:
                mock_get_ids.return_value = ['test_cam']
                mock_get_camera.return_value = config['test_cam']
                
                manager = WorkerManager(heartbeat_timeout=2.0, max_restart_delay=5.0)
                
                try:
                    manager.start()
                    
                    # Get worker info
                    worker_info = manager.workers['test_cam']
                    original_process = worker_info.process
                    
                    # Simulate worker crash
                    if original_process and original_process.is_alive():
                        original_process.terminate()
                        original_process.join(timeout=2.0)
                        
                    # Wait for supervisor to detect crash and schedule restart
                    time.sleep(3.0)
                    
                    # Verify restart was scheduled
                    assert worker_info.state == WorkerState.CRASHED
                    assert worker_info.restart_count > 0
                    assert worker_info.next_restart is not None
                    
                    # Wait for restart
                    time.sleep(8.0)  # Wait longer than restart delay
                    
                    # Verify new process was started
                    assert worker_info.process != original_process
                    
                finally:
                    manager.stop()


class TestWorkerIsolation:
    """Test that worker failures don't affect supervisor or other workers."""
    
    def test_multiple_workers_isolation(self):
        """Test that one worker crashing doesn't affect others."""
        with patch('core.camera_helper.CameraHelper.get_all_camera_ids') as mock_get_ids:
            with patch('core.camera_helper.CameraHelper.get_camera_by_id') as mock_get_camera:
                # Setup two cameras
                mock_get_ids.return_value = ['cam1', 'cam2']
                mock_get_camera.side_effect = lambda cid: {
                    'url': f'rtsp://test/{cid}',
                    'fps': 15,
                    'threshold': 25,
                    'area': 500
                }
                
                manager = WorkerManager(heartbeat_timeout=3.0)
                
                try:
                    manager.start()
                    time.sleep(2.0)  # Let workers start
                    
                    # Verify both workers started
                    assert len(manager.workers) == 2
                    cam1_worker = manager.workers['cam1']
                    cam2_worker = manager.workers['cam2']
                    
                    # Crash cam1 worker
                    if cam1_worker.process and cam1_worker.process.is_alive():
                        cam1_worker.process.terminate()
                        cam1_worker.process.join(timeout=2.0)
                        
                    time.sleep(4.0)  # Wait for supervisor to detect
                    
                    # Verify cam1 is marked as crashed but cam2 is still running
                    assert cam1_worker.state == WorkerState.CRASHED
                    assert cam2_worker.state in [WorkerState.RUNNING, WorkerState.STARTING]
                    assert cam2_worker.process and cam2_worker.process.is_alive()
                    
                finally:
                    manager.stop()


class TestPerformanceBenchmarks:
    """Performance and resource usage tests."""
    
    def test_heartbeat_frequency(self):
        """Test that heartbeat messages arrive at expected frequency."""
        cmd_queue = mp.Queue()
        status_queue = mp.Queue()
        
        with patch('core.camera_helper.CameraHelper.get_camera_by_id') as mock_get_camera:
            with patch('cv2.VideoCapture'):
                mock_get_camera.return_value = {
                    'url': 'rtsp://test/stream1',
                    'sub_url': 'rtsp://test/stream2',
                    'fps': 15
                }
                
                worker = CameraWorker('test_cam', cmd_queue, status_queue)
                
                worker_process = mp.Process(target=worker.start)
                worker_process.start()
                
                try:
                    # Collect heartbeats for 15 seconds
                    heartbeats = []
                    start_time = time.time()
                    
                    while time.time() - start_time < 15.0:
                        try:
                            msg = status_queue.get(timeout=1.0)
                            if msg.get('worker_id') == 'test_cam':
                                heartbeats.append(time.time())
                        except:
                            continue
                            
                    # Should receive ~3 heartbeats (every 5 seconds)
                    assert len(heartbeats) >= 2, f"Expected at least 2 heartbeats, got {len(heartbeats)}"
                    
                    # Check frequency - should be ~5 seconds apart
                    if len(heartbeats) > 1:
                        intervals = [heartbeats[i] - heartbeats[i-1] for i in range(1, len(heartbeats))]
                        avg_interval = sum(intervals) / len(intervals)
                        assert 4.0 <= avg_interval <= 6.0, f"Heartbeat interval {avg_interval:.1f}s outside expected range"
                        
                finally:
                    worker_process.terminate()
                    worker_process.join(timeout=5.0)


@pytest.fixture
def temp_output_dir():
    """Create temporary output directory for test recordings."""
    import tempfile
    import shutil
    
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir, ignore_errors=True)


def test_integration_full_flow(temp_output_dir):
    """End-to-end integration test with file-based video source."""
    
    # Create a simple test video file using OpenCV
    import cv2
    import numpy as np
    
    test_video_path = os.path.join(temp_output_dir, "test_input.mp4")
    
    # Create test video - moving white square on black background
    fourcc = cv2.VideoWriter_fourcc(*'mp4v')
    out = cv2.VideoWriter(test_video_path, fourcc, 15.0, (640, 480))
    
    for i in range(150):  # 10 seconds at 15 fps
        frame = np.zeros((480, 640, 3), dtype=np.uint8)
        # Moving square to trigger motion
        x = (i * 5) % 600
        cv2.rectangle(frame, (x, 200), (x + 40, 240), (255, 255, 255), -1)
        out.write(frame)
        
    out.release()
    
    # Mock camera config to use file instead of RTSP
    test_config = {
        'test_file_cam': {
            'url': test_video_path,
            'sub_url': test_video_path,
            'fps': 15,
            'width': 640,
            'height': 480,
            'threshold': 25,
            'area': 500
        }
    }
    
    with patch('core.camera_helper.CameraHelper.get_all_camera_ids') as mock_get_ids:
        with patch('core.camera_helper.CameraHelper.get_camera_by_id') as mock_get_camera:
            mock_get_ids.return_value = ['test_file_cam']
            mock_get_camera.return_value = test_config['test_file_cam']
            
            manager = WorkerManager(heartbeat_timeout=5.0)
            
            try:
                manager.start()
                
                # Run for 30 seconds
                start_time = time.time()
                while time.time() - start_time < 30.0:
                    status = manager.get_status()
                    
                    # Check that worker is running
                    worker_status = status['workers'].get('test_file_cam')
                    if worker_status:
                        assert worker_status['state'] in ['starting', 'running']
                        
                    time.sleep(2.0)
                    
                # Final status check
                final_status = manager.get_status()
                assert final_status['supervisor']['running']
                assert len(final_status['workers']) == 1
                
            finally:
                manager.stop()


if __name__ == "__main__":
    # Simple test runner
    pytest.main([__file__, "-v"])