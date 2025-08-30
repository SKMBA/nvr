# NVR Supervisor-Worker Architecture Design Summary

## Core Design Decisions

### 1. **Multiprocessing Architecture**
- **Choice**: `multiprocessing.Process` for workers + `threading.Thread` within workers
- **Rationale**: Process isolation prevents camera crashes from affecting supervisor or other cameras
- **Windows Compatibility**: Uses `spawn` method for clean process separation

### 2. **IPC Strategy**
- **Choice**: `multiprocessing.Queue` for supervisor ↔ worker communication
- **Alternative Considered**: Sockets/pipes for cross-machine flexibility
- **Decision**: Queues provide better error handling and built-in serialization for local deployment

### 3. **Dual Stream Architecture**
- **SUB Stream Thread**: Low-res preview + motion detection (camera_helper + motion_detector)
- **MAIN Stream Thread**: High-res recording via FFmpeg (ffmpeg_recorder)
- **Benefit**: Motion detection doesn't impact recording quality/performance

### 4. **Heartbeat & Health Monitoring**
- **Frequency**: 5-second heartbeat interval
- **Timeout**: 15-second unhealthy threshold, 30-second restart threshold  
- **Backoff**: Exponential (1s, 2s, 4s...60s max) with restart counter reset on recovery

## Message Contracts

### Heartbeat Message Schema
```json
{
  "schema_version": "1.0",
  "worker_id": "camera_id", 
  "timestamp": "2025-08-30T12:00:00",
  "stream_state": "capturing|recording|error",
  "fps": 15.5,
  "recording": false,
  "error_message": null
}
```

### Command Message Schema
```json
{
  "schema_version": "1.0",
  "command": "start|stop|restart|ptz_move",
  "camera_id": "camera_id",
  "params": {"direction": "left", "duration": 1.0}
}
```

## Key Integration Points

### 1. **Existing Module Reuse**
- **camera_helper.py**: Used as-is for URL resolution and camera config loading
- **ffmpeg_recorder.py**: Integrated into MAIN stream thread with minimal wrapper
- **motion_detector.py**: Used directly in SUB stream thread for frame analysis
- **logger_config.py**: Centralized logging across all processes

### 2. **Module Modifications Required**
- **NONE** - All existing modules preserved
- **Integration Layer**: New supervisor.py and camera_worker.py wrap existing functionality
- **Benefits**: Zero regression risk, maintains existing test coverage

## Performance Characteristics

### Resource Usage (Per Worker)
- **Memory**: ~150-200MB (OpenCV + frame buffers + FFmpeg)
- **CPU**: 5-10% idle, 15-25% during recording
- **Network**: Dual stream bandwidth (SUB: ~1-2Mbps, MAIN: ~5-10Mbps)

### Scalability Targets
- **Current Phase**: 2-4 cameras tested
- **Phase 2 Target**: 10-15 cameras
- **Ultimate Goal**: 30 cameras (requires resource pooling optimizations)

### Latency Targets
- **Motion Detection**: < 100ms (frame-to-detection)
- **Recording Start**: < 2 seconds (motion-to-file-write)
- **Worker Restart**: < 30 seconds (crash-to-recovery)
- **Heartbeat Response**: < 1 second

## Fault Tolerance Design

### Worker Isolation
- Process crashes don't affect supervisor or other workers
- Individual camera failures don't impact system
- Resource leaks contained within worker process boundaries

### Restart Strategy
```
Attempt 1: 1 second delay
Attempt 2: 2 second delay  
Attempt 3: 4 second delay
Attempt 4: 8 second delay
...
Max delay: 60 seconds
Reset counter: On successful heartbeat recovery
```

### Error Recovery
- **Stream Disconnection**: Worker retries with camera_helper fallback logic
- **FFmpeg Crashes**: Recorder reinitializes with new process
- **Motion Detection Errors**: Worker continues with error logging
- **Supervisor Crashes**: Manual restart required (systemd/Windows Service recommended)

## Trade-offs & Assumptions

### Trade-offs
1. **Memory vs. Isolation**: Higher memory usage due to process separation, but better fault tolerance
2. **Complexity vs. Reliability**: More complex IPC, but deterministic failure recovery
3. **Resource Usage vs. Quality**: Dual streams increase bandwidth but enable optimized motion detection

### Key Assumptions
1. **Windows Deployment**: Optimized for Windows 10/11 with multiprocessing spawn method
2. **Local Processing**: All processing on single machine (not distributed)
3. **RTSP Cameras**: Primary target is IP cameras with RTSP streams
4. **Storage Capacity**: Sufficient disk space for continuous recording segments
5. **Network Stability**: Cameras accessible via reliable local network

### Limitations
1. **No Cross-Machine**: Current design is single-machine only
2. **No Database**: Status/metrics stored in memory only
3. **Manual Scaling**: No auto-scaling based on system resources
4. **Basic Health API**: No advanced metrics collection or alerting

## Production Deployment Notes

### Windows Service Setup
```batch
# Install as Windows Service using NSSM
nssm install NVRService "C:\path\to\python.exe" "C:\path\to\main.py"
nssm set NVRService Start SERVICE_AUTO_START
nssm start NVRService
```

### PyInstaller Packaging
```python
# nvr.spec for PyInstaller
a = Analysis(['main.py'], 
             pathex=['.'],
             binaries=[('ffmpeg.exe', '.')],  # Bundle FFmpeg
             datas=[('config/', 'config/')],   # Include config
             runtime_hooks=[])
```

### Resource Monitoring
- Use Task Manager or Resource Monitor to track per-worker CPU/memory
- Monitor disk I/O for recording performance
- Network monitoring for stream stability

## Acceptance Criteria Checklist

✅ **Worker Isolation**: Each camera runs in separate process  
✅ **Heartbeat Monitoring**: 5-second intervals with 15-second timeout  
✅ **Restart Logic**: Exponential backoff with max 60-second delay  
✅ **Health API**: JSON status endpoint for monitoring  
✅ **Existing Module Integration**: No modifications to tested modules  
✅ **Logging**: Structured JSON logs with correlation IDs  
✅ **Signal Handling**: Graceful shutdown on SIGINT/SIGTERM  
✅ **Configuration**: Uses existing cameras.json format  

### Next Phase Requirements
- [ ] Web UI integration with worker status
- [ ] Multi-camera preview grid (9+ cameras)
- [ ] Advanced recording policies (schedules, motion zones)
- [ ] Metrics persistence and trending
- [ ] PTZ command routing and conflict resolution