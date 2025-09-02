# Bug Fixes Required

Let me analyze your code and identify the specific bugs that need fixing:

✅ → completed
❌ → not done / pending
🟡 → in progress/pending
🔹 → info / minor task
⏳ → hourglass, indicates waiting/testing pending
⚠️ → warning, attention required

## 🟡❌ 1. FFmpeg Integration Bugs

**✅ Issue 1.1: No Graceful Shutdown**  - [x] Issue completed
- **Location:** `ffmpeg_recorder.py` `stop_recording()` method  
- **Problem:** Uses `stdin.close()` and `wait()` but doesn't send FFmpeg 'q' command  
- **Impact:** FFmpeg may not close files properly, leading to corrupted recordings  

**✅ Issue 1.2: Missing Reconnection Parameters**  
- **Location:** `ffmpeg_recorder.py` `start_recording()` FFmpeg command  
- **Problem:** No RTSP reconnection flags for network interruptions  
- **Impact:** Recording stops permanently on brief network issues  

**❌ Issue 1.3: No Error Recovery for Stream Drops**  
- **Location:** `ffmpeg_recorder.py` - no monitoring of FFmpeg process health  
- **Problem:** If FFmpeg crashes, recorder doesn't detect or restart  
- **Impact:** Silent recording failures  

---

## ⏳2. Worker Thread Synchronization Bugs

**✅⏳⚠️🟡Issue 2.1: Race Condition in Recording State**  
- **Location:** `camera_worker.py` - recording state shared between threads  
- **Problem:** `self.recording` accessed without consistent locking  
- **Impact:** Recording may not start/stop properly  

**✅⏳Issue 2.2: Event Coordination Issues**  
- **Location:** `camera_worker.py` `_main_stream_loop()`  
- **Problem:** `recording_event` and `stop_recording_event` coordination  
- **Impact:** Recording threads may miss triggers or hang  

---

## 🟡3. Error Handling Gaps

**✅⏳Issue 3.1: Silent OpenCV Failures**  
- **Location:** `camera_worker.py` `_sub_stream_loop()`  
- **Problem:** `cap.read()` failures only log warning, no recovery  
- **Impact:** Workers become unresponsive without supervisor knowing  

**🟡Issue 3.2: Queue Exception Handling**  
- **Location:** `camera_worker.py` heartbeat and command loops  
- **Problem:** Queue operations can raise exceptions that aren't handled  
- **Impact:** Worker threads may crash silently  

**Issue 3.3: Missing Process Health Monitoring**  
- **Location:** `camera_worker.py` - no monitoring of FFmpeg subprocess  
- **Problem:** If FFmpeg dies, worker doesn't know  
- **Impact:** Recording failures not detected  

---

## 4. Resource Management Bugs

**Issue 4.1: OpenCV VideoCapture Not Released on Error**  
- **Location:** `camera_worker.py` `_sub_stream_loop()`  
- **Problem:** `cap.release()` only in `finally`, not on reconnection  
- **Impact:** Resource leaks on stream errors  

**Issue 4.2: Memory Leak in Frame Queue**  
- **Location:** `ffmpeg_recorder.py` - frame queue grows unbounded during errors  
- **Problem:** If `write_queue` blocks, memory usage grows  
- **Impact:** Worker memory exhaustion  

---

## 5. Motion Detection State Bugs

**Issue 5.1: Complex State Management**  
- **Location:** `motion_detector.py` - multiple overlapping state variables  
- **Problem:** Debug prints in production, complex state transitions  
- **Impact:** Difficult to debug motion detection issues  

**Issue 5.2: Timer Precision Issues**  
- **Location:** Motion timeout logic uses floating point comparisons  
- **Problem:** Potential timing edge cases  
- **Impact:** Inconsistent motion confirmation  

---

## Priority Bug Fix List

**High Priority (System Stability):**  
1. FFmpeg graceful shutdown - prevents corrupted recordings  
2. OpenCV error recovery - prevents worker hangs  
3. Recording state synchronization - prevents recording failures  

**Medium Priority (Reliability):**  
4. FFmpeg process monitoring - detects recording failures  
5. Queue exception handling - prevents silent thread crashes  
6. Resource cleanup on errors - prevents memory leaks  

**Low Priority (Polish):**  
7. Motion detection state cleanup - removes debug code  
8. RTSP reconnection parameters - improves network resilience
