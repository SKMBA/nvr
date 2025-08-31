# core\ffmpeg_recorder.py
import subprocess 
import threading 
import collections 
import queue 
import cv2 
from config .ui_constants import FFMPEG_LOG_FILE 
from core.logger_config import logger
class FFmpegRecorder :
    def __init__ (self ,url ,output_file ,pre_record_time ,fps ,frame_size ):
        self .url =url 
        self .output_file =output_file 
        self .pre_record_time =pre_record_time 
        self .fps =fps 
        self .frame_size =frame_size 
        self .frame_queue =collections .deque (maxlen =self .pre_record_time *self .fps )
        self .write_queue =queue .Queue (maxsize =1000 )
        self .process =None 
        self .recording =False 
        self .lock =threading .Lock ()
        self .writer_thread =None 
        self .stop_writer_event =threading .Event ()
        self .dropped_frames =0 
        print (f"[FFmpegRecorder] Initialized with frame_size={self .frame_size }")
    def add_frame (self ,frame ):
        resized =cv2 .resize (frame ,self .frame_size )
        with self .lock :
            self .frame_queue .append (resized )
            if self .recording :
                try :
                    self .write_queue .put_nowait (resized )
                except queue .Full :
                    self .dropped_frames +=1 
                    print (f"[FFmpegRecorder] Write queue full! Dropping frame #{self .dropped_frames }.")
    def start_recording (self ):
        with self .lock :
            if self .recording :
                return 
            print (f"[Recorder] Starting recording @{self .fps } ...")
            # cmd =[
            # "ffmpeg",
            # "-y",
            # "-f","rawvideo",
            # "-pix_fmt","bgr24",
            # "-s",f"{self .frame_size [0 ]}x{self .frame_size [1 ]}",
            # "-r",str (self .fps ),
            # "-i","-",
            # "-an",
            # "-c:v","libx264",
            # "-preset","ultrafast",
            # "-pix_fmt","yuv420p",
            # self .output_file 
            # ]
            # cmd = [
            #     "ffmpeg",
            #     "-rtsp_transport", "tcp",
            #     "-i", self.url,
            #     "-an",
            #     "-vf",
            #     (
            #         # (optional) scale first if you want a specific size:
            #         # "scale=1280:720," 
            #         "drawtext=text='%{localtime}':x=10:y=10:fontsize=24:fontcolor=white:box=1:boxcolor=black@0.5,"
            #         f"drawtext=text='{self.camera_name}':x=(w-tw-10):y=(h-th-10):fontsize=24:fontcolor=yellow:box=1:boxcolor=black@0.5"
            #     ),
            #     # (optional) cap output fps cleanly:
            #     # "-vf", "fps=15,scale=1280:720,drawtext=...,drawtext=..."
            #     "-c:v", "libx264",
            #     "-preset", "ultrafast",
            #     "-pix_fmt", "yuv420p",
            #     self.output_file,
            # ]

            cmd = [
                "ffmpeg",
                "-rtsp_transport", "tcp",
                "-i", self.url,
                "-an",
                # "-vf",
                # (
                #     # (optional) scale first if you want a specific size:
                #     # "scale=1280:720," 
                #     "drawtext=text='%{localtime}':x=10:y=10:fontsize=24:fontcolor=white:box=1:boxcolor=black@0.5,"
                #     f"drawtext=text='{self.camera_name}':x=(w-tw-10):y=(h-th-10):fontsize=24:fontcolor=yellow:box=1:boxcolor=black@0.5"
                # ),
                # (optional) cap output fps cleanly:
                # "-vf", "fps=15,scale=1280:720,drawtext=...,drawtext=..."
                "-c:v", "libx264",
                "-preset", "ultrafast",
                "-pix_fmt", "yuv420p",
                self.output_file,
            ]



            # cmd = [
            #     "ffmpeg",
            #     "-rtsp_transport", "tcp",
            #     "-i", self.url,
            #     "-an",
            #     "-c:v", "copy",
            #     self.output_file,  # e.g., "out.mkv" or "out.mp4"
            # ]




            


            print(cmd)
            from core.logger_config import logger

            import shlex
            # logger.log(cmd)
            print (shlex.join(cmd))
            logger.debug (f"Using shlex:{ shlex.join(cmd)}")
            logger.debug (f"Using join:{ " ".join(cmd)}")

            self .process =subprocess .Popen (
            cmd ,
            stdin =subprocess .PIPE ,
            stdout =open (FFMPEG_LOG_FILE ,"w"),
            stderr =subprocess .STDOUT ,
            bufsize =10 **8 
            )
            for frame in self .frame_queue :
                try :
                    self .write_queue .put_nowait (frame )
                except queue .Full :
                    print ("[FFmpegRecorder] Write queue full during prebuffer.")
            self .recording =True 
            self .stop_writer_event .clear ()
            self .writer_thread =threading .Thread (target =self ._writer_loop ,daemon =True )
            self .writer_thread .start ()
    def _writer_loop (self ):
        while not self .stop_writer_event .is_set ():
            try :
                frame =self .write_queue .get (timeout =0.1 )
                self .process .stdin .write (frame .tobytes ())
            except queue .Empty :
                continue 
            except Exception as e :
                print (f"[FFmpegRecorder] Writer error: {e }")
                break 

    # ffmpeg_recorder.py - Updated stop_recording() method only
    def stop_recording(self):
        """Stop FFmpeg recording with graceful shutdown."""
        print("SKM-core-ffmpeg_recorder.py-FFmpegRecorder.stop_recording()")
        with self.lock:
            if not self.recording:
                return
                
            logger.debug("[Recorder] Stopping recording...")
            logger.debug(f"[Recorder] Stopping recording... Total dropped frames: {self.dropped_frames}")
            
            # Mark as not recording first
            self.recording = False
            
            # Stop writer thread
            self.stop_writer_event.set()
            if self.writer_thread:
                self.writer_thread.join(timeout=2)
                self.writer_thread = None
                
            # Graceful FFmpeg shutdown
            try:
                if self.process and self.process.poll() is None:  # Process still running
                    logger.debug("[Recorder] Sending 'q' command to FFmpeg for graceful shutdown...")
                    
                    try:
                        # Send 'q' command for graceful shutdown
                        self.process.stdin.write(b'q\n')
                        self.process.stdin.flush()
                        logger.debug("[Recorder] 'q' command sent, waiting for FFmpeg to finish...")
                        
                        # Wait for graceful shutdown (up to 10 seconds)
                        exit_code = self.process.wait(timeout=10)
                        logger.info(f"[Recorder] FFmpeg finished gracefully with exit code: {exit_code}")
                        
                    except subprocess.TimeoutExpired:
                        logger.warning("[Recorder] Graceful shutdown timeout, terminating FFmpeg...")
                        self.process.terminate()
                        
                        try:
                            # Wait for termination
                            exit_code = self.process.wait(timeout=5)
                            logger.debug(f"[Recorder] FFmpeg terminated with exit code: {exit_code}")
                        except subprocess.TimeoutExpired:
                            logger.debug("[Recorder] Termination timeout, force killing FFmpeg...")
                            self.process.kill()
                            self.process.wait()  # This should not timeout after kill
                            logger.error("[Recorder] FFmpeg force killed")
                            
                    except BrokenPipeError:
                        # FFmpeg already closed - this is normal
                        logger.error("[Recorder] FFmpeg already closed (broken pipe)")
                        self.process.wait(timeout=5)
                        
                    except Exception as e:
                        logger.error(f"[Recorder] Error during graceful shutdown: {e}")
                        # Fallback to termination
                        try:
                            self.process.terminate()
                            self.process.wait(timeout=5)
                        except:
                            self.process.kill()
                            self.process.wait()
                            
                    finally:
                        # Always close stdin to prevent resource leaks
                        try:
                            if self.process.stdin:
                                self.process.stdin.close()
                        except:
                            pass
                            
            except Exception as e:
                logger.critical(f"[Recorder] Error stopping FFmpeg: {e}")
                
            finally:
                self.process = None
                logger.info("[Recorder] Recording stopped successfully")

def get_dropped_frames (self ):
    return self .dropped_frames 
def reset_dropped_frames (self ):
    self .dropped_frames =0 
def get_dropped_frame_count (self ):
    return self .dropped_frames 
