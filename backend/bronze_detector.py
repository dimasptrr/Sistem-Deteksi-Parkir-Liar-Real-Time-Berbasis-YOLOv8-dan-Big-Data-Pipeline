from __future__ import annotations

import logging
import os
import threading
import time
import warnings
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Tuple

import cv2

# Suppress supervision and other libraries deprecation/future warnings
warnings.filterwarnings("ignore", category=FutureWarning)
import numpy as np
import supervision as sv
from ultralytics import YOLO

try:
    import torch
except Exception:  # pragma: no cover - optional dependency guard
    torch = None

from backend.ffmpeg_capture import FFmpegPipeCapture
from backend.logger import ViolationLogger
from backend.zone_manager import ZoneManager


LOGGER = logging.getLogger(__name__)
VEHICLE_CLASS_IDS = {2: "mobil", 3: "motor", 5: "bus", 7: "truk"}
PROJECT_ROOT = Path(__file__).resolve().parents[1]



@dataclass
class VehicleState:
    track_id: int
    class_id: int
    class_name: str
    last_center: Tuple[float, float]
    last_seen_ts: float
    stationary_since: Optional[float] = None
    last_motion_ts: float = field(default_factory=time.time)
    zone_name: Optional[str] = None
    status: str = "moving"
    violation_logged: bool = False
    history: deque = field(default_factory=lambda: deque(maxlen=30))
    consecutive_moving_frames: int = 0



class KafkaCapture:
    def __init__(self, broker: str, topic: str, group_id: str) -> None:
        from confluent_kafka import Consumer
        conf = {
            'bootstrap.servers': broker,
            'group.id': group_id,
            'auto.offset.reset': 'latest',
            'enable.auto.commit': True
        }
        self.consumer = Consumer(conf)
        self.consumer.subscribe([topic])
        self.opened = True

    def isOpened(self) -> bool:
        return self.opened

    def read(self) -> Tuple[bool, Optional[np.ndarray]]:
        if not self.opened:
            return False, None
        
        # Poll pesan Kafka dengan timeout 1.0 detik
        msg = self.consumer.poll(1.0)
        if msg is None:
            # Timeout biasa, kembalikan True dan None agar pemanggil tahu loop masih berjalan
            return True, None
        
        if msg.error():
            from confluent_kafka import KafkaError
            if msg.error().code() == KafkaError._PARTITION_EOF:
                return True, None
            LOGGER.error(f"Kafka error: {msg.error()}")
            return False, None

        try:
            import json
            import base64
            payload = json.loads(msg.value().decode('utf-8'))
            
            # Decode data frame Base64 ke frame numpy array
            frame_data = base64.b64decode(payload['frame_data'])
            nparr = np.frombuffer(frame_data, np.uint8)
            frame = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
            
            return True, frame
        except Exception as e:
            LOGGER.error(f"Gagal mendecode pesan Kafka: {e}")
            return True, None

    def release(self) -> None:
        if self.opened:
            try:
                self.consumer.close()
            finally:
                self.opened = False


class ParkingDetector:
    def __init__(
        self,
        stream_url: str,
        model_name: str = "yolov8n.pt",
        confidence: float = 0.35,
        device: str | int | None = None,
        display_width: int = 960,
        stationary_speed_threshold: float = 12.0,
        stationary_grace_seconds: float = 5.0,
        violation_seconds: int = 15 * 60,
        reconnect_delay_seconds: float = 1.5,
        max_reconnect_delay_seconds: float = 8.0,
        track_timeout_seconds: float = 20.0,
        motion_grace_frames: int = 10,
        use_kafka: bool = False,
    ) -> None:
        self.stream_url = stream_url
        self.use_kafka = use_kafka
        self.model = YOLO(model_name)
        self.confidence = confidence
        if device is not None:
            self.device = device
        elif torch is not None and torch.cuda.is_available():
            self.device = "cuda:0"
        else:
            self.device = "cpu"
        self.use_half_precision = torch is not None and str(self.device).startswith("cuda")
        self.display_width = display_width
        self.stationary_speed_threshold = stationary_speed_threshold
        self.stationary_grace_seconds = stationary_grace_seconds
        self.violation_seconds = violation_seconds
        self.reconnect_delay_seconds = reconnect_delay_seconds
        self.max_reconnect_delay_seconds = max_reconnect_delay_seconds
        self.track_timeout_seconds = track_timeout_seconds
        self.motion_grace_frames = motion_grace_frames

        self.tracker = sv.ByteTrack()
        self.logger = ViolationLogger(
            csv_path=Path(__file__).resolve().with_name("violations_log.csv"),
            screenshot_dir=Path(__file__).resolve().with_name("violations"),
        )
        self.zone_manager = ZoneManager()

        # Bronze Layer setup
        self.bronze_path = PROJECT_ROOT / "data" / "bronze" / "raw_detections.json"
        self.bronze_path.parent.mkdir(parents=True, exist_ok=True)
        self.bronze_buffer = []
        self.bronze_lock = threading.Lock()


        self.capture: Optional[object] = None
        self.capture_backend: str = "opencv"
        self.running = False
        self.thread: Optional[threading.Thread] = None
        self.lock = threading.Lock()

        self.latest_frame_bgr: Optional[np.ndarray] = None
        self.latest_annotated_bgr: Optional[np.ndarray] = None
        self.latest_stats: Dict[str, int] = {
            "total_tracked": 0,
            "stationary_count": 0,
            "violations_today": 0,
            "moving_count": 0,
        }
        self.latest_alert: Optional[str] = None
        self.active_states: Dict[int, VehicleState] = {}
        self.all_seen_track_ids: set[int] = set()

        self._auto_zone_initialized = False
        self._current_frame_size = (1280, 720)

    def start(self) -> None:
        if self.running:
            return
        self.running = True
        self._open_capture()
        self.latest_raw_frame = None
        self.capture_thread = threading.Thread(target=self._capture_loop, daemon=True)
        self.capture_thread.start()
        self.thread = threading.Thread(target=self._loop, daemon=True)
        self.thread.start()

    def stop(self) -> None:
        self.running = False
        self._release_capture()
        with self.bronze_lock:
            self._flush_bronze_buffer()


    def _open_capture(self) -> None:
        self._release_capture()
        
        if self.use_kafka:
            try:
                from backend.kafka_config import KAFKA_BROKER, KAFKA_TOPIC, KAFKA_GROUP_ID
                self.capture = KafkaCapture(
                    broker=KAFKA_BROKER,
                    topic=KAFKA_TOPIC,
                    group_id=KAFKA_GROUP_ID
                )
                self.capture_backend = "kafka"
                LOGGER.info("Berhasil menginisialisasi Kafka capture backend.")
                return
            except Exception as e:
                LOGGER.exception(f"Gagal menginisialisasi Kafka capture backend: {e}")
                self.capture = None
                return

        os.environ.setdefault(
            "OPENCV_FFMPEG_CAPTURE_OPTIONS",
            "fflags;nobuffer|flags;low_delay|max_delay;0|probesize;32|analyzeduration;0",
        )
        
        # Check if the URL is an HLS stream (.m3u8). OpenCV on Windows has issues 
        # opening HLS fmp4 streams, causing uncatchable stderr warnings and delay.
        # So we directly bypass OpenCV for HLS streams and use FFmpeg.
        is_hls = ".m3u8" in self.stream_url.lower()
        
        if not is_hls:
            capture = cv2.VideoCapture(self.stream_url, cv2.CAP_FFMPEG)
            capture.set(cv2.CAP_PROP_BUFFERSIZE, 1)
            if capture.isOpened():
                self.capture = capture
                self.capture_backend = "opencv"
                return
            capture.release()
            LOGGER.warning("OpenCV gagal membuka stream, memakai FFmpeg pipe fallback.")

        try:
            self.capture = FFmpegPipeCapture(self.stream_url, max_width=self.display_width)
            self.capture_backend = "ffmpeg"
        except Exception:
            LOGGER.exception("Gagal membuka stream CCTV dengan OpenCV maupun FFmpeg pipe.")
            self.capture = None

    def _release_capture(self) -> None:
        if self.capture is not None:
            try:
                self.capture.release()
            finally:
                self.capture = None

    def _reconnect_capture(self) -> None:
        delay = self.reconnect_delay_seconds
        while self.running:
            LOGGER.warning("Mencoba reconnect stream HLS...")
            self._open_capture()
            if self.capture is not None and self.capture.isOpened():
                return
            time.sleep(delay)
            delay = min(delay * 1.5, self.max_reconnect_delay_seconds)

    def _read_frame(self) -> Optional[np.ndarray]:
        if self.capture is None or not self.capture.isOpened():
            self._reconnect_capture()
        if self.capture is None:
            return None

        ok, frame = self.capture.read()
        if not ok or frame is None:
            self._reconnect_capture()
            if self.capture is None:
                return None
            ok, frame = self.capture.read()
            if not ok or frame is None:
                return None

        latest_frame = frame
        for _ in range(2):
            ok, buffered_frame = self.capture.read()
            if not ok or buffered_frame is None:
                break
            latest_frame = buffered_frame

        return latest_frame

    def _resize_for_cpu(self, frame: np.ndarray) -> np.ndarray:
        height, width = frame.shape[:2]
        if width <= self.display_width:
            return frame
        ratio = self.display_width / float(width)
        new_height = int(height * ratio)
        return cv2.resize(frame, (self.display_width, new_height), interpolation=cv2.INTER_AREA)

    def _track_detections(self, detections: sv.Detections) -> sv.Detections:
        if detections is None or len(detections) == 0:
            return detections

        if hasattr(self.tracker, "update_with_detections"):
            return self.tracker.update_with_detections(detections)
        if hasattr(self.tracker, "update"):
            return self.tracker.update(detections)
        return detections

    @staticmethod
    def _center_from_xyxy(xyxy: np.ndarray) -> Tuple[float, float]:
        x1, y1, x2, y2 = xyxy
        return float((x1 + x2) / 2.0), float((y1 + y2) / 2.0)

    def _vehicle_name(self, class_id: int) -> str:
        return VEHICLE_CLASS_IDS.get(int(class_id), f"kelas_{class_id}")

    def _ensure_default_zones(self, frame_width: int, frame_height: int) -> None:
        if self.zone_manager.is_custom():
            return
        if not self._auto_zone_initialized or self._current_frame_size != (frame_width, frame_height):
            self.zone_manager.reset(frame_width, frame_height)
            self._auto_zone_initialized = True
            self._current_frame_size = (frame_width, frame_height)

    def update_zones_from_json(self, json_text: str) -> None:
        self.zone_manager.load_from_json(json_text)

    def reset_zones(self) -> None:
        width, height = self._current_frame_size
        self.zone_manager.reset(width, height)
        self._auto_zone_initialized = True

    def _flush_bronze_buffer(self) -> None:
        if not self.bronze_buffer:
            return
        try:
            import json
            with open(self.bronze_path, "a", encoding="utf-8") as f:
                for record in self.bronze_buffer:
                    f.write(json.dumps(record) + "\n")
            self.bronze_buffer.clear()
        except Exception as e:
            LOGGER.error(f"Gagal menulis ke Bronze Layer: {e}")


    def _update_vehicle_state(
        self,
        *,
        track_id: int,
        class_id: int,
        center: Tuple[float, float],
        now: float,
        zone_name: Optional[str],
    ) -> VehicleState:
        state = self.active_states.get(track_id)
        if state is None:
            state = VehicleState(
                track_id=track_id,
                class_id=class_id,
                class_name=self._vehicle_name(class_id),
                last_center=center,
                last_seen_ts=now,
                last_motion_ts=now,
                zone_name=zone_name,
            )
            self.active_states[track_id] = state
        else:
            # 1. Stable speed calculation using a historical reference point (closest to 0.5s ago)
            speed = 0.0
            if len(state.history) > 0:
                target_ts = now - 0.5
                best_diff = float("inf")
                past_time = None
                past_center = None
                for h_time, h_center in state.history:
                    diff = abs(h_time - target_ts)
                    if diff < best_diff:
                        best_diff = diff
                        past_time = h_time
                        past_center = h_center

                if past_time is not None:
                    elapsed = now - past_time
                    if elapsed > 0.1:
                        distance = float(np.linalg.norm(np.array(center) - np.array(past_center)))
                        speed = distance / elapsed
                    else:
                        distance = float(np.linalg.norm(np.array(center) - np.array(state.last_center)))
                        elapsed = max(now - state.last_seen_ts, 1e-6)
                        speed = distance / elapsed
            else:
                distance = float(np.linalg.norm(np.array(center) - np.array(state.last_center)))
                elapsed = max(now - state.last_seen_ts, 1e-6)
                speed = distance / elapsed

            # 2. Update state with consecutive frame stabilization
            is_moving_or_outside = (speed > self.stationary_speed_threshold) or (zone_name is None)

            if state.stationary_since is not None:
                if is_moving_or_outside:
                    state.consecutive_moving_frames += 1
                    if state.consecutive_moving_frames >= self.motion_grace_frames:
                        state.last_motion_ts = now
                        state.stationary_since = None
                        state.violation_logged = False
                        state.zone_name = zone_name
                else:
                    state.consecutive_moving_frames = 0
                    state.zone_name = zone_name
            else:
                state.consecutive_moving_frames = 0
                state.zone_name = zone_name
                if not is_moving_or_outside:
                    state.stationary_since = now

            state.last_center = center
            state.last_seen_ts = now
            state.class_id = class_id
            state.class_name = self._vehicle_name(class_id)

        state.history.append((now, center))
        return state

    def _state_status(self, state: VehicleState, now: float) -> str:
        stationary_since = state.stationary_since
        if stationary_since is None:
            state.status = "green"
            state.violation_logged = False
            return state.status

        stationary_duration = now - stationary_since
        if stationary_duration >= self.stationary_grace_seconds:
            if stationary_duration >= self.violation_seconds:
                state.status = "red"
            else:
                state.status = "yellow"
            return state.status

        state.status = "green"
        return state.status

    def _cleanup_states(self, now: float) -> None:
        stale_ids = [track_id for track_id, state in self.active_states.items() if now - state.last_seen_ts > self.track_timeout_seconds]
        for track_id in stale_ids:
            self.active_states.pop(track_id, None)
            self.all_seen_track_ids.discard(track_id)

    def _draw_detection(self, frame: np.ndarray, box: np.ndarray, label: str, color: Tuple[int, int, int]) -> None:
        x1, y1, x2, y2 = [int(v) for v in box]
        cv2.rectangle(frame, (x1, y1), (x2, y2), color, 2)
        text_size, _ = cv2.getTextSize(label, cv2.FONT_HERSHEY_SIMPLEX, 0.55, 2)
        text_width, text_height = text_size
        label_y = max(20, y1 - 10)
        cv2.rectangle(frame, (x1, label_y - text_height - 8), (x1 + text_width + 8, label_y + 4), color, -1)
        cv2.putText(frame, label, (x1 + 4, label_y - 4), cv2.FONT_HERSHEY_SIMPLEX, 0.55, (0, 0, 0), 2, cv2.LINE_AA)

    def process_frame(self, frame_bgr: np.ndarray) -> np.ndarray:
        resized = self._resize_for_cpu(frame_bgr)
        self._ensure_default_zones(resized.shape[1], resized.shape[0])

        annotated = self.zone_manager.draw_overlay(resized.copy())
        results = self.model.predict(
            resized,
            conf=self.confidence,
            verbose=False,
            classes=list(VEHICLE_CLASS_IDS.keys()),
            device=self.device,
            imgsz=self.display_width,
            half=self.use_half_precision,
        )

        now = time.time()
        if not results:
            with self.lock:
                self.latest_frame_bgr = resized
                self.latest_annotated_bgr = annotated
                self.latest_stats = {
                    "total_tracked": len(self.all_seen_track_ids),
                    "stationary_count": 0,
                    "violations_today": self.logger.today_count(),
                    "moving_count": 0,
                }
                self.latest_alert = None
            return annotated

        detections = sv.Detections.from_ultralytics(results[0])
        detections = self._track_detections(detections)

        # Record Bronze Layer data (frame metadata + raw detections)
        record = {
            "timestamp": datetime.now().isoformat(),
            "stream_url": self.stream_url,
            "frame_width": resized.shape[1],
            "frame_height": resized.shape[0],
            "detections": []
        }
        
        if len(detections) > 0 and detections.xyxy is not None:
            tracker_ids = detections.tracker_id if detections.tracker_id is not None else np.arange(len(detections))
            for index in range(len(detections)):
                tracker_id = int(tracker_ids[index])
                class_id = int(detections.class_id[index]) if detections.class_id is not None else 0
                box = [float(v) for v in detections.xyxy[index]]
                center = self._center_from_xyxy(detections.xyxy[index])
                zone_name = self.zone_manager.point_to_zone(center)
                
                record["detections"].append({
                    "track_id": tracker_id,
                    "class_id": class_id,
                    "class_name": self._vehicle_name(class_id),
                    "bbox": box,
                    "confidence": float(detections.confidence[index]) if detections.confidence is not None else 1.0,
                    "center": [float(center[0]), float(center[1])],
                    "zone_name": zone_name
                })
                
        with self.bronze_lock:
            self.bronze_buffer.append(record)
            if len(self.bronze_buffer) >= 50:
                self._flush_bronze_buffer()


        alert_found = False
        stationary_count = 0
        active_count = 0
        moving_count = 0

        if len(detections) > 0 and detections.xyxy is not None:
            tracker_ids = detections.tracker_id if detections.tracker_id is not None else np.arange(len(detections))
            for index in range(len(detections)):
                tracker_id = int(tracker_ids[index])
                class_id = int(detections.class_id[index]) if detections.class_id is not None else 0
                box = detections.xyxy[index]
                center = self._center_from_xyxy(box)
                zone_name = self.zone_manager.point_to_zone(center)

                state = self._update_vehicle_state(
                    track_id=tracker_id,
                    class_id=class_id,
                    center=center,
                    now=now,
                    zone_name=zone_name,
                )
                status = self._state_status(state, now)
                self.all_seen_track_ids.add(tracker_id)
                active_count += 1

                if status == "red":
                    alert_found = True
                    stationary_count += 1
                    if not state.violation_logged:
                        entered_at = datetime.fromtimestamp(state.stationary_since or now)
                        screenshot_timestamp = datetime.now()
                        screenshot_path = self.logger.screenshot_dir / f"violation_{entered_at.strftime('%Y%m%d_%H%M%S')}_id{tracker_id}.jpg"
                        cv2.imwrite(str(screenshot_path), resized)
                        self.logger.log_violation(
                            timestamp_entry=entered_at,
                            timestamp_violation=screenshot_timestamp,
                            duration_seconds=now - (state.stationary_since or now),
                            vehicle_type=state.class_name,
                            track_id=tracker_id,
                            zone_name=zone_name or "unknown",
                            screenshot_path=str(screenshot_path),
                        )
                        state.violation_logged = True

                elif status == "yellow":
                    stationary_count += 1
                else:
                    moving_count += 1
                    state.violation_logged = False

                if status == "red":
                    color = (0, 0, 255)
                elif status == "yellow":
                    color = (0, 255, 255)
                else:
                    color = (0, 200, 0)

                duration_seconds = 0.0
                if state.stationary_since is not None:
                    duration_seconds = now - state.stationary_since
                label = f"ID {tracker_id} | {state.class_name} | {status.upper()} | {duration_seconds/60:.1f}m"
                self._draw_detection(annotated, box, label, color)

        self._cleanup_states(now)

        stats = {
            "total_tracked": len(self.all_seen_track_ids),
            "stationary_count": stationary_count,
            "violations_today": self.logger.today_count(),
            "moving_count": moving_count,
        }
        with self.lock:
            self.latest_frame_bgr = resized
            self.latest_annotated_bgr = annotated
            self.latest_stats = stats
            self.latest_alert = "PARKIR LIAR" if alert_found else None
        return annotated

    def _capture_loop(self) -> None:
        reconnect_sleep = self.reconnect_delay_seconds
        while self.running:
            if self.capture is None or not self.capture.isOpened():
                self._reconnect_capture()
            if self.capture is None:
                time.sleep(reconnect_sleep)
                reconnect_sleep = min(reconnect_sleep * 1.5, self.max_reconnect_delay_seconds)
                continue

            reconnect_sleep = self.reconnect_delay_seconds
            start_time = time.time()
            try:
                ok, frame = self.capture.read()
                if ok and frame is not None:
                    with self.lock:
                        self.latest_raw_frame = frame
                else:
                    LOGGER.warning("Gagal membaca frame dari capture, memicu reconnect.")
                    self._release_capture()
            except Exception as e:
                LOGGER.error(f"Error pada loop capture: {e}")
                self._release_capture()
                time.sleep(0.5)
            
            # Throttle the capture rate to match natural speed (~25 FPS / 40ms per frame)
            elapsed = time.time() - start_time
            sleep_time = max(0, 0.04 - elapsed)
            if sleep_time > 0:
                time.sleep(sleep_time)

    def _loop(self) -> None:
        while self.running:
            with self.lock:
                frame = self.latest_raw_frame
                self.latest_raw_frame = None

            if frame is None:
                time.sleep(0.01)
                continue

            try:
                self.process_frame(frame)
            except Exception:
                LOGGER.exception("Gagal memproses frame CCTV.")
                time.sleep(0.1)

    def get_snapshot(self) -> Dict[str, object]:
        with self.lock:
            frame_bgr = None if self.latest_annotated_bgr is None else self.latest_annotated_bgr.copy()
            stats = dict(self.latest_stats)
            alert = self.latest_alert

        logs = self.logger.load_recent(limit=20)
        return {
            "frame_bgr": frame_bgr,
            "stats": stats,
            "alert": alert,
            "logs": logs,
            "zone_json": self.zone_manager.to_json(),
            "custom_zones": self.zone_manager.is_custom(),
        }

    def get_latest_frame(self) -> Optional[np.ndarray]:
        with self.lock:
            return None if self.latest_annotated_bgr is None else self.latest_annotated_bgr.copy()
