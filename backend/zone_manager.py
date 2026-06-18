from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import cv2
import numpy as np


Point = Tuple[int, int]
Polygon = List[Point]


@dataclass
class ZoneDefinition:
    name: str
    points: Polygon


class ZoneManager:
    @classmethod
    def default(cls, frame_width: int = 1280, frame_height: int = 720) -> "ZoneManager":
        return cls(frame_width=frame_width, frame_height=frame_height)

    def __init__(self, frame_width: int = 1280, frame_height: int = 720) -> None:
        self.frame_width = frame_width
        self.frame_height = frame_height
        self._zones: Dict[str, Polygon] = self.default_zones(frame_width, frame_height)
        self._is_custom = False

    @staticmethod
    def default_zones(frame_width: int, frame_height: int) -> Dict[str, Polygon]:
        shoulder_width = max(120, int(frame_width * 0.16))
        top_margin = int(frame_height * 0.15)
        bottom_margin = int(frame_height * 0.96)
        mid_gap = int(frame_width * 0.05)

        left_zone: Polygon = [
            (0, top_margin),
            (shoulder_width, top_margin + int(frame_height * 0.04)),
            (shoulder_width + mid_gap // 2, bottom_margin - int(frame_height * 0.03)),
            (0, bottom_margin),
        ]
        right_zone: Polygon = [
            (frame_width - shoulder_width - mid_gap // 2, top_margin + int(frame_height * 0.04)),
            (frame_width, top_margin),
            (frame_width, bottom_margin),
            (frame_width - shoulder_width, bottom_margin - int(frame_height * 0.03)),
        ]
        return {"left": left_zone, "right": right_zone}

    def reset(self, frame_width: Optional[int] = None, frame_height: Optional[int] = None) -> None:
        if frame_width is not None:
            self.frame_width = frame_width
        if frame_height is not None:
            self.frame_height = frame_height
        self._zones = self.default_zones(self.frame_width, self.frame_height)
        self._is_custom = False

    def set_zones(self, zones: Dict[str, Polygon]) -> None:
        self._zones = {name: [(int(x), int(y)) for x, y in points] for name, points in zones.items()}
        self._is_custom = True

    def get_zones(self) -> Dict[str, Polygon]:
        return self._zones

    def is_custom(self) -> bool:
        return self._is_custom

    def to_json(self) -> str:
        serializable = {name: [[int(x), int(y)] for x, y in points] for name, points in self._zones.items()}
        return json.dumps(serializable, indent=2, ensure_ascii=False)

    def load_from_json(self, text: str) -> None:
        payload = json.loads(text)
        if not isinstance(payload, dict):
            raise ValueError("Format JSON zona harus berupa object dengan key left dan right.")

        zones: Dict[str, Polygon] = {}
        for name, points in payload.items():
            if not isinstance(points, list) or len(points) < 3:
                raise ValueError(f"Zona {name} harus berisi minimal 3 titik.")
            parsed_points: Polygon = []
            for point in points:
                if not isinstance(point, (list, tuple)) or len(point) != 2:
                    raise ValueError(f"Titik pada zona {name} harus berisi dua angka.")
                parsed_points.append((int(point[0]), int(point[1])))
            zones[name] = parsed_points

        self.set_zones(zones)

    def point_to_zone(self, point: Tuple[float, float]) -> Optional[str]:
        x, y = point
        for zone_name, polygon in self._zones.items():
            polygon_array = np.array(polygon, dtype=np.int32)
            result = cv2.pointPolygonTest(polygon_array, (float(x), float(y)), False)
            if result >= 0:
                return zone_name
        return None

    def draw_overlay(self, frame: np.ndarray) -> np.ndarray:
        overlay = frame.copy()
        for zone_name, polygon in self._zones.items():
            polygon_array = np.array(polygon, dtype=np.int32)
            color = (0, 0, 255) if zone_name == "left" else (255, 0, 0)
            cv2.fillPoly(overlay, [polygon_array], color)
            cv2.polylines(overlay, [polygon_array], True, (255, 255, 255), 2)
            label_point = polygon[0]
            cv2.putText(
                overlay,
                f"Zona {zone_name}",
                (label_point[0] + 8, max(20, label_point[1] + 20)),
                cv2.FONT_HERSHEY_SIMPLEX,
                0.6,
                (255, 255, 255),
                2,
                cv2.LINE_AA,
            )
        return cv2.addWeighted(overlay, 0.18, frame, 0.82, 0)
