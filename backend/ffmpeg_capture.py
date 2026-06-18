from __future__ import annotations

import logging
import re
import subprocess
from typing import Optional, Tuple

import numpy as np

try:
    import imageio_ffmpeg
except Exception:  # pragma: no cover - optional dependency guard
    imageio_ffmpeg = None


LOGGER = logging.getLogger(__name__)
VIDEO_SIZE_PATTERN = re.compile(r"(\d{2,5})x(\d{2,5})")


class FFmpegPipeCapture:
    def __init__(self, stream_url: str, max_width: Optional[int] = None) -> None:
        self.stream_url = stream_url
        self.max_width = max_width
        self.ffmpeg_path = self._resolve_ffmpeg_path()
        self.source_width, self.source_height = self._probe_video_size()
        self.output_width, self.output_height = self._compute_output_size()
        self.process: Optional[subprocess.Popen] = None
        self.stdout = None
        self.stderr = None
        self._start_process()

    def _resolve_ffmpeg_path(self) -> str:
        if imageio_ffmpeg is None:
            return "ffmpeg"
        return imageio_ffmpeg.get_ffmpeg_exe()

    def _probe_video_size(self) -> Tuple[int, int]:
        probe_command = [
            self.ffmpeg_path,
            "-hide_banner",
            "-loglevel",
            "info",
            "-allowed_extensions",
            "ALL",
            "-i",
            self.stream_url,
            "-frames:v",
            "1",
            "-f",
            "null",
            "-",
        ]
        process = subprocess.Popen(
            probe_command,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        detected_size: Optional[Tuple[int, int]] = None
        assert process.stderr is not None
        try:
            for line in process.stderr:
                match = VIDEO_SIZE_PATTERN.search(line)
                if match:
                    detected_size = (int(match.group(1)), int(match.group(2)))
                    break
        finally:
            process.terminate()
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()

        if detected_size is None:
            raise RuntimeError(f"Tidak bisa membaca ukuran stream dari {self.stream_url}")

        return detected_size

    def _compute_output_size(self) -> Tuple[int, int]:
        if self.max_width is None or self.source_width <= self.max_width:
            return self.source_width, self.source_height

        target_width = int(self.max_width)
        target_height = max(2, int(round(self.source_height * target_width / float(self.source_width))))
        if target_height % 2 != 0:
            target_height += 1
        return target_width, target_height

    def _start_process(self) -> None:
        if self.isOpened():
            return

        decode_command = [
            self.ffmpeg_path,
            "-hide_banner",
            "-loglevel",
            "error",
            "-fflags",
            "nobuffer",
            "-flags",
            "low_delay",
            "-allowed_extensions",
            "ALL",
            "-i",
            self.stream_url,
            "-an",
            "-sn",
            "-dn",
        ]
        if (self.output_width, self.output_height) != (self.source_width, self.source_height):
            decode_command.extend([
                "-vf",
                f"scale={self.output_width}:{self.output_height}",
            ])
        decode_command.extend([
            "-f",
            "rawvideo",
            "-pix_fmt",
            "bgr24",
            "pipe:1",
        ])

        self.process = subprocess.Popen(
            decode_command,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
        self.stdout = self.process.stdout
        self.stderr = self.process.stderr

    def isOpened(self) -> bool:
        return self.process is not None and self.process.poll() is None and self.stdout is not None

    def read(self) -> Tuple[bool, Optional[np.ndarray]]:
        if not self.isOpened():
            return False, None

        frame_size = self.output_width * self.output_height * 3
        assert self.stdout is not None
        raw_frame = self.stdout.read(frame_size)
        if raw_frame is None or len(raw_frame) != frame_size:
            return False, None

        frame = np.frombuffer(raw_frame, dtype=np.uint8).reshape((self.output_height, self.output_width, 3))
        return True, frame.copy()

    def release(self) -> None:
        if self.process is None:
            return

        try:
            self.process.terminate()
            try:
                self.process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self.process.kill()
        finally:
            if self.stdout is not None:
                self.stdout.close()
                self.stdout = None
            if self.stderr is not None:
                self.stderr.close()
                self.stderr = None
            self.process = None
