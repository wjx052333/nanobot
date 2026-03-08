"""
DEIMv2 detection database query tools for nanobot.

Provides interactive querying of video detection results stored in SQLite.
DB path defaults to DEIMV2_DB_PATH env var or the project default.
"""

import base64
import json
import os
import sqlite3
from typing import Any

from .base import Tool

_DEFAULT_DB = os.path.join(
    os.path.dirname(__file__),  # .../nanobot/nanobot/agent/tools/
    "..", "..", "..", "..",     # → dino_DEIMv2/
    "output", "detections.db"
)
_DEFAULT_IMG_ROOT = os.path.join(
    os.path.dirname(__file__),
    "..", "..", "..", "..",
    "output", "video_detections"
)


def _db_path() -> str:
    return os.environ.get("DEIMV2_DB_PATH", os.path.normpath(_DEFAULT_DB))


def _img_root() -> str:
    return os.environ.get("DEIMV2_IMG_ROOT", os.path.normpath(_DEFAULT_IMG_ROOT))


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(_db_path(), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


# ── Tool 1: 查询轨迹列表 ──────────────────────────────────────────────────────

class DeimQueryTracksTool(Tool):
    """Query video detection tracks with optional filters."""

    @property
    def name(self) -> str:
        return "deimv2_query_tracks"

    @property
    def description(self) -> str:
        return (
            "查询 DEIMv2 视频检测轨迹。支持按视频名、目标类别、置信度过滤，"
            "返回轨迹列表（track_id、视频名、类别、时长、置信度、图像路径）。"
        )

    @property
    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "video_name": {
                    "type": "string",
                    "description": "视频名称过滤，不填则查所有视频",
                },
                "class_name": {
                    "type": "string",
                    "description": "目标类别过滤，如 person、car",
                },
                "min_confidence": {
                    "type": "number",
                    "description": "最低平均置信度（0-1），不填则不限",
                },
                "limit": {
                    "type": "integer",
                    "description": "最多返回条数，默认 20，最大 100",
                },
                "sort_by": {
                    "type": "string",
                    "description": "排序字段：avg_confidence（默认）、duration_sec、track_id",
                },
                "sort_order": {
                    "type": "string",
                    "description": "排序方向：desc（默认）或 asc",
                },
            },
        }

    async def execute(
        self,
        video_name: str | None = None,
        class_name: str | None = None,
        min_confidence: float | None = None,
        limit: int = 20,
        sort_by: str = "avg_confidence",
        sort_order: str = "desc",
        **_: Any,
    ) -> str:
        limit = max(1, min(int(limit), 100))
        allowed_sorts = {"avg_confidence", "duration_sec", "track_id", "max_confidence"}
        if sort_by not in allowed_sorts:
            sort_by = "avg_confidence"
        order = "DESC" if sort_order.lower() != "asc" else "ASC"

        conditions: list[str] = []
        params: list[Any] = []
        if video_name:
            conditions.append("video_name = ?")
            params.append(video_name)
        if class_name:
            conditions.append("class_name = ?")
            params.append(class_name)
        if min_confidence is not None:
            conditions.append("avg_confidence >= ?")
            params.append(float(min_confidence))

        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        sql = f"""
            SELECT track_id, video_name, class_name,
                   start_frame, end_frame, total_frames,
                   duration_sec, avg_confidence, max_confidence,
                   image_path
            FROM tracks
            {where}
            ORDER BY {sort_by} {order}
            LIMIT {limit}
        """

        try:
            conn = _connect()
            rows = [dict(r) for r in conn.execute(sql, params).fetchall()]
            conn.close()
            if not rows:
                return "没有找到符合条件的轨迹。"
            return json.dumps(rows, ensure_ascii=False, indent=2)
        except Exception as e:
            return f"查询失败：{e}"


# ── Tool 2: 获取轨迹图像 ──────────────────────────────────────────────────────

class DeimGetTrackImageTool(Tool):
    """Return the best-confidence frame image for a track as base64."""

    @property
    def name(self) -> str:
        return "deimv2_get_track_image"

    @property
    def description(self) -> str:
        return (
            "获取指定轨迹的最佳帧图像（base64 data URL），可直接传给视觉模型分析。"
            "需要提供 video_name 和 track_id（从 deimv2_query_tracks 结果中获取）。"
        )

    @property
    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "video_name": {
                    "type": "string",
                    "description": "视频名称",
                },
                "track_id": {
                    "type": "integer",
                    "description": "轨迹 ID",
                },
            },
            "required": ["video_name", "track_id"],
        }

    async def execute(self, video_name: str, track_id: int, **_: Any) -> str:
        path = os.path.join(_img_root(), video_name, f"track_{track_id}", "best.jpg")
        if not os.path.exists(path):
            # 从数据库里找 image_path 作为备选
            try:
                conn = _connect()
                row = conn.execute(
                    "SELECT image_path FROM tracks WHERE video_name=? AND track_id=?",
                    (video_name, int(track_id)),
                ).fetchone()
                conn.close()
                if row and row["image_path"]:
                    alt = os.path.join(
                        os.path.dirname(_db_path()), "..", row["image_path"]
                    )
                    path = os.path.normpath(alt)
            except Exception:
                pass

        if not os.path.exists(path):
            return f"图像文件不存在：{path}"

        try:
            data = base64.b64encode(open(path, "rb").read()).decode()
            size_kb = os.path.getsize(path) // 1024
            return (
                f"图像路径：{path}（{size_kb} KB）\n"
                f"data:image/jpeg;base64,{data}"
            )
        except Exception as e:
            return f"读取图像失败：{e}"


# ── Tool 3: 统计摘要 ──────────────────────────────────────────────────────────

class DeimStatisticsTool(Tool):
    """Get detection database statistics overview."""

    @property
    def name(self) -> str:
        return "deimv2_statistics"

    @property
    def description(self) -> str:
        return (
            "获取 DEIMv2 检测数据库统计摘要：视频数、轨迹数、检测帧数、"
            "各类别分布、各视频分布、平均置信度等。"
        )

    @property
    def parameters(self) -> dict[str, Any]:
        return {"type": "object", "properties": {}}

    async def execute(self, **_: Any) -> str:
        try:
            conn = _connect()
            total_tracks = conn.execute("SELECT COUNT(*) FROM tracks").fetchone()[0]
            total_dets = conn.execute("SELECT COUNT(*) FROM detections").fetchone()[0]
            avg_conf = conn.execute(
                "SELECT AVG(avg_confidence) FROM tracks"
            ).fetchone()[0]
            by_class = dict(
                conn.execute(
                    "SELECT class_name, COUNT(*) FROM tracks GROUP BY class_name ORDER BY COUNT(*) DESC"
                ).fetchall()
            )
            by_video = dict(
                conn.execute(
                    "SELECT video_name, COUNT(*) FROM tracks GROUP BY video_name ORDER BY COUNT(*) DESC"
                ).fetchall()
            )
            conn.close()

            stats = {
                "total_tracks": total_tracks,
                "total_detections": total_dets,
                "avg_confidence": round(avg_conf or 0, 3),
                "by_class": by_class,
                "by_video": by_video,
            }
            return json.dumps(stats, ensure_ascii=False, indent=2)
        except Exception as e:
            return f"统计失败：{e}"
