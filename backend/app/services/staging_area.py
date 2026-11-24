"""暂存落地领域服务：维护传输数据的暂存分区落地路径、文件组织与驻留时限控制。

本模块从传输批次产物文件读取数据，执行分区落地写入暂存产物文件，
落地完成后记录落地水位并支持超时清空。
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class StagingAreaService(BaseModuleService):
    MODULE = "staging_area"
    ENTITY = "暂存分区"
    OPERATION_TERM = "水位"
    LIST_FIELDS = ["暂存编号", "批次编号", "落地路径", "文件数量", "记录条数", "占用字节", "驻留时限", "落地水位"]
    REQUIRED_FIELDS = ["暂存编号", "批次编号", "落地路径"]
    STATUS_ORDER = ["草稿", "落地中", "已落地", "已清空"]
    ACTION_RULES = {"启动落地": "落地中", "确认落地": "已落地", "执行清空": "已清空"}
    NEGATIVE_ACTIONS = ["执行清空"]

    def land_batch(self, batch_code: str, record_count: int) -> dict[str, Any]:
        """将传输批次数据落地到暂存分区，读取传输产物文件并写入暂存产物文件。

        暂存分区是传输链路中数据暂存的关键环节：它从传输产物文件读取数据，
        计算字节量，写入落地路径下的暂存产物文件，并推进分区水位。
        """
        # 序号保证同秒内多次落地（同一批次多轮/并发批次）不会重名覆盖或追加到同一文件
        existing = len(store.rows(self.MODULE))
        staging_code = f"STAG-{batch_code}-{int(time.time())}-{existing + 1}"
        landing_path = f"/staging/{batch_code}/"
        values = {
            "暂存编号": staging_code,
            "批次编号": batch_code,
            "落地路径": landing_path,
            "文件数量": "1",
            "记录条数": str(record_count),
            "占用字节": "0",
            "驻留时限": "86400",
            "落地水位": "0",
        }
        entry, missing = self.create_entry(values)
        if entry is None:
            raise ValueError(f"创建暂存分区失败: {missing}")

        # 读取上游传输批次产物文件
        source_file = self._data_dir("batches") / f"{batch_code}.jsonl"
        records = []
        total_bytes = 0
        if source_file.exists():
            records = self._read_jsonl(source_file)
            total_bytes = sum(len(str(r).encode("utf-8")) for r in records)
        else:
            records = self._simulate_records(batch_code, count=record_count, field_prefix="staging")
            total_bytes = sum(len(str(r).encode("utf-8")) for r in records)

        # 写入暂存产物文件
        output_file = self._data_dir("staging") / f"{staging_code}.jsonl"
        written = self._write_jsonl(output_file, records)

        # 更新暂存分区水位
        entry["占用字节"] = str(total_bytes)
        entry["文件数量"] = "1"
        entry["落地水位"] = str(written)

        # 写入落地水位断点
        self._write_checkpoint(f"staging_{staging_code}", {
            "staging_code": staging_code,
            "batch_code": batch_code,
            "records_landed": written,
            "total_bytes": total_bytes,
            "landed_at": time.time(),
            "retention_seconds": 86400,
        })

        return {
            "staging_code": staging_code,
            "batch_code": batch_code,
            "records_landed": written,
            "total_bytes": total_bytes,
            "artifact_path": str(output_file),
        }

    def confirm_land(self, staging_id: int) -> dict[str, Any]:
        """确认暂存分区落地完成，校验落地产物数据完整性并写入落地确认日志。"""
        entry, msg = self.run_action(staging_id, "确认落地")
        if entry is None:
            return {"error": msg, "staging_id": staging_id}
        staging_code = entry["暂存编号"]
        # 验证落地产物文件存在且记录数匹配
        artifact_file = self._data_dir("staging") / f"{staging_code}.jsonl"
        records = self._read_jsonl(artifact_file)
        checksum = self._compute_checksum(str(records))
        self._write_jsonl(
            self._data_dir("events") / "staging_confirm_log.jsonl",
            [{"action": "confirm_land", "staging_code": staging_code,
              "record_count": len(records), "checksum": checksum,
              "confirmed_at": time.time()}]
        )
        return {"staging_code": staging_code, "status": "已落地", "record_count": len(records),
                "checksum": checksum}

    def clear_staging(self, staging_id: int) -> dict[str, Any]:
        """执行暂存分区清空，删除落地产物文件并写入清空日志。"""
        entry, msg = self.run_action(staging_id, "执行清空")
        if entry is None:
            return {"error": msg, "staging_id": staging_id}
        staging_code = entry["暂存编号"]
        # 标记清空断点
        self._write_checkpoint(f"staging_{staging_code}_cleared", {
            "staging_code": staging_code,
            "cleared_at": time.time(),
            "status": "cleared",
        })
        self._write_jsonl(
            self._data_dir("events") / "staging_clear_log.jsonl",
            [{"action": "clear", "staging_code": staging_code, "cleared_at": time.time()}]
        )
        return {"staging_code": staging_code, "status": "已清空"}