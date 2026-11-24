"""校验核对领域服务：维护传输批次源端与目标端校验值的逐分片比对与差异条数结论。

本模块从传输批次产物文件读取数据，计算源端与目标端校验值，执行逐分片比对，
校验完成后自动驱动冲突裁决和拒收回执创建。
"""
from __future__ import annotations

import hashlib
import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class ChecksumVerifyService(BaseModuleService):
    MODULE = "checksum_verify"
    ENTITY = "校验任务"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["校验编号", "批次编号", "源端校验值", "目标校验值", "分片校验", "差异条数", "校验算法", "校验结论"]
    REQUIRED_FIELDS = ["校验编号", "批次编号", "源端校验值"]
    STATUS_ORDER = ["草稿", "待校验", "校验中", "已通过", "未通过"]
    ACTION_RULES = {"启动校验": "校验中", "判定通过": "已通过", "判定未过": "未通过"}
    NEGATIVE_ACTIONS = ["判定未过"]

    def create_verification(self, batch_code: str, record_count: int) -> dict[str, Any]:
        """为传输批次创建校验核对任务，登记校验编号与校验算法，用于源端目标端数据对账。"""
        verify_code = f"CHEC-{batch_code}-{int(time.time())}"
        values = {
            "校验编号": verify_code,
            "批次编号": batch_code,
            "源端校验值": "pending",
            "目标校验值": "pending",
            "分片校验": "sha256",
            "差异条数": "0",
            "校验算法": "SHA256",
            "校验结论": "待校验",
        }
        entry, missing = self.create_entry(values)
        if entry is None:
            raise ValueError(f"创建校验任务失败: {missing}")
        return {"verify_code": verify_code, "verify_id": entry["id"], "batch_code": batch_code}

    def execute_verification(self, verify_id: int) -> dict[str, Any]:
        """执行校验核对：读取传输批次产物文件和暂存落地产物文件，逐分片比对校验值。

        本方法实现数据交换平台的核心质量保障：从传输产物文件计算源端校验值，
        从暂存产物文件计算目标端校验值，逐分片比对并统计差异条数。
        校验结果直接决定后续冲突裁决和拒收处置。
        """
        verify_entry = self.get_entry(verify_id)
        if verify_entry is None:
            return {"error": f"校验任务 {verify_id} 不存在"}

        entry, msg = self.run_action(verify_id, "启动校验")
        if entry is None:
            return {"error": msg, "verify_id": verify_id}

        batch_code = verify_entry["批次编号"]

        # 读取传输批次产物文件（源端数据）
        batch_file = self._data_dir("batches") / f"{batch_code}.jsonl"
        batch_records = self._read_jsonl(batch_file)

        # 从暂存产品目录查找对应落地产物文件（目标端数据）。
        # 必须按完整批次编号精确解析暂存分区，同前缀批次的分区不得串用；
        # 优先用暂存台账定位，缺失台账时再用通配候选 + 记录内批次身份过滤。
        staging_dir = self._data_dir("staging")
        staging_records = []
        exact_entries = [
            se for se in store.rows("staging_area") if se.get("批次编号") == batch_code
        ]
        target_staging = None
        if exact_entries:
            latest_entry = max(exact_entries, key=lambda se: int(se.get("id", 0)))
            candidate = staging_dir / f"{latest_entry.get('暂存编号')}.jsonl"
            if candidate.exists():
                target_staging = candidate
        else:
            for staging_file in staging_dir.glob(f"*{batch_code}*.jsonl"):
                recs = self._read_jsonl(staging_file)
                if any(
                    r.get("batch_code") == batch_code or r.get("batch_key") == batch_code
                    for r in recs
                ):
                    target_staging = staging_file
                    break
        if target_staging is not None:
            staging_records = [
                r for r in self._read_jsonl(target_staging)
                if r.get("batch_code") == batch_code
                or r.get("batch_key") == batch_code
            ]

        # 计算源端与目标端校验值
        source_checksum = self._compute_checksum(str(batch_records))
        target_checksum = self._compute_checksum(str(staging_records))

        # 逐分片比对差异
        total_diff = 0
        chunk_diffs = []
        chunk_size = 5
        for i in range(0, max(len(batch_records), len(staging_records)), chunk_size):
            b_chunk = batch_records[i:i + chunk_size]
            s_chunk = staging_records[i:i + chunk_size]
            b_hash = self._compute_checksum(str(b_chunk))
            s_hash = self._compute_checksum(str(s_chunk))
            diff = 1 if b_hash != s_hash else 0
            total_diff += diff
            chunk_diffs.append({"chunk_index": i // chunk_size, "source_hash": b_hash,
                               "target_hash": s_hash, "diff": diff})

        # 判定校验结论
        passed = total_diff == 0
        source_val = f"SHA256:{source_checksum}"
        target_val = f"SHA256:{target_checksum}"
        entry["源端校验值"] = source_val
        entry["目标校验值"] = target_val
        entry["差异条数"] = str(total_diff)

        result_action = "判定通过" if passed else "判定未过"
        final_entry, _ = self.run_action(verify_id, result_action)

        # 写入校验结论日志，记录源端与目标端校验详情
        self._write_jsonl(
            self._data_dir("events") / "verify_log.jsonl",
            [{"action": "verify", "batch_code": batch_code, "source_checksum": source_checksum,
              "target_checksum": target_checksum, "diff_count": total_diff,
              "passed": passed, "verified_at": time.time()}]
        )

        result = {
            "verify_code": verify_entry["校验编号"],
            "batch_code": batch_code,
            "source_checksum": source_checksum,
            "target_checksum": target_checksum,
            "diff_count": total_diff,
            "passed": passed,
            "chunk_diffs": chunk_diffs,
        }

        # 校验差异条目驱动冲突裁决创建
        if total_diff > 0:
            try:
                cr_svc = self._safe_import("app.services.conflict_resolve", "ConflictResolveService")()
                conflict_result = cr_svc.create_conflict(batch_code, total_diff)
                result["auto_conflict"] = conflict_result
            except Exception as e:
                result["auto_conflict"] = {"error": str(e)}

            # 差异驱动拒收回执
            try:
                rr_svc = self._safe_import("app.services.reject_record", "RejectRecordService")()
                reject_result = rr_svc.create_rejection(batch_code, "校验", f"SHA256校验差异{total_diff}条")
                result["auto_reject"] = reject_result
            except Exception as e:
                result["auto_reject"] = {"error": str(e)}

        return result