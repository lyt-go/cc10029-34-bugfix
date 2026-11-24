"""到达对账领域服务：维护批次应到与实到记录数的对账、迟到与缺失条数统计及结论。

本模块读取传输批次产物文件和暂存落地产物文件进行数量对账，统计迟到与缺失条数，
对账完成后自动驱动时效监控指标更新和传输血缘记录。
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class ArrivalReconcileService(BaseModuleService):
    MODULE = "arrival_reconcile"
    ENTITY = "对账单"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["对账编号", "批次编号", "应到条数", "实到条数", "迟到条数", "缺失条数", "对账窗口", "对账结论"]
    REQUIRED_FIELDS = ["对账编号", "批次编号", "应到条数"]
    STATUS_ORDER = ["草稿", "待对账", "对账中", "已平账", "存在差异"]
    ACTION_RULES = {"启动对账": "对账中", "确认平账": "已平账", "标记差异": "存在差异"}
    NEGATIVE_ACTIONS = ["标记差异"]

    def create_reconciliation(self, batch_code: str, expected_count: int) -> dict[str, Any]:
        """为传输批次创建到达对账单，登记应到条数，为后续到账核对准备基线。"""
        recon_code = f"ARRI-{batch_code}-{int(time.time())}"
        values = {
            "对账编号": recon_code,
            "批次编号": batch_code,
            "应到条数": str(expected_count),
            "实到条数": "0",
            "迟到条数": "0",
            "缺失条数": "0",
            "对账窗口": "daily",
            "对账结论": "待对账",
        }
        entry, missing = self.create_entry(values)
        if entry is None:
            raise ValueError(f"创建对账单失败: {missing}")
        return {"recon_code": recon_code, "recon_id": entry["id"], "batch_code": batch_code,
                "expected_count": expected_count}

    def execute_reconciliation(self, recon_id: int) -> dict[str, Any]:
        """执行到达对账：核实传输批次应到与实到记录数的一致性，统计迟到与缺失条数。

        本方法从传输批次产物文件获取应到条数，从暂存落地产物文件获取实到条数，
        计算迟到（超时到达）和缺失（未到达）条数，判定对账结论。
        """
        recon_entry = self.get_entry(recon_id)
        if recon_entry is None:
            return {"error": f"对账单 {recon_id} 不存在"}

        entry, msg = self.run_action(recon_id, "启动对账")
        if entry is None:
            return {"error": msg, "recon_id": recon_id}

        batch_code = recon_entry["批次编号"]
        expected = int(recon_entry.get("应到条数", 0))

        # 从传输批次产物文件读取源数据
        batch_file = self._data_dir("batches") / f"{batch_code}.jsonl"
        batch_records = self._read_jsonl(batch_file)
        source_count = len(batch_records)

        # 从暂存产品目录查找目标落地产物文件。
        # 只认可「批次编号完全相等」的暂存分区，同前缀批次（如 BATCH-01-A /
        # BATCH-01-B）不得互相串账；同一批次多轮落地时只对账最新一轮分区，
        # 避免把历史轮次的记录累加进本轮实到条数。通配命中的候选文件还要按
        # 记录内实际批次身份二次过滤。
        staging_dir = self._data_dir("staging")
        staging_count = 0
        late_count = 0
        staging_file_name = None
        staging_partition = None

        target_file = None
        exact_entries = [
            se for se in store.rows("staging_area") if se.get("批次编号") == batch_code
        ]
        if exact_entries:
            latest_entry = max(exact_entries, key=lambda se: int(se.get("id", 0)))
            staging_partition = latest_entry.get("落地路径")
            candidate = staging_dir / f"{latest_entry.get('暂存编号')}.jsonl"
            if candidate.exists():
                target_file = candidate
        else:
            # 兼容没有台账行的旧产物：通配候选后按记录携带的实际批次身份二次过滤
            candidates = []
            for sf in staging_dir.glob(f"*{batch_code}*.jsonl"):
                recs = self._read_jsonl(sf)
                if any(
                    r.get("batch_code") == batch_code or r.get("batch_key") == batch_code
                    for r in recs
                ):
                    candidates.append(sf)
            if candidates:
                target_file = max(candidates, key=lambda p: p.stat().st_mtime)
                staging_partition = f"/staging/{batch_code}/"

        if target_file is not None:
            s_records = self._read_jsonl(target_file)
            # 只统计携带本批实际身份的记录，杜绝同前缀/同分区异批记录串入
            exact_records = [
                r for r in s_records
                if r.get("batch_code") == batch_code or r.get("batch_key") == batch_code
            ]
            staging_count = len(exact_records)
            staging_file_name = target_file.name
            # 计算迟到条数：目标端实际记录数少于源端基准
            if staging_count < expected:
                late_count = expected - staging_count

        missing_count = max(0, expected - staging_count)
        is_balanced = expected == staging_count

        entry["实到条数"] = str(staging_count)
        entry["迟到条数"] = str(late_count)
        entry["缺失条数"] = str(missing_count)

        result_action = "确认平账" if is_balanced else "标记差异"
        final_entry, _ = self.run_action(recon_id, result_action)

        # 写入对账结论日志
        self._write_jsonl(
            self._data_dir("events") / "reconcile_log.jsonl",
            [{"action": "reconcile", "batch_code": batch_code, "expected": expected,
              "actual": staging_count, "late": late_count, "missing": missing_count,
              "balanced": is_balanced, "reconciled_at": time.time()}]
        )

        result = {
            "recon_code": recon_entry["对账编号"],
            "batch_code": batch_code,
            "expected": expected,
            "actual": staging_count,
            "late": late_count,
            "missing": missing_count,
            "balanced": is_balanced,
            "conclusion": final_entry["status"] if final_entry else ("已平账" if is_balanced else "存在差异"),
            "staging_partition": staging_partition,
            "staging_file": staging_file_name,
        }

        # 对账结果驱动时效监控指标更新
        if late_count > 0 or not is_balanced:
            try:
                sm_svc = self._safe_import("app.services.sla_monitor", "SlaMonitorService")()
                sm_result = sm_svc.update_arrival_stats(batch_code, late_count, missing_count)
                result["auto_sla_update"] = sm_result
            except Exception as e:
                result["auto_sla_update"] = {"error": str(e)}

        # 对账完成后记录传输血缘：把对账入口的实际批次身份与对账结果整体传入，
        # 血缘侧将来源与该对账结果写入同一条记录（原子配对），避免跨批次错配。
        try:
            tl_svc = self._safe_import("app.services.transfer_lineage", "TransferLineageService")()
            lineage_result = tl_svc.record_lineage(batch_code, is_balanced, reconcile=result)
            result["auto_lineage"] = lineage_result
        except Exception as e:
            result["auto_lineage"] = {"error": str(e)}

        return result