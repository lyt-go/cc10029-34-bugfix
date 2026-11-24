"""传输血缘领域服务：维护源端经抽取窗口、传输批次到目标端的交换链路血缘追溯。

本模块记录每一次数据交换的完整链路信息（源端→窗口→批次→目标端），
生成血缘产物文件，供下游数据追溯和质量审计使用。

关键设计（精确批次血缘）：

1. 血缘按**完整批次编号**（transfer_batch）精确落盘与检索，不再按批次编号前缀
   做通配匹配。同前缀的两个批次（如 BATCH-01-A / BATCH-01-B）各自独立，
   查询任一批次都不会混入另一批次的记录。
2. 每条血缘记录把「来源（源端/窗口/契约）」与「对账结果（对账编号/对账结论/
   应到实到/暂存分区）」写入同一条不可拆分的关系记录，复核时只可能拿到
   同一批次同一轮次内配好对的版本，杜绝拿 A 的来源配 B 的结果。
3. 血缘支持同一批次多轮次（round）与血缘版本（lineage_version）。每轮先写
   pending 记录（写入未完成/未确认），确认后追加 confirmed 记录。查询默认只
   返回已确认的完整版本；最新一轮尚未确认时明示「等待确认」，同时仍可复核
   上一轮的完整已确认版本，半条关系不会进入复核结果。
4. 新格式统一写入 lineage 目录下的 ``lineage_journal.jsonl`` 追加日志；
   历史遗留的 ``TRAN-*.jsonl`` 单链路文件仍可读取（视为已确认记录），
   保证旧文件中的有效记录可继续查询。
"""
from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from app.services.base import BaseModuleService
from app.store import store

JOURNAL_NAME = "lineage_journal.jsonl"
LEGACY_GLOB = "TRAN-*.jsonl"


class TransferLineageService(BaseModuleService):
    MODULE = "transfer_lineage"
    ENTITY = "血缘链路"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["链路编号", "源端编号", "抽取窗口", "传输批次", "目标编号", "字段契约", "血缘版本", "采集时间"]
    REQUIRED_FIELDS = ["链路编号", "源端编号", "抽取窗口"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "确认生效": "已生效", "标记停用": "已停用"}
    NEGATIVE_ACTIONS = ["标记停用"]

    # ------------------------------------------------------------------
    # 产物文件
    # ------------------------------------------------------------------

    def _journal_file(self) -> Path:
        return self._data_dir("lineage") / JOURNAL_NAME

    def _next_seq(self) -> int:
        """基于日志现有条数生成单调递增序号，避免同秒写入产生重名链路编号。"""
        return len(self._load_all_records()) + 1

    def _load_all_records(self) -> list[dict[str, Any]]:
        """读取全部血缘记录：新格式追加日志 + 历史遗留单链路文件。

        遗留文件没有 round/confirm_state 字段，读取时补齐为「第 1 轮、已确认」，
        使旧文件中的有效记录仍可被精确查询与复核。
        """
        records: list[dict[str, Any]] = []
        journal = self._journal_file()
        if journal.exists():
            records.extend(self._read_jsonl(journal))

        lineage_dir = self._data_dir("lineage")
        for legacy in sorted(lineage_dir.glob(LEGACY_GLOB)):
            for rec in self._read_jsonl(legacy):
                rec.setdefault("round", 1)
                rec.setdefault("confirm_state", "confirmed")
                rec.setdefault("lineage_version", rec.get("lineage_version", "v1.0"))
                rec.setdefault("lineage_code", rec.get("lineage_code", legacy.stem))
                rec.setdefault("transfer_batch", rec.get("transfer_batch", rec.get("batch_code", "")))
                rec["_legacy_file"] = legacy.name
                records.append(rec)
        return records

    # ------------------------------------------------------------------
    # 写入：分两阶段（pending → confirmed），来源与对账结果同条原子配对
    # ------------------------------------------------------------------

    def _resolve_context(self, batch_code: str) -> dict[str, str]:
        """解析血缘上下文中的源端、目标端、抽取窗口与字段契约标识。"""
        source_info = target_info = window_info = contract_info = {}
        for s in store.rows("source_register"):
            if s.get("status") == "已登记":
                source_info = s
                break
        for t in store.rows("target_register"):
            if t.get("status") == "已登记":
                target_info = t
                break
        for w in store.rows("extract_window"):
            if w.get("窗口编号", "").startswith("EXTR-"):
                window_info = w
                break
        for c in store.rows("field_contract"):
            if c.get("status") == "已生效":
                contract_info = c
                break
        return {
            "source_code": source_info.get("源端编号", f"SOUR-{batch_code}"),
            "source_system": source_info.get("系统名称", "unknown"),
            "extract_window": window_info.get("窗口编号", f"EXTR-{batch_code}"),
            "target_code": target_info.get("目标编号", f"TARG-{batch_code}"),
            "target_system": target_info.get("系统名称", "unknown"),
            "field_contract": contract_info.get("契约编号", f"FIEL-{batch_code}"),
        }

    def _next_round(self, batch_code: str) -> int:
        """计算批次的下一轮轮次：已存在记录（含 pending）的最大轮次 + 1。"""
        rounds = [
            int(r.get("round", 1))
            for r in self._load_all_records()
            if r.get("transfer_batch") == batch_code
        ]
        return (max(rounds) if rounds else 0) + 1

    def _build_record(
        self,
        batch_code: str,
        round_no: int,
        confirm_state: str,
        reconcile: dict[str, Any] | None,
        is_balanced: bool,
    ) -> dict[str, Any]:
        """构建一条血缘记录。来源信息与对账结果在同一条记录内配对，不可拆分。"""
        ctx = self._resolve_context(batch_code)
        now = time.time()
        seq = self._next_seq()
        lineage_code = f"TRAN-{batch_code}-r{round_no}-{seq}"
        record = {
            # —— 实际批次身份：全流程保留完整批次编号，不做任何前缀截断 ——
            "lineage_code": lineage_code,
            "transfer_batch": batch_code,
            "batch_code": batch_code,  # 兼容旧字段命名
            "round": round_no,
            "lineage_version": f"v{round_no}.0",
            "confirm_state": confirm_state,  # pending / confirmed
            # —— 来源侧：源端 → 抽取窗口 → 字段契约 ——
            "source_code": ctx["source_code"],
            "source_system": ctx["source_system"],
            "extract_window": ctx["extract_window"],
            "field_contract": ctx["field_contract"],
            # —— 目标侧：目标端 + 实际暂存分区（到达对账入口保留实际批次身份）——
            "target_code": ctx["target_code"],
            "target_system": ctx["target_system"],
            "staging_partition": (reconcile or {}).get("staging_partition"),
            "staging_file": (reconcile or {}).get("staging_file"),
            # —— 对账结果：与来源同条配对，复核不会跨批次/跨轮次错配 ——
            "balanced": is_balanced,
            "reconcile_code": (reconcile or {}).get("recon_code"),
            "reconcile_result": (reconcile or {}).get("conclusion"),
            "expected_count": (reconcile or {}).get("expected"),
            "actual_count": (reconcile or {}).get("actual"),
            "late_count": (reconcile or {}).get("late"),
            "missing_count": (reconcile or {}).get("missing"),
            "recorded_at": now,
            "confirmed_at": now if confirm_state == "confirmed" else None,
        }
        return record

    def _persist_record(self, record: dict[str, Any]) -> dict[str, Any]:
        """把血缘记录追加到统一日志，并同步内存台账（store）状态。"""
        self._write_jsonl(self._journal_file(), [record])

        values = {
            "链路编号": record["lineage_code"],
            "源端编号": record["source_code"],
            "抽取窗口": record["extract_window"],
            "传输批次": record["transfer_batch"],
            "目标编号": record["target_code"],
            "字段契约": record["field_contract"],
            "血缘版本": record["lineage_version"],
            "采集时间": time.strftime("%Y-%m-%d", time.localtime(record["recorded_at"])),
        }
        entry, missing = self.create_entry(values)
        if entry is not None:
            if record["confirm_state"] == "confirmed":
                self.run_action(entry["id"], "确认生效")
            else:
                # 半条未确认关系停留在待校验，不进入可复核的已生效版本
                self.run_action(entry["id"], "提交校验")
            entry["round"] = record["round"]
            entry["confirm_state"] = record["confirm_state"]
        return record

    def record_lineage(
        self,
        batch_code: str,
        is_balanced: bool,
        reconcile: dict[str, Any] | None = None,
        *,
        confirm: bool = True,
    ) -> dict[str, Any]:
        """记录传输批次的血缘链路（对账完成后调用）。

        默认一次写入并确认（``confirm=True``），形成完整已确认版本；
        传入 ``confirm=False`` 时只落 pending 记录，模拟「来源已写、确认中断」
        的场景，随后可用 :meth:`confirm_lineage` 补确认。

        若该批次已存在尚未确认的 pending 轮次（新一轮写入中断中），对账完成
        触发的血缘写入会复用该轮次而不是另开一轮——对账结果正是这一轮缺失的
        另一半，随后的确认动作把同一轮提升为完整已确认版本。
        """
        open_pending = self._effective_pending(
            [r for r in self._load_all_records() if r.get("transfer_batch") == batch_code]
        )
        if open_pending:
            latest = max(open_pending, key=lambda r: (int(r.get("round", 1)), r.get("recorded_at", 0)))
            round_no = int(latest.get("round", 1))
        else:
            round_no = self._next_round(batch_code)
        record = self._build_record(
            batch_code, round_no, "confirmed" if confirm else "pending", reconcile, is_balanced
        )
        self._persist_record(record)
        return self._public_view(record)

    def begin_lineage_round(
        self, batch_code: str, is_balanced: bool, reconcile: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """开启新一轮血缘写入：先落 pending 记录（写入未完成，不可用于复核）。"""
        return self.record_lineage(batch_code, is_balanced, reconcile, confirm=False)

    def confirm_lineage(
        self, batch_code: str, reconcile: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """确认批次最新一轮血缘：把 pending 关系补齐对账结果并追加 confirmed 记录。

        pending 记录保持原样留痕（可识别中断），确认以追加新版本的方式完成，
        旧文件/旧轮次的已确认记录不受影响。
        """
        pending = self._effective_pending(
            [r for r in self._load_all_records() if r.get("transfer_batch") == batch_code]
        )
        if not pending:
            raise ValueError(f"批次 {batch_code} 没有等待确认的血缘写入（可能该轮已确认）")
        latest = max(pending, key=lambda r: (int(r.get("round", 1)), r.get("recorded_at", 0)))
        round_no = int(latest.get("round", 1))
        is_balanced = bool(latest.get("balanced", False))
        record = self._build_record(batch_code, round_no, "confirmed", reconcile, is_balanced)
        self._persist_record(record)
        return self._public_view(record)

    # ------------------------------------------------------------------
    # 查询：精确批次血缘结果（默认只返回完整已确认版本）
    # ------------------------------------------------------------------

    @staticmethod
    def _public_view(record: dict[str, Any]) -> dict[str, Any]:
        """对外视图：保留实际批次身份与完整链路字段，隐藏内部遗留标记。"""
        view = {k: v for k, v in record.items() if not k.startswith("_")}
        return view

    @staticmethod
    def _effective_pending(exact: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """返回仍未确认的 pending 记录。

        日志为追加式：确认新一轮时会保留原 pending 记录留痕，但若同一轮次已经
        存在 confirmed 记录，则该 pending 视为被取代，不再计入等待确认。
        """
        confirmed_rounds = {
            int(r.get("round", 1)) for r in exact if r.get("confirm_state") == "confirmed"
        }
        return [
            r for r in exact
            if r.get("confirm_state") == "pending" and int(r.get("round", 1)) not in confirmed_rounds
        ]

    def query_lineage(self, batch_code: str, *, include_pending: bool = False) -> dict[str, Any]:
        """查询指定传输批次的精确血缘链路。

        - 只匹配 ``transfer_batch`` 与完整批次编号**相等**的记录，同前缀批次互不串账；
        - 默认仅返回 ``confirmed`` 完整版本，按轮次升序；
        - 若存在未确认的新一轮写入，返回 ``pending_rounds`` 并在消息中明示等待确认，
          同时仍给出最近一个已确认版本供复核。
        """
        all_records = self._load_all_records()
        exact = [r for r in all_records if r.get("transfer_batch") == batch_code]
        confirmed = sorted(
            (r for r in exact if r.get("confirm_state") == "confirmed"),
            key=lambda r: (int(r.get("round", 1)), r.get("recorded_at", 0)),
        )
        pending = sorted(
            self._effective_pending(exact),
            key=lambda r: (int(r.get("round", 1)), r.get("recorded_at", 0)),
        )

        lineages = [self._public_view(r) for r in confirmed]
        if include_pending:
            lineages.extend(self._public_view(r) for r in pending)
            lineages.sort(key=lambda r: (int(r.get("round", 1)), r.get("recorded_at", 0)))

        latest_confirmed = self._public_view(confirmed[-1]) if confirmed else None
        waiting = bool(pending and not include_pending)
        message = "OK"
        if waiting:
            message = (
                f"批次 {batch_code} 第 {pending[-1].get('round')} 轮血缘尚未确认，"
                "当前仅可复核最近已确认版本；请等待确认后再复核新一轮。"
            )
        elif not lineages:
            message = f"批次 {batch_code} 暂无可复核的已确认血缘记录"

        return {
            "batch_code": batch_code,
            "exact_match": True,
            "lineages": lineages,
            "count": len(lineages),
            "latest_confirmed": latest_confirmed,
            "pending_rounds": [int(r.get("round", 1)) for r in pending],
            "waiting_confirmation": waiting,
            "message": message,
        }

    # ------------------------------------------------------------------
    # 复核：只基于精确批次的完整已确认血缘版本
    # ------------------------------------------------------------------

    def review_lineage(self, batch_code: str, round_no: int | None = None) -> dict[str, Any]:
        """从精确批次血缘结果发起复核。

        - 严格限定在该完整批次编号的已确认记录内选取版本，绝不引用其他批次
          （含同前缀批次）的来源或对账结果；
        - ``round_no`` 为空时复核最近已确认版本；指定轮次时复核该轮确认版本；
        - 目标轮次仍处于 pending（写入未完成）时拒绝复核并明示等待确认，
          此时若存在历史已确认版本则一并返回，保证始终有可复核结果。
        """
        exact = [
            r for r in self._load_all_records() if r.get("transfer_batch") == batch_code
        ]
        confirmed = [r for r in exact if r.get("confirm_state") == "confirmed"]
        pending_rounds = {int(r.get("round", 1)) for r in self._effective_pending(exact)}

        if not confirmed:
            return {
                "ok": False,
                "batch_code": batch_code,
                "round": round_no,
                "reviewable": False,
                "waiting_confirmation": bool(pending_rounds),
                "message": (
                    f"批次 {batch_code} 暂无已确认血缘版本"
                    + ("，新一轮写入尚未完成，请等待确认" if pending_rounds else "")
                ),
            }

        if round_no is None:
            target = max(confirmed, key=lambda r: (int(r.get("round", 1)), r.get("recorded_at", 0)))
        else:
            candidates = [r for r in confirmed if int(r.get("round", 1)) == round_no]
            if not candidates:
                if round_no in pending_rounds:
                    latest = max(confirmed, key=lambda r: (int(r.get("round", 1)), r.get("recorded_at", 0)))
                    return {
                        "ok": False,
                        "batch_code": batch_code,
                        "round": round_no,
                        "reviewable": False,
                        "waiting_confirmation": True,
                        "fallback_confirmed_round": int(latest.get("round", 1)),
                        "review": self._review_payload(latest),
                        "message": (
                            f"批次 {batch_code} 第 {round_no} 轮血缘尚未确认，"
                            f"暂不可复核；可先复核第 {latest.get('round')} 轮已确认版本。"
                        ),
                    }
                raise ValueError(f"批次 {batch_code} 不存在第 {round_no} 轮已确认血缘")
            target = max(candidates, key=lambda r: r.get("recorded_at", 0))

        return {
            "ok": True,
            "batch_code": batch_code,
            "round": int(target.get("round", 1)),
            "reviewable": True,
            "waiting_confirmation": False,
            "review": self._review_payload(target),
            "message": "复核通过：来源与对账结果来自同一批次同一轮次的已确认血缘版本",
        }

    def _review_payload(self, record: dict[str, Any]) -> dict[str, Any]:
        """组装复核明细：批次身份、采用版本、实际暂存分区与来源-结果配对校验。"""
        paired = (
            record.get("transfer_batch") == record.get("batch_code", record.get("transfer_batch"))
            and record.get("confirm_state") == "confirmed"
            and record.get("reconcile_code") is not None
        )
        return {
            "lineage_code": record.get("lineage_code"),
            "transfer_batch": record.get("transfer_batch"),
            "round": int(record.get("round", 1)),
            "lineage_version": record.get("lineage_version"),
            "confirm_state": record.get("confirm_state"),
            "source_code": record.get("source_code"),
            "extract_window": record.get("extract_window"),
            "field_contract": record.get("field_contract"),
            "target_code": record.get("target_code"),
            "staging_partition": record.get("staging_partition"),
            "staging_file": record.get("staging_file"),
            "reconcile_code": record.get("reconcile_code"),
            "reconcile_result": record.get("reconcile_result"),
            "expected_count": record.get("expected_count"),
            "actual_count": record.get("actual_count"),
            "late_count": record.get("late_count"),
            "missing_count": record.get("missing_count"),
            "balanced": record.get("balanced"),
            "source_result_paired": paired,
            "reviewed_at": time.time(),
        }

    def get_active_lineages(self) -> list[dict[str, Any]]:
        """获取所有已生效的血缘链路列表。"""
        return [r for r in store.rows(self.MODULE) if r.get("status") == "已生效"]
