"""精确批次血缘集成测试：同前缀批次串账、半条写入隔离、复核版本选择与旧文件兼容。

复现值班场景：
1. 同一文件（血缘追加日志）中保留两个同秒完成对账的同前缀批次
   BATCH-01-A / BATCH-01-B，查询任一者都不得混入另一者；
2. A 的下一轮血缘写入在「来源与对账结果之间」中断（pending），B 并发追加，
   值班人员从 A 的血缘发起复核：只能看到 A 完整已确认版本；
3. A 新一轮确认后，复核必须精确关联该轮的来源与对账结果及实际暂存分区；
4. 历史遗留的单链路 jsonl 文件中的有效记录仍可精确查询与复核。
"""
from __future__ import annotations

import uuid

import pytest

from app.services.arrival_reconcile import ArrivalReconcileService
from app.services.base import DATA_ROOT
from app.services.staging_area import StagingAreaService
from app.services.transfer_lineage import JOURNAL_NAME, TransferLineageService


@pytest.fixture(autouse=True)
def reset_store():
    """每个测试前重置内存台账；磁盘产物按唯一批次号隔离，追加历史不影响断言。"""
    from app.seed import SEED_ROWS
    from app.store import store
    store._tables = {
        name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
    }
    yield


def _reconcile_batch(batch_code: str, record_count: int) -> dict:
    """落地暂存分区并完成到达对账，返回对账结果（同时自动写入已确认血缘）。"""
    StagingAreaService().land_batch(batch_code, record_count)
    ar_svc = ArrivalReconcileService()
    recon = ar_svc.create_reconciliation(batch_code, record_count)
    return ar_svc.execute_reconciliation(recon["recon_id"])


class TestSamePrefixBatchLineage:
    """同前缀批次的精确血缘查询与复核，不得串账。"""

    def test_same_prefix_batches_do_not_mix(self):
        suffix = uuid.uuid4().hex[:8]
        batch_a = f"BATCH-01-A-{suffix}"
        batch_b = f"BATCH-01-B-{suffix}"
        # 缺陷根因：旧实现按批次编号前 8 位通配匹配，两批前 8 位完全相同
        assert batch_a[:8] == batch_b[:8] == "BATCH-01"

        result_a = _reconcile_batch(batch_a, 5)
        result_b = _reconcile_batch(batch_b, 7)
        assert result_a["balanced"] is True
        assert result_b["balanced"] is True

        tl = TransferLineageService()
        qa = tl.query_lineage(batch_a)
        qb = tl.query_lineage(batch_b)

        # 精确批次：A 的结果里一条 B 都不能有，反之亦然
        assert qa["count"] == 1
        assert all(r["transfer_batch"] == batch_a for r in qa["lineages"])
        assert qb["count"] == 1
        assert all(r["transfer_batch"] == batch_b for r in qb["lineages"])

        # 对账批号与血缘采用的对账结果一一对应，不能拿 A 的来源配 B 的结果
        lineage_a = qa["lineages"][0]
        lineage_b = qb["lineages"][0]
        assert lineage_a["reconcile_code"] == result_a["recon_code"]
        assert lineage_b["reconcile_code"] == result_b["recon_code"]
        assert batch_b not in str(lineage_a["reconcile_code"])
        assert batch_a not in str(lineage_b["reconcile_code"])

        # 到达对账入口与血缘都保留实际批次身份与实际暂存分区
        assert lineage_a["staging_partition"] == f"/staging/{batch_a}/"
        assert lineage_a["staging_file"] == result_a["staging_file"]
        assert lineage_a["actual_count"] == 5
        assert lineage_b["actual_count"] == 7

        review_a = tl.review_lineage(batch_a)
        assert review_a["ok"] is True
        assert review_a["review"]["transfer_batch"] == batch_a
        assert review_a["review"]["source_result_paired"] is True
        assert review_a["review"]["reconcile_code"] == result_a["recon_code"]
        assert review_a["review"]["staging_partition"] == f"/staging/{batch_a}/"

        # 同一份追加日志文件里同时承载两批记录
        journal = DATA_ROOT / "lineage" / JOURNAL_NAME
        assert journal.exists()

    def test_reconcile_counts_use_exact_staging_partition(self):
        """同前缀两批的暂存分区同时存在时，对账实到条数只统计本批分区。"""
        suffix = uuid.uuid4().hex[:8]
        batch_a = f"BATCH-01-A-{suffix}"
        batch_b = f"BATCH-01-B-{suffix}"
        _reconcile_batch(batch_b, 9)  # 先落地 B，制造通配匹配干扰
        result_a = _reconcile_batch(batch_a, 4)
        assert result_a["actual"] == 4
        assert result_a["balanced"] is True


class TestInterruptedRoundAndConcurrentAppend:
    """A 新一轮写入未确认期间 B 并发追加：两时段查询与复核的版本正确性。"""

    def test_pending_round_is_not_reviewable_until_confirmed(self):
        suffix = uuid.uuid4().hex[:8]
        batch_a = f"BATCH-01-A-{suffix}"
        batch_b = f"BATCH-01-B-{suffix}"

        # 第 1 轮：A、B 同秒完成对账（唯一后缀保证编号稳定，时间戳可能相同）
        round1_a = _reconcile_batch(batch_a, 5)
        round1_b = _reconcile_batch(batch_b, 6)

        tl = TransferLineageService()

        # A 开启下一轮血缘写入，但确认被暂停：只有来源侧，没有对账确认
        pending = tl.begin_lineage_round(batch_a, is_balanced=True, reconcile=None)
        assert pending["confirm_state"] == "pending"
        assert pending["round"] == 2

        # B 并发追加一条新的已确认记录
        round2_b = _reconcile_batch(batch_b, 8)

        # —— 时段一：A 新一轮写入未完成 ——
        qa = tl.query_lineage(batch_a)
        assert qa["waiting_confirmation"] is True
        assert qa["pending_rounds"] == [2]
        # 一次查询只见 A 的完整已确认版本（第 1 轮），半条关系不出现，B 也不混入
        assert qa["count"] == 1
        assert qa["lineages"][0]["round"] == 1
        assert qa["lineages"][0]["lineage_version"] == "v1.0"
        assert all(r["transfer_batch"] == batch_a for r in qa["lineages"])
        assert "等待确认" in qa["message"]
        # 显式带上 pending 时可见半条，但状态明确为未确认
        qa_all = tl.query_lineage(batch_a, include_pending=True)
        assert {r["round"]: r["confirm_state"] for r in qa_all["lineages"]} == {
            1: "confirmed",
            2: "pending",
        }

        # 默认复核落到第 1 轮完整版本，采用第 1 轮的分区与对账结果
        review = tl.review_lineage(batch_a)
        assert review["ok"] is True
        assert review["round"] == 1
        assert review["review"]["staging_partition"] == f"/staging/{batch_a}/"
        assert review["review"]["reconcile_code"] == round1_a["recon_code"]
        # 绝不允许拿 A 的来源配 B 的结果
        assert round1_b["recon_code"] != review["review"]["reconcile_code"]
        assert round2_b["recon_code"] != review["review"]["reconcile_code"]

        # 直接复核未确认的第 2 轮：明示等待确认，并给出可复核的第 1 轮回退
        review_pending = tl.review_lineage(batch_a, round_no=2)
        assert review_pending["ok"] is False
        assert review_pending["waiting_confirmation"] is True
        assert review_pending["fallback_confirmed_round"] == 1
        assert review_pending["review"]["round"] == 1

        # B 在并发期间始终可查且只有 B 自己的记录
        qb = tl.query_lineage(batch_b)
        assert qb["count"] == 2
        assert all(r["transfer_batch"] == batch_b for r in qb["lineages"])

        # —— A 新一轮对账完成：对账驱动的血缘写入复用挂起的第 2 轮，
        # 把该轮来源与对账结果原子配成完整已确认版本 ——
        round2_recon = _reconcile_batch(batch_a, 10)
        qa_mid = tl.query_lineage(batch_a)
        assert qa_mid["waiting_confirmation"] is False
        confirmed = qa_mid["latest_confirmed"]
        assert confirmed["confirm_state"] == "confirmed"
        assert confirmed["round"] == 2
        assert confirmed["lineage_version"] == "v2.0"

        # —— 时段二：确认完成后 ——
        qa2 = tl.query_lineage(batch_a)
        assert qa2["waiting_confirmation"] is False
        assert qa2["pending_rounds"] == []
        assert [r["round"] for r in qa2["lineages"]] == [1, 2]
        latest = qa2["latest_confirmed"]
        assert latest["round"] == 2
        assert latest["reconcile_code"] == round2_recon["recon_code"]
        assert latest["staging_partition"] == f"/staging/{batch_a}/"
        assert latest["actual_count"] == 10

        # 从精确批次血缘结果关联并复核新一轮：版本、分区、对账结果全部对齐
        review2 = tl.review_lineage(batch_a, round_no=2)
        assert review2["ok"] is True
        assert review2["round"] == 2
        assert review2["review"]["lineage_version"] == "v2.0"
        assert review2["review"]["reconcile_code"] == round2_recon["recon_code"]
        assert review2["review"]["staging_file"] == round2_recon["staging_file"]
        assert review2["review"]["source_result_paired"] is True

        # 默认复核现在采用最新已确认版本；旧轮次仍可精确复核
        assert tl.review_lineage(batch_a)["round"] == 2
        assert tl.review_lineage(batch_a, round_no=1)["round"] == 1

        # 全局不变量：任一条 A 血缘的对账结果都属于 A，从未配上 B
        for rec in tl.query_lineage(batch_a, include_pending=True)["lineages"]:
            if rec["confirm_state"] == "confirmed":
                assert batch_a in rec["reconcile_code"]
                assert batch_b not in rec["reconcile_code"]


class TestManualConfirmLineage:
    """无对账自动触发时，手动挂起 → 补确认的两阶段写入。"""

    def test_begin_then_manual_confirm(self):
        suffix = uuid.uuid4().hex[:8]
        batch = f"BATCH-01-A-{suffix}"
        tl = TransferLineageService()

        pending = tl.begin_lineage_round(batch, is_balanced=True)
        assert pending["round"] == 1 and pending["confirm_state"] == "pending"
        assert tl.query_lineage(batch)["waiting_confirmation"] is True
        assert tl.review_lineage(batch)["ok"] is False

        reconcile_bundle = {
            "recon_code": f"ARRI-{batch}-manual",
            "conclusion": "已平账",
            "expected": 3,
            "actual": 3,
            "late": 0,
            "missing": 0,
            "staging_partition": f"/staging/{batch}/",
            "staging_file": f"STAG-{batch}-manual.jsonl",
        }
        confirmed = tl.confirm_lineage(batch, reconcile=reconcile_bundle)
        assert confirmed["round"] == 1
        assert confirmed["confirm_state"] == "confirmed"
        assert confirmed["reconcile_code"] == reconcile_bundle["recon_code"]

        review = tl.review_lineage(batch)
        assert review["ok"] is True
        assert review["review"]["source_result_paired"] is True
        assert review["review"]["staging_partition"] == reconcile_bundle["staging_partition"]

        # 重复确认必须显式报错，不能再制造重复版本
        with pytest.raises(ValueError):
            tl.confirm_lineage(batch, reconcile=reconcile_bundle)


class TestLegacyLineageFile:
    """旧文件中的有效血缘记录必须继续可查、可复核，且仍按完整批次精确匹配。"""

    def test_legacy_jsonl_still_queryable_with_exact_identity(self):
        suffix = uuid.uuid4().hex[:8]
        legacy_a = f"BATCH-01-A-{suffix}"
        legacy_b = f"BATCH-01-B-{suffix}"
        tl = TransferLineageService()
        legacy_file = DATA_ROOT / "lineage" / f"TRAN-{legacy_a}-legacy.jsonl"
        tl._write_jsonl(legacy_file, [{
            "lineage_code": f"TRAN-{legacy_a}-legacy",
            "transfer_batch": legacy_a,
            "source_code": "SOUR-LEGACY",
            "source_system": "legacy-src",
            "extract_window": "EXTR-LEGACY",
            "target_code": "TARG-LEGACY",
            "target_system": "legacy-tgt",
            "field_contract": "FIEL-LEGACY",
            "balanced": True,
            "reconcile_code": f"ARRI-{legacy_a}-old",
            "recorded_at": 1_700_000_000.0,
        }])

        qa = tl.query_lineage(legacy_a)
        assert qa["count"] == 1
        assert qa["lineages"][0]["transfer_batch"] == legacy_a
        assert qa["lineages"][0]["confirm_state"] == "confirmed"
        assert qa["lineages"][0]["round"] == 1

        review = tl.review_lineage(legacy_a)
        assert review["ok"] is True
        assert review["review"]["reconcile_code"] == f"ARRI-{legacy_a}-old"

        # 同前缀的另一批次查不到这条遗留记录
        qb = tl.query_lineage(legacy_b)
        assert qb["count"] == 0
        assert all(r["transfer_batch"] != legacy_a for r in qb["lineages"])
