"""传输血缘专用接口：精确批次血缘明细查询与血缘复核。

与通用资源路由（/api/{module}）分离，承载血缘工作台特有的按完整批次编号
精确查询、轮次版本选择与复核动作，必须在通用动态路由之前注册。
"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from app.services.transfer_lineage import TransferLineageService

router = APIRouter(prefix="/api/transfer_lineage", tags=["传输血缘"])


@router.get("/batch/{batch_code}/query")
def query_batch_lineage(
    batch_code: str,
    include_pending: bool = Query(default=False, description="是否包含未确认的半条血缘"),
) -> dict:
    """按完整批次编号查询精确血缘明细，同前缀批次互不串账。"""
    service = TransferLineageService()
    return service.query_lineage(batch_code, include_pending=include_pending)


@router.post("/batch/{batch_code}/review")
def review_batch_lineage(
    batch_code: str,
    round: int | None = Query(default=None, description="指定复核轮次，缺省复核最近已确认版本"),
) -> dict:
    """基于精确批次血缘结果发起复核，仅采用完整已确认版本。"""
    service = TransferLineageService()
    try:
        return service.review_lineage(batch_code, round)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
