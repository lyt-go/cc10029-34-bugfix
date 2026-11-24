"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import lineage, resource

# 血缘专用固定路径必须排在通用动态资源路由（/api/{module}）之前，
# 否则 /api/transfer_lineage/batch/... 会被动态路径抢先匹配。
ROUTERS = [lineage, resource]
