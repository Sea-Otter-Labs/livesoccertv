from typing import List, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, desc
from repo.base_repo import BaseRepository
from models.alert_log import AlertLog


class AlertLogRepository(BaseRepository[AlertLog]):
    """告警记录数据访问层"""
    
    def __init__(self, session: AsyncSession):
        super().__init__(session, AlertLog)
    
    async def get_unresolved(
        self,
        alert_type: Optional[str] = None,
        severity: Optional[str] = None
    ) -> List[AlertLog]:
        """获取未解决的告警"""
        query = select(AlertLog).where(AlertLog.is_resolved == False)
        
        if alert_type:
            query = query.where(AlertLog.alert_type == alert_type)
        
        if severity:
            query = query.where(AlertLog.severity == severity)
        
        result = await self.session.execute(
            query.order_by(desc(AlertLog.created_at))
        )
        return result.scalars().all()
    
    async def get_by_type(
        self,
        alert_type: str,
        is_resolved: Optional[bool] = None
    ) -> List[AlertLog]:
        """根据告警类型获取记录"""
        query = select(AlertLog).where(AlertLog.alert_type == alert_type)
        
        if is_resolved is not None:
            query = query.where(AlertLog.is_resolved == is_resolved)
        
        result = await self.session.execute(
            query.order_by(desc(AlertLog.created_at))
        )
        return result.scalars().all()
    
    async def get_by_league(
        self,
        league_id: int,
        is_resolved: Optional[bool] = None
    ) -> List[AlertLog]:
        """根据联赛ID获取告警"""
        query = select(AlertLog).where(AlertLog.league_id == league_id)
        
        if is_resolved is not None:
            query = query.where(AlertLog.is_resolved == is_resolved)
        
        result = await self.session.execute(
            query.order_by(desc(AlertLog.created_at))
        )
        return result.scalars().all()
    
    async def get_by_fixture(
        self,
        fixture_id: int
    ) -> List[AlertLog]:
        """根据fixture_id获取告警"""
        result = await self.session.execute(
            select(AlertLog)
            .where(AlertLog.fixture_id == fixture_id)
            .order_by(desc(AlertLog.created_at))
        )
        return result.scalars().all()
    
    async def resolve_alert(
        self,
        alert_id: int,
        resolved_by: str,
        resolution_notes: Optional[str] = None
    ) -> Optional[AlertLog]:
        """解决告警"""
        from datetime import datetime
        
        update_data = {
            'is_resolved': True,
            'resolved_at': datetime.now(),
            'resolved_by': resolved_by
        }
        
        if resolution_notes:
            update_data['resolution_notes'] = resolution_notes
        
        return await self.update(alert_id, update_data)
    
    async def mark_as_notified(
        self,
        alert_id: int,
        notification_response: Optional[str] = None
    ) -> Optional[AlertLog]:
        """标记告警为已通知"""
        from datetime import datetime
        
        update_data = {'notified_at': datetime.now()}
        
        if notification_response:
            update_data['notification_response'] = notification_response
        
        return await self.update(alert_id, update_data)
    
    async def exists_similar_alert(
        self,
        alert_type: str,
        fixture_id: Optional[int] = None,
        hours_window: int = 24
    ) -> bool:
        """检查是否存在相似告警（去重）"""
        from datetime import datetime, timedelta
        from sqlalchemy import text
        
        cutoff_time = datetime.now() - timedelta(hours=hours_window)
        
        query = select(AlertLog).where(
            and_(
                AlertLog.alert_type == alert_type,
                AlertLog.created_at >= cutoff_time
            )
        )
        
        if fixture_id:
            query = query.where(AlertLog.fixture_id == fixture_id)
        
        result = await self.session.execute(query)
        return result.scalar_one_or_none() is not None
