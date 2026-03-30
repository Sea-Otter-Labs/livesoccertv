from typing import List, Optional
from datetime import date
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, desc
from repo.base_repo import BaseRepository
from models.crawl_task_status import CrawlTaskStatus


class CrawlTaskStatusRepository(BaseRepository[CrawlTaskStatus]):
    """抓取任务状态数据访问层"""

    def __init__(self, session: AsyncSession):
        super().__init__(session, CrawlTaskStatus)
    
    async def get_by_batch_and_league(
        self,
        batch_id: str,
        league_config_id: int
    ) -> Optional[CrawlTaskStatus]:
        """根据批次ID和联赛配置ID获取任务状态"""
        result = await self.session.execute(
            select(CrawlTaskStatus)
            .where(
                and_(
                    CrawlTaskStatus.crawl_batch_id == batch_id,
                    CrawlTaskStatus.league_config_id == league_config_id
                )
            )
        )
        return result.scalar_one_or_none()
    
    async def get_by_batch_id(self, batch_id: str) -> List[CrawlTaskStatus]:
        """根据批次ID获取所有任务状态"""
        result = await self.session.execute(
            select(CrawlTaskStatus)
            .where(CrawlTaskStatus.crawl_batch_id == batch_id)
            .order_by(desc(CrawlTaskStatus.created_at))
        )
        return result.scalars().all()
    
    async def get_by_league_config(
        self,
        league_config_id: int,
        limit: int = 10
    ) -> List[CrawlTaskStatus]:
        """根据联赛配置ID获取任务历史"""
        result = await self.session.execute(
            select(CrawlTaskStatus)
            .where(CrawlTaskStatus.league_config_id == league_config_id)
            .order_by(desc(CrawlTaskStatus.created_at))
            .limit(limit)
        )
        return result.scalars().all()
    
    async def get_by_status(
        self,
        status: str
    ) -> List[CrawlTaskStatus]:
        """根据状态获取任务"""
        result = await self.session.execute(
            select(CrawlTaskStatus)
            .where(CrawlTaskStatus.status == status)
            .order_by(desc(CrawlTaskStatus.created_at))
        )
        return result.scalars().all()
    
    async def get_by_phase(
        self,
        phase: str
    ) -> List[CrawlTaskStatus]:
        """根据阶段获取任务"""
        result = await self.session.execute(
            select(CrawlTaskStatus)
            .where(CrawlTaskStatus.task_phase == phase)
            .order_by(desc(CrawlTaskStatus.created_at))
        )
        return result.scalars().all()
    
    # async def get_paused_tasks(
    #     self
    # ) -> List[CrawlTaskStatus]:
    #     """获取暂停的任务（等待人工处理）"""
    #     result = await self.session.execute(
    #         select(CrawlTaskStatus)
    #         .where(
    #             and_(
    #                 CrawlTaskStatus.task_phase == TaskPhase.PAUSED_CAPTCHA,
    #                 CrawlTaskStatus.status == TaskStatus.PAUSED
    #             )
    #         )
    #         .order_by(desc(CrawlTaskStatus.captcha_detected_at))
    #     )
    #     return result.scalars().all()
    
    async def get_running_tasks(
        self
    ) -> List[CrawlTaskStatus]:
        """获取正在运行的任务"""
        result = await self.session.execute(
            select(CrawlTaskStatus)
            .where(CrawlTaskStatus.status == 'running')
        )
        return result.scalars().all()
    
    async def update_phase(
        self,
        task_id: int,
        phase: str
    ) -> Optional[CrawlTaskStatus]:
        """更新任务阶段"""
        return await self.update(task_id, {'task_phase': phase})
    
    async def update_status(
        self,
        task_id: int,
        status: str,
        error_message: Optional[str] = None
    ) -> Optional[CrawlTaskStatus]:
        """更新任务状态"""
        update_data = {'status': status}

        if error_message:
            update_data['error_message'] = error_message

        return await self.update(task_id, update_data)
    
    async def update_pagination(
        self,
        task_id: int,
        cursor: str,
        direction: str
    ) -> Optional[CrawlTaskStatus]:
        """更新分页信息"""
        return await self.update(task_id, {
            'current_pagination_cursor': cursor,
            'pagination_direction': direction
        })
    
    async def update_match_counts(
        self,
        task_id: int,
        crawled: Optional[int] = None,
        matched: Optional[int] = None
    ) -> Optional[CrawlTaskStatus]:
        """更新比赛计数"""
        update_data = {}
        
        if crawled is not None:
            update_data['matches_crawled'] = crawled
        
        if matched is not None:
            update_data['matches_matched'] = matched
        
        if update_data:
            return await self.update(task_id, update_data)
        return None
    
    # async def mark_captcha_detected(
    #     self,
    #     task_id: int
    # ) -> Optional[CrawlTaskStatus]:
    #     """标记检测到验证码"""
    #     from datetime import datetime
        
    #     return await self.update(task_id, {
    #         'task_phase': TaskPhase.PAUSED_CAPTCHA,
    #         'status': TaskStatus.PAUSED,
    #         'captcha_detected_at': datetime.now()
    #     })
    
    # async def mark_captcha_resolved(
    #     self,
    #     task_id: int
    # ) -> Optional[CrawlTaskStatus]:
    #     """标记验证码已解决"""
    #     from datetime import datetime
        
    #     return await self.update(task_id, {
    #         'status': TaskStatus.RUNNING,
    #         'captcha_resolved_at': datetime.now()
    #     })
    
    async def complete_task(
        self,
        task_id: int
    ) -> Optional[CrawlTaskStatus]:
        """完成任务"""
        from datetime import datetime

        return await self.update(task_id, {
            'task_phase': 'completed',
            'status': 'success',
            'completed_at': datetime.now()
        })
    
    async def fail_task(
        self,
        task_id: int,
        error_message: str
    ) -> Optional[CrawlTaskStatus]:
        """标记任务失败"""
        from datetime import datetime

        return await self.update(task_id, {
            'task_phase': 'failed',
            'status': 'failed',
            'error_message': error_message,
            'completed_at': datetime.now()
        })
    
    async def get_or_create(
        self,
        batch_id: str,
        league_config_id: int
    ) -> CrawlTaskStatus:
        """获取或创建任务状态"""
        existing = await self.get_by_batch_and_league(batch_id, league_config_id)

        if existing:
            return existing

        return await self.create({
            'crawl_batch_id': batch_id,
            'league_config_id': league_config_id,
            'task_phase': 'init',
            'status': 'pending'
        })
