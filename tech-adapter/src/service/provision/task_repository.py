import threading
import uuid
from functools import lru_cache
from typing import Optional, Tuple
from uuid import UUID

from src.models.api_models import Info, ProvisioningStatus, Status1
from src.models.exceptions import AsyncHandlingError


class MemoryTaskRepository:
    """
    Class to manage async provisioning status. Access to the provisioning status map is governed by a lock
    """

    _status_map_lock = threading.Lock()

    def __init__(self):
        self.status_map: dict[UUID, ProvisioningStatus] = {}

    def create_task(self) -> Tuple[UUID, ProvisioningStatus]:
        with self._status_map_lock:
            id = uuid.uuid4()
            status = ProvisioningStatus(status=Status1.RUNNING, result="")
            self.status_map[id] = status
            return id, status

    def get_task(self, id: str) -> Optional[ProvisioningStatus]:
        with self._status_map_lock:
            uuid = UUID(id)
            return self.status_map.get(uuid)

    def update_task(
        self, *, id: str, status: Optional[Status1] = None, result: Optional[str] = None, info: Optional[Info] = None
    ) -> ProvisioningStatus:
        with self._status_map_lock:
            uuid = UUID(id)
            stored_status = self.status_map.get(uuid)
            if not stored_status:
                raise AsyncHandlingError(
                    [
                        "Couldn't find task to update. This is due to bad management of async "
                        "operations. Please contact the platform team for assistance"
                    ]
                )
            if status:
                stored_status.status = status
            if result:
                stored_status.result = result
            if info:
                stored_status.info = info
            self.status_map[uuid] = stored_status
            return stored_status


@lru_cache
def get_task_repository():
    return MemoryTaskRepository()
