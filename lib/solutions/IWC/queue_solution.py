from dataclasses import dataclass
from datetime import datetime
from enum import IntEnum

# RESOLVED on deploy
from solutions.IWC.task_types import TaskSubmission, TaskDispatch


class Priority(IntEnum):
    """Represents the queue ordering tiers observed in the system."""
    HIGH = 1
    NORMAL = 2


@dataclass
class Provider:
    name: str
    base_url: str
    depends_on: list[str]


MAX_TIMESTAMP = datetime.max.replace(tzinfo=None)

COMPANIES_HOUSE_PROVIDER = Provider(
    name="companies_house", base_url="https://fake.companieshouse.co.uk", depends_on=[]
)


CREDIT_CHECK_PROVIDER = Provider(
    name="credit_check",
    base_url="https://fake.creditcheck.co.uk",
    depends_on=["companies_house"],
)


BANK_STATEMENTS_PROVIDER = Provider(
    name="bank_statements", base_url="https://fake.bankstatements.co.uk", depends_on=[]
)

ID_VERIFICATION_PROVIDER = Provider(
    name="id_verification", base_url="https://fake.idv.co.uk", depends_on=[]
)


REGISTERED_PROVIDERS: list[Provider] = [
    BANK_STATEMENTS_PROVIDER,
    COMPANIES_HOUSE_PROVIDER,
    CREDIT_CHECK_PROVIDER,
    ID_VERIFICATION_PROVIDER,
]


class Queue:
    def __init__(self):
        self._queue = []

    def _collect_dependencies(self, task: TaskSubmission) -> list[TaskSubmission]:
        provider = next((p for p in REGISTERED_PROVIDERS if p.name == task.provider), None)
        if provider is None:
            return []

        tasks: list[TaskSubmission] = []
        for dependency in provider.depends_on:
            dependency_task = TaskSubmission(
                provider=dependency,
                user_id=task.user_id,
                timestamp=task.timestamp,
            )
            tasks.extend(self._collect_dependencies(dependency_task))
            tasks.append(dependency_task)
        return tasks

    @staticmethod
    def _priority_for_task(task):
        metadata = task.metadata
        raw_priority = metadata.get("priority", Priority.NORMAL)
        try:
            return Priority(raw_priority)
        except (TypeError, ValueError):
            return Priority.NORMAL

    @staticmethod
    def _earliest_group_timestamp_for_task(task):
        metadata = task.metadata
        return metadata.get("group_earliest_timestamp", MAX_TIMESTAMP)

    @staticmethod
    def _timestamp_for_task(task):
        timestamp = task.timestamp
        if isinstance(timestamp, datetime):
            return timestamp.replace(tzinfo=None)
        if isinstance(timestamp, str):
            return datetime.fromisoformat(timestamp).replace(tzinfo=None)
        return timestamp

    @staticmethod
    def _is_bank_statements(task) -> int:
        return 1 if task.provider == "bank_statements" else 0

    def _compute_user_stats(self) -> tuple[dict[int, int], dict[int, object]]:
        task_count: dict[int, int] = {}
        priority_timestamps: dict[int, object] = {}
        for task in self._queue:
            uid = task.user_id
            task_count[uid] = task_count.get(uid, 0) + 1
            if uid not in priority_timestamps or task.timestamp < priority_timestamps[uid]:
                priority_timestamps[uid] = task.timestamp
        return task_count, priority_timestamps

    def _apply_priorities(
        self,
        task_count: dict[int, int],
        priority_timestamps: dict[int, object],
    ) -> None:
        for task in self._queue:
            metadata = task.metadata
            current_earliest = metadata.get("group_earliest_timestamp", MAX_TIMESTAMP)
            raw_priority = metadata.get("priority")
            try:
                priority_level = Priority(raw_priority)
            except (TypeError, ValueError):
                priority_level = None

            if priority_level is None or priority_level == Priority.NORMAL:
                metadata["group_earliest_timestamp"] = MAX_TIMESTAMP
                if task_count[task.user_id] >= 3:
                    metadata["group_earliest_timestamp"] = priority_timestamps[task.user_id]
                    metadata["priority"] = Priority.HIGH
                else:
                    metadata["priority"] = Priority.NORMAL
            else:
                metadata["group_earliest_timestamp"] = current_earliest
                metadata["priority"] = priority_level

    def _sort_key(self, task):
        return (
            self._priority_for_task(task),
            self._earliest_group_timestamp_for_task(task),
            self._is_bank_statements(task),
            self._timestamp_for_task(task),
        )

    def enqueue(self, item: TaskSubmission) -> int:
        tasks = [*self._collect_dependencies(item), item]

        for task in tasks:
            metadata = task.metadata
            metadata.setdefault("priority", Priority.NORMAL)
            metadata.setdefault("group_earliest_timestamp", MAX_TIMESTAMP)

            existing = next(
                (t for t in self._queue
                 if t.user_id == task.user_id and t.provider == task.provider),
                None,
            )
            if existing is not None:
                if self._timestamp_for_task(task) < self._timestamp_for_task(existing):
                    existing.timestamp = task.timestamp
                    existing.metadata = task.metadata
            else:
                self._queue.append(task)

        return self.size

    def _find_oldest_bank_statements_candidate(self) -> TaskSubmission | None:
        max_ts = max(self._timestamp_for_task(t) for t in self._queue)
        cand_task = None
        for task in self._queue:
            if (task.provider == "bank_statements" and (max_ts - self._timestamp_for_task(task)).total_seconds() >= 300):
                if (cand_task is None or self._timestamp_for_task(task) < self._timestamp_for_task(cand_task)):
                    cand_task = task
        return cand_task

    def _find_older_task(self, task) -> TaskSubmission | None:
        timestamp = self._timestamp_for_task(task)
        older_tasks = [
            t for t in self._queue
            if self._timestamp_for_task(t) < timestamp and t is not task
        ]
        if older_tasks:
            older_tasks.sort(key=self._sort_key)
            return older_tasks[0]
        return None

    def dequeue(self) -> TaskDispatch | None:
        if self.size == 0:
            return None

        task_count, priority_timestamps = self._compute_user_stats()
        self._apply_priorities(task_count, priority_timestamps)

        bank_task = self._find_oldest_bank_statements_candidate()

        if bank_task is not None:
            older_task = self._find_older_task(bank_task)
            if older_task is not None:
                task = older_task
            else:
                task = bank_task
            self._queue.remove(task)
            return TaskDispatch(provider=task.provider, user_id=task.user_id)

        # No bank candidates — normal sort and dequeue
        self._queue.sort(key=self._sort_key)

        task = self._queue.pop(0)
        return TaskDispatch(
            provider=task.provider,
            user_id=task.user_id,
        )

    @property
    def size(self) -> int:
        return len(self._queue)

    @property
    def age(self) -> int:
        if len(self._queue) < 2:
            return 0
        timestamps = [self._timestamp_for_task(task) for task in self._queue]
        return int((max(timestamps) - min(timestamps)).total_seconds())

    def purge(self) -> bool:
        self._queue.clear()
        return True

"""
===================================================================================================

The following code is only to visualise the final usecase.
No changes are needed past this point.

To test the correct behaviour of the queue system, import the `Queue` class directly in your tests.

===================================================================================================

```python
import asyncio
import logging
from fastapi import FastAPI
from contextlib import asynccontextmanager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(queue_worker())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        logger.info("Queue worker cancelled on shutdown.")


app = FastAPI(lifespan=lifespan)
queue = Queue()


@app.get("/")
def read_root():
    return {
        "registered_providers": [
            {"name": p.name, "base_url": p.base_url} for p in registered_providers
        ]
    }


class DataRequest(BaseModel):
    user_id: int
    providers: list[str]


@app.post("/fetch_customer_data")
def fetch_customer_data(data: DataRequest):
    provider_names = [p.name for p in registered_providers]

    for provider in data.providers:
        if provider not in provider_names:
            logger.warning(f"Provider {provider} doesn't exists. Skipping")
            continue

        queue.enqueue(
            TaskSubmission(
                provider=provider,
                user_id=data.user_id,
                timestamp=datetime.now(),
            )
        )

    return {"status": f"{len(data.providers)} Task(s) added to queue"}


async def queue_worker():
    while True:
        if queue.size == 0:
            await asyncio.sleep(1)
            continue

        task = queue.dequeue()
        if not task:
            continue

        logger.info(f"Processing task: {task}")
        await asyncio.sleep(2)
        logger.info(f"Finished task: {task}")
```
"""

