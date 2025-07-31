import unittest
import uuid

from src.models.api_models import Info, Status1
from src.models.exceptions import AsyncHandlingError
from src.service.provision.task_repository import MemoryTaskRepository, get_task_repository


class TestMemoryTaskRepository(unittest.TestCase):
    """Unit tests for the MemoryTaskRepository class."""

    def setUp(self):
        """
        Set up a fresh repository for each test.
        Crucially, we also clear the lru_cache for the factory function
        to ensure tests are isolated and don't reuse the same instance.
        """
        self.repo = MemoryTaskRepository()
        get_task_repository.cache_clear()

    def test_create_task_success(self):
        """Test that creating a task returns a valid ID and status, and stores it correctly."""
        # Act
        task_id, status = self.repo.create_task()

        # Assert
        self.assertIsInstance(task_id, uuid.UUID)
        self.assertIsNotNone(status)
        self.assertEqual(status.status, Status1.RUNNING)
        self.assertEqual(status.result, "")

        # Verify it was stored in the internal map
        self.assertIn(task_id, self.repo.status_map)
        self.assertEqual(self.repo.status_map[task_id], status)

    def test_get_task_success(self):
        """Test retrieving an existing task by its ID."""
        # Arrange
        task_id, original_status = self.repo.create_task()

        # Act
        retrieved_status = self.repo.get_task(str(task_id))

        # Assert
        self.assertIsNotNone(retrieved_status)
        self.assertEqual(retrieved_status, original_status)

    def test_get_task_not_found(self):
        """Test that getting a non-existent task returns None."""
        # Arrange
        non_existent_id = str(uuid.uuid4())

        # Act
        retrieved_status = self.repo.get_task(non_existent_id)

        # Assert
        self.assertIsNone(retrieved_status)

    def test_update_task_success(self):
        """Test updating all fields of an existing task."""
        # Arrange
        task_id, _ = self.repo.create_task()
        new_info = Info(publicInfo={"detail": "Step 1 complete"}, privateInfo={})

        # Act
        updated_status = self.repo.update_task(
            id=str(task_id), status=Status1.COMPLETED, result="Success", info=new_info
        )

        # Assert
        self.assertEqual(updated_status.status, Status1.COMPLETED)
        self.assertEqual(updated_status.result, "Success")
        self.assertEqual(updated_status.info, new_info)

        # Verify the change is persisted
        retrieved_status = self.repo.get_task(str(task_id))
        self.assertEqual(retrieved_status.status, Status1.COMPLETED)

    def test_update_task_not_found_raises_error(self):
        """Test that updating a non-existent task raises an AsyncHandlingError."""
        # Arrange
        non_existent_id = str(uuid.uuid4())

        # Act & Assert
        with self.assertRaisesRegex(AsyncHandlingError, "Couldn't find task to update"):
            self.repo.update_task(id=non_existent_id, status=Status1.FAILED)
