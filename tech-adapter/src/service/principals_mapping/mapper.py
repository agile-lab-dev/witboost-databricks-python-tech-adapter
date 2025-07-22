from abc import ABC, abstractmethod
from typing import Mapping, Set, Union

from src.models.databricks.exceptions import MapperError


class Mapper(ABC):
    @abstractmethod
    def map(self, subjects: Set[str]) -> Mapping[str, Union[str, MapperError]]:
        """
        Defines the main mapping logic for a set of subjects.

        Args:
            subjects: A set of strings representing the subjects to be mapped
                      (e.g., WitBoost users and groups).

        Returns:
            A dictionary where:
            - Keys are the original subject strings from the input set.
            - Values are either the successfully mapped principal (str) on success,
              or an Exception object on failure for that specific subject.
        """
        pass


class AsyncMapper(ABC):
    @abstractmethod
    async def map(self, subjects: Set[str]) -> Mapping[str, Union[str, MapperError]]:
        """
        Defines the main mapping logic for a set of subjects.

        Args:
            subjects: A set of strings representing the subjects to be mapped
                      (e.g., WitBoost users and groups).

        Returns:
            A dictionary where:
            - Keys are the original subject strings from the input set.
            - Values are either the successfully mapped principal (str) on success,
              or an Exception object on failure for that specific subject.
        """
        pass
