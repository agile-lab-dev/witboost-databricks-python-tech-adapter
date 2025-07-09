from abc import ABC, abstractmethod

from databricks.sdk.service.catalog import SecurableType


class DBObject(ABC):
    """Abstract base class for a Databricks securable object."""

    @property
    @abstractmethod
    def fully_qualified_name(self) -> str:
        pass

    @property
    @abstractmethod
    def securable_type(self) -> SecurableType:
        pass


class Catalog(DBObject):
    def __init__(self, name: str):
        self.name = name

    @property
    def fully_qualified_name(self) -> str:
        return self.name

    @property
    def securable_type(self) -> SecurableType:
        return SecurableType.CATALOG


class Schema(DBObject):
    def __init__(self, catalog_name: str, schema_name: str):
        self.catalog_name = catalog_name
        self.schema_name = schema_name

    @property
    def fully_qualified_name(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}"

    @property
    def securable_type(self) -> SecurableType:
        return SecurableType.SCHEMA


class View(DBObject):
    def __init__(self, catalog_name: str, schema_name: str, view_name: str):
        self.catalog_name = catalog_name
        self.schema_name = schema_name
        self.view_name = view_name

    @property
    def fully_qualified_name(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.view_name}"

    @property
    def securable_type(self) -> SecurableType:
        # Views and Tables are both secured as TABLE type in UC
        return SecurableType.TABLE
