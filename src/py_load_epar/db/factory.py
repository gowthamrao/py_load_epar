from py_load_epar.config import Settings
from py_load_epar.db.interfaces import IDatabaseAdapter
from py_load_epar.db.postgres import PostgresAdapter


class DatabaseAdapterFactory:
    """
    Factory for creating database adapter instances based on configuration.
    """

    _adapters = {
        "postgresql": PostgresAdapter,
    }

    @staticmethod
    def get_adapter(settings: Settings) -> IDatabaseAdapter:
        """
        Gets the appropriate database adapter based on the configured type.

        Args:
            settings: The application settings object.

        Returns:
            An instance of a class that implements the IDatabaseAdapter interface.

        Raises:
            NotImplementedError: If the configured database type is not supported.
        """
        db_type = settings.db.type.lower()
        adapter_class = DatabaseAdapterFactory._adapters.get(db_type)

        if not adapter_class:
            raise NotImplementedError(
                f"No adapter implemented for database type: '{db_type}'"
            )

        return adapter_class(settings.db)


# For convenience, a direct function can be exposed.
get_db_adapter = DatabaseAdapterFactory.get_adapter
