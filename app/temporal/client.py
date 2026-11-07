from app.configs.settings import get_settings
from temporalio.client import Client

async def get_temporal_client() -> Client:
    """Get the temporal client for the application."""
    settings = get_settings()
    return await Client.connect(
        settings.temporal_host,
        namespace=settings.temporal_namespace,
    )