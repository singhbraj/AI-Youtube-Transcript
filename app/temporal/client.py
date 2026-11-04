from app.configs.settings import get_settings
from temporalio.client import Client

async def get_temporal_client() -> Client:
    """Get the temporal client for the application."""
    settings = get_settings()
    return Client.connect(
        address=settings.temporal_url,
        namespace=settings.temporal_namespace
    )