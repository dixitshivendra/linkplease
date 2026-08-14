import httpx
from app.config import settings


async def send_dm(recipient_user_id: str, message: str, comment_id: str, idempotency_key: str) -> httpx.Response:
    headers = {
        "X-API-Key": settings.API_KEY,
        "Idempotency-Key": idempotency_key,
    }
    payload = {
        "recipient_user_id": recipient_user_id,
        "message": message,
        "comment_id": comment_id,
    }
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{settings.BASE_URL}/v1/dm/send",
            json=payload,
            headers=headers,
            timeout=10.0,
        )
    return response


async def check_dm_status(dm_id: str) -> httpx.Response:
    headers = {"X-API-Key": settings.API_KEY}
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{settings.BASE_URL}/v1/dm/{dm_id}",
            headers=headers,
            timeout=10.0,
        )
    return response
