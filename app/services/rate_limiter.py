import asyncio
import time


class RollingRateLimiter:
    def __init__(self, max_requests: int = 10, window_seconds: float = 60.0):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.request_times: list[float] = []
        self._lock = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            now = time.monotonic()
            cutoff = now - self.window_seconds
            self.request_times = [t for t in self.request_times if t > cutoff]

            if len(self.request_times) < self.max_requests:
                self.request_times.append(now)
                return 0

            wait_time = self.request_times[0] + self.window_seconds - now + 0.01

        await asyncio.sleep(wait_time)
        return await self.acquire()
