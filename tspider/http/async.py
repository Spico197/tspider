import aiohttp


async def async_request(method: str, url: str, **kwargs):
    async with aiohttp.ClientSession() as session:
        func = getattr(session, method.lower())
        async with func(url, **kwargs) as response:
            return response
