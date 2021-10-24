import asyncio
import aiohttp


class ProxyManager:
    def __init__(self, host: str, port: str) -> None:
        self.host = host
        self.port = port

    async def get_proxy(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(f'http://{self.host}:{self.port}/get/') as response:
                result = await response.json()
                proxy = f"http://{result.get('proxy')}"
                return proxy

    async def delete_proxy(self, proxy: str):
        if proxy.startswith('http://'):
            proxy = proxy[7:]
        elif proxy.startswith('https://'):
            proxy = proxy[8:]
        async with aiohttp.ClientSession() as session:
            async with session.get(f"http://{self.host}:{self.port}/delete/?proxy={proxy}") as response:
                result = await response.json()
                return result


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    pm = ProxyManager("localhost", "26888")
    proxy = asyncio.run(pm.get_proxy())
    print(proxy)
    result = asyncio.run(pm.delete_proxy(proxy))
    print(result)
