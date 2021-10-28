import os
import asyncio
from pathlib import Path
from typing import Iterable

from loguru import logger
from omegaconf.omegaconf import OmegaConf


class SpiderBase(object):
    def __init__(self, config_filepath: str):
        self.config = OmegaConf.load(config_filepath)
        self.name = self.config.name
        self.stop_signal = False
        self.output_dir = Path(self.config.output_dir, self.config.name)
        if not self.output_dir.exists() or not self.output_dir.is_dir():
            self.output_dir.mkdir(parents=True)
        logger.add(self.output_dir.joinpath('log.log'))

    def start(self, *args, **kwargs):
        loop = asyncio.get_event_loop()
        try:
            if self.stop_signal:
                raise InterruptedError

            url_list = loop.run_until_complete(self.get_url_list(*args, **kwargs))

            # with self.output_dir.joinpath('url_set.txt').open('wt') as fout:
            #     for url in url_set:
            #         fout.write(f"{url}\n")
            #         fout.flush()

            # url_set = set()
            # with self.output_dir.joinpath('url_list.txt').open('rt') as fin:
            #     for url in fin:
            #         url_set.add(url.strip())

            if self.stop_signal:
                raise InterruptedError

            logger.info(f"url list len: {len(url_list)}")

            """sync"""
            # for url in url_list:
            #     loop.run_until_complete(self.craw_one(url))

            """async"""
            loop.run_until_complete(self.craw_urls(url_list))

        finally:
            loop.close()

    def stop(self):
        self.stop_signal = True

    async def craw_urls(self, url_list):
        tasks = []
        for url in url_list:
            if self.stop_signal:
                raise InterruptedError
            task = asyncio.create_task(self.craw_one(url))
            tasks.append(task)
        results = await asyncio.gather(*tasks)
        return results

    async def get_url_list(self, *args, **kwargs) -> Iterable[str]:
        raise NotImplementedError

    async def craw_one(self, url: str) -> object:
        raise NotImplementedError
