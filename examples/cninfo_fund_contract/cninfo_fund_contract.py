import re
import uuid
import random
import asyncio
import urllib.parse
from pathlib import Path
from typing import Iterable

import pymongo
import aiohttp
from loguru import logger
from lxml import etree

from tspider.spiders.base import SpiderBase
from tspider.utils.proxy import ProxyManager
from tspider.db.mongo import MongoDBCollection
from tspider.utils.time import get_now


class CnInfoFundContractSpider(SpiderBase):
    def __init__(self, config_filepath: str):
        super().__init__(config_filepath)

        self.base_list_url = self.config.base_list_url
        self.timeout = aiohttp.ClientTimeout(total=self.config.timeout)
        self.proxy_manager = ProxyManager(self.config.proxy_host, self.config.proxy_port)
        self.db_manager = MongoDBCollection(
            self.config.mongo_host,
            self.config.mongo_port,
            self.config.mongo_db,
            self.config.mongo_collection
        )
        self.db_manager.collection.create_index(
            [("document_id", pymongo.TEXT)],
            unique=True,
            name="document_index"
        )

    async def get_one_url_list(self, url: str, page_num: int) -> Iterable[str]:
        headers = {
            "Host": "www.cninfo.com.cn",
            "Connection": "keep-alive",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": "http://www.cninfo.com.cn/new/fulltextSearch?notautosubmit=&keyWord=%E5%9F%BA%E9%87%91%E5%90%88%E5%90%8C",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "zh-CN,zh;q=0.9",
        }
        params = {
            "searchkey": "基金合同",
            "sdate": "",
            "edate": "",
            "isfulltext": "false",
            "sortName": "pubdate",
            "sortType": "desc",
            "pageNum": str(page_num),
        }
        proxy = await self.proxy_manager.get_proxy()
        for _ in range(self.config.max_request_attempt):
            try:
                logger.info(f"Crawling {url}, page: {page_num}, proxy: {proxy}")
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        url,
                        headers=headers,
                        params=params,
                        proxy=proxy,
                        timeout=self.timeout
                    ) as response:
                        response.raise_for_status()
                        result = await response.json()
                        logger.info(f"Successfully Crawled {url}, page: {page_num}, proxy: {proxy}")
                        inses = result.get('announcements')
                        if inses is None:
                            inses = []
                        return inses
            except aiohttp.ClientResponseError:
                logger.warning(f"ClientResponseError Page Num: {page_num}")
                await asyncio.sleep(self.config.sleep + random.random())
            except aiohttp.client_exceptions.ClientProxyConnectionError:
                logger.warning(f"Proxy invalid: {proxy}")
                await self.proxy_manager.delete_proxy(proxy)
                proxy = await self.proxy_manager.get_proxy()
            except aiohttp.client_exceptions.ClientOSError:
                logger.warning(f"ClientOSError Page Num: {page_num}")
                await asyncio.sleep(self.config.sleep + random.random())
            except aiohttp.client_exceptions.ServerDisconnectedError:
                logger.warning(f"ClientOSError Page Num: {page_num}")
                await asyncio.sleep(self.config.sleep + random.random())
            except asyncio.TimeoutError:
                logger.warning(f"TimeoutError Page Num: {page_num}")
                await asyncio.sleep(self.config.sleep + random.random())
            except aiohttp.client_exceptions.ClientPayloadError:
                logger.warning(f"ClientPayloadError Page Num: {page_num}")
                await asyncio.sleep(self.config.sleep + random.random())

        # logger.warning(f"Proxy invalid: {proxy}")
        # await self.proxy_manager.delete_proxy(proxy)
        return []

    async def get_url_list(self, *args, **kwargs) -> Iterable[str]:
        # url_list only holds documentId here
        url_list = []
        start_page_num = self.config.start_page_num
        tot_page_num = self.config.tot_page_num
        logger.info(f"Total page number: {tot_page_num}")

        tasks = []
        page_num_list = list(range(start_page_num, tot_page_num + 1))
        for page_num in page_num_list:
            task = asyncio.create_task(self.get_one_url_list(self.base_list_url, page_num))
            tasks.append(task)
        results = await asyncio.gather(*tasks)
        for page_num, list_ in zip(page_num_list, results):
            logger.info(f"page: {page_num}, result: {len(list_)}")
            for ins in list_:
                url_list.append(ins)
                document_id = ins['announcementId']
                title = ins['announcementTitle'].replace('<em>', '').replace('</em>', '')
                download_url = urllib.parse.urljoin(self.config.download_base_url, ins['adjunctUrl'])
                self.db_manager.insert_one({
                    "document_id": document_id,
                    "title": title,
                    "insert_time": get_now(),
                    "download_url": download_url,
                    "downloaded": False,
                    "download_time": None,
                    "filename": None,
                    "filepath": None,
                })

        return url_list

    async def craw_one(self, instance: dict) -> object:
        headers = {
            "Host": "static.cninfo.com.cn",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "Referer": "http://www.cninfo.com.cn/",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "zh-CN,zh;q=0.9",
        }
        document_id = instance['announcementId']
        download_suffix = instance['adjunctUrl']
        title = instance['announcementTitle'].replace('<em>', '').replace('</em>', '')
        filetype = download_suffix.split('.')[-1]
        download_url = urllib.parse.urljoin(self.config.download_base_url, download_suffix)
        while True:
            try:
                proxy = await self.proxy_manager.get_proxy()
                break
            except:
                await asyncio.sleep(random.random() * 5)
                continue
        # in case of competing with each requests
        # await asyncio.sleep(1.5 + random.random() * 10)
        for _ in range(self.config.max_request_attempt):
            try:
                logger.info(f"Requesting: {title}")
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        download_url,
                        headers=headers,
                        proxy=proxy,
                        timeout=self.timeout
                    ) as response:
                        response.raise_for_status()
                        save_status = self.save_one(title + f'.{filetype}', await response.read())
                        if save_status is not None:
                            logger.info(f"Successfully Downloading: {title}")
                            self.db_manager.update_one(
                                {"document_id": document_id},
                                {
                                    "$set": {
                                        "download_time": get_now(),
                                        "downloaded": True,
                                        "filename": save_status[0],
                                        "filepath": str(save_status[1].absolute())
                                    }
                                }
                            )
                        else:
                            raise asyncio.TimeoutError("Error Downloading")
            except aiohttp.client_exceptions.ServerDisconnectedError:
                logger.error(f"ServerDisconnectedError {download_url}")
                await asyncio.sleep(self.config.sleep + random.random())
            except aiohttp.client_exceptions.ClientPayloadError:
                logger.error(f"ClientPayloadError {download_url}")
                await asyncio.sleep(self.config.sleep + random.random())
            except aiohttp.ClientResponseError:
                logger.warning(f"ClientResponseError: {document_id}")
                await asyncio.sleep(self.config.sleep + random.random())
            except aiohttp.client_exceptions.ClientProxyConnectionError:
                logger.warning(f"Proxy invalid: {proxy}")
                await self.proxy_manager.delete_proxy(proxy)
                proxy = await self.proxy_manager.get_proxy()
            except aiohttp.client_exceptions.ClientOSError:
                logger.warning(f"ClientOSError Downloading {document_id} Failed")
                await asyncio.sleep(self.config.sleep + random.random())
            except aiohttp.client_exceptions.ServerDisconnectedError:
                logger.warning(f"ServerDisconnectedError Downloading {document_id} Failed")
                await asyncio.sleep(self.config.sleep + random.random())
            except asyncio.TimeoutError:
                logger.warning(f"TimeoutError Downloading {document_id} Failed")
                await asyncio.sleep(self.config.sleep + random.random())

    def save_one(self, title: str, content: bytes):
        try:
            filename = title
            if len(filename.encode()) > 30:
                filename = filename[-30:]
            filepath = Path.joinpath(self.output_dir, filename)
            logger.info(f"Save into: {filepath.absolute()}")
            with filepath.open('wb') as fout:
                fout.write(content)
            return (filename, filepath)
        except Exception as err:
            logger.error(f"{str(err)}")
            return None
