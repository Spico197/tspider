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

from tspider.spiders.base import SpiderBase
from tspider.utils.proxy import ProxyManager
from tspider.db.mongo import MongoDBCollection
from tspider.utils.time import get_now


class CebPubServiceSpider(SpiderBase):
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
        post_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        }
        payload = {
            "pageNo": str(page_num),
            "keyWord": "",
            "row": "10",
            "page": "10",
        }
        proxy = await self.proxy_manager.get_proxy()
        for _ in range(self.config.max_request_attempt):
            try:
                logger.info(f"Crawling {url}, page: {page_num}, proxy: {proxy}")
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        url,
                        headers=post_headers,
                        data=payload,
                        proxy=proxy,
                        timeout=self.timeout
                    ) as response:
                        if response.status != 200:
                            return {"object": {"page": {"totalPage": 0}, "list": []}}
                        return await response.json()
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
                logger.error(f"ClientPayloadError Page Num: {page_num}")
                await asyncio.sleep(self.config.sleep + random.random())

        return {"object": {"page": {"totalPage": 0}, "list": []}}

    async def get_url_list(self, *args, **kwargs) -> Iterable[str]:
        # url_set only holds documentId here
        url_set = set()
        start_page_num = self.config.start_page_num
        tot_page_num = self.config.tot_page_num
        logger.info(f"Total page number: {tot_page_num}")

        tasks = []
        for page_num in range(start_page_num, tot_page_num + 1):
            task = asyncio.create_task(self.get_one_url_list(self.base_list_url, page_num))
            tasks.append(task)
        results = await asyncio.gather(*tasks)
        for result in results:
            result = result.get('object')
            if result is not None:
                page_ = result.get('page')
                list_ = result.get('list')
                logger.info(f"page: {page_}, result: {len(list_)}")
                if list_ is not None:
                    for ins in list_:
                        document_id = ins.get('documentid')
                        if document_id is not None:
                            url_set.add(document_id)
                            self.db_manager.insert_one({
                                "document_id": document_id,
                                "guid": None,
                                "download_url": None,
                                "downloaded": False,
                                "filename": None,
                                "filepath": None,
                                "raw_content_disposition": None,
                                "insert_time": get_now(),
                                "insert_guid_time": None,
                                "download_time": None,
                            })

        return url_set

    async def craw_one(self, document_id: str) -> object:
        guid = await self.get_guid(document_id)
        if guid is not None:
            guid = guid.get('object')
            if guid is not None:
                guid = guid.get("newFileId1")

        if guid is not None:
            download_url = self.config.download_api + f"?guid={guid}&documentId={document_id}&type=yes"
            with self.output_dir.joinpath('download_urls.txt').open('a') as fout:
                fout.write(f"{download_url}\n")
                fout.flush()
            self.db_manager.update_one(
                {"document_id": document_id},
                {"$set": {
                    "guid": guid,
                    "download_url": download_url,
                    "insert_guid_time": get_now()
                }}
            )

            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36",
                "Connection": "keep-alive",
            }
            params = {
                "guid": guid,
                "documentId": document_id,
                "type": "yes"
            }
            proxy = await self.proxy_manager.get_proxy()
            # in case of competing with each requests
            await asyncio.sleep(1.5 + random.random() * 10)
            for _ in range(self.config.max_request_attempt):
                try:
                    logger.info(f"Downloading: {document_id}")
                    async with aiohttp.ClientSession() as session:
                        async with session.post(
                            self.config.download_api,
                            headers=headers,
                            params=params,
                            proxy=proxy,
                            timeout=self.timeout
                        ) as response:
                            if response.status != 200:
                                return
                            save_status = await self.save_one(response)
                            if save_status is not None:
                                self.db_manager.update_one(
                                    {"document_id": document_id},
                                    {
                                        "$set": {
                                            "download_time": get_now(),
                                            "downloaded": True,
                                            "filename": save_status[0],
                                            "filepath": str(save_status[1].absolute()),
                                            "raw_content_disposition": response.headers.get('Content-Disposition'),
                                        }
                                    }
                                )
                            else:
                                raise asyncio.TimeoutError("Error Downloading")
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

    async def save_one(self, response):
        try:
            filename = self.get_filename(response)
            if len(filename.encode()) > 30:
                filename = filename[-30:]
            filepath = Path.joinpath(self.output_dir, filename)
            content = await response.read()
            logger.info(f"Save into: {filepath.absolute()}")
            with filepath.open('wb') as fout:
                fout.write(content)
            return (filename, filepath)
        except aiohttp.client_exceptions.ClientPayloadError:
            logger.error(f"ClientPayloadError {response.url}")
            return False

    def get_filename(self, response):
        filename = response.headers.get('Content-Disposition', uuid.uuid4().hex)
        obj = re.search(r'filename="(.*)"', filename)
        if obj:
            filename = obj.group(1)
            filename = urllib.parse.unquote(filename)
        return filename

    async def get_guid(self, document_id: str):
        url = "http://www.cebpubservice.com/tenderdocument/mhDocumentLibNoSessionAction/queryMhDocumentLibDetails.do"
        post_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36",
        }
        payload = {
            "documentId": document_id
        }
        proxy = await self.proxy_manager.get_proxy()
        # in case of competing with each requests
        await asyncio.sleep(1.5 + random.random() * 10)
        for _ in range(self.config.max_request_attempt):
            try:
                logger.info(f"Get GUID: documentId: {document_id}, proxy: {proxy}")
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        url,
                        headers=post_headers,
                        data=payload,
                        proxy=proxy,
                        timeout=self.timeout
                    ) as response:
                        if response.status != 200:
                            return {"object": {"newFileId1": None}}
                        return await response.json()
            except aiohttp.client_exceptions.ClientProxyConnectionError:
                logger.warning(f"Proxy invalid: {proxy}")
                await self.proxy_manager.delete_proxy(proxy)
                proxy = await self.proxy_manager.get_proxy()
            except aiohttp.client_exceptions.ClientOSError:
                logger.warning(f"ClientOSError Get GUID {document_id} Failed")
                await asyncio.sleep(self.config.sleep + random.random())
            except aiohttp.client_exceptions.ServerDisconnectedError:
                logger.warning(f"ServerDisconnectedError Get GUID {document_id} Failed")
                await asyncio.sleep(self.config.sleep + random.random())
            except asyncio.TimeoutError:
                logger.warning(f"TimeoutError Get GUID {document_id} Failed")
                await asyncio.sleep(self.config.sleep + random.random())
            except aiohttp.client_exceptions.ClientPayloadError:
                logger.error(f"ClientPayloadError Get GUID {document_id}")
                await asyncio.sleep(self.config.sleep + random.random())

        return {"object": {"newFileId1": None}}
