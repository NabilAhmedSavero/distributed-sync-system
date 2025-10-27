import asyncio
import logging
from typing import Dict, Optional, Any
from aiohttp import ClientSession, ClientTimeout, ClientError, TCPConnector

logger = logging.getLogger(__name__)

class AsyncHttpClient:
    def __init__(self, timeout_seconds: float = 5.0, connection_limit: int = 100):
        self._session: Optional[ClientSession] = None
        self._timeout = ClientTimeout(total=timeout_seconds)
        
        self._connector = TCPConnector(limit=connection_limit) 

    async def start(self):
        if not self._session:
            self._session = ClientSession(timeout=self._timeout, connector=self._connector)
            logger.info("HTTP client session started.")

    async def stop(self):
        if self._session:
            await self._session.close()
            self._session = None
            logger.info("HTTP client session stopped.")

    async def post(self, url: str, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not self._session:
            logger.error("HTTP client session not started.")
            return None

        try:
            async with self._session.post(url, json=data) as response:
                response.raise_for_status() 
                
                if response.content_type == 'application/json':
                    return await response.json()
                else:
                    text_response = await response.text()
                    logger.warning(f"Received non-JSON response from {url}: {response.status} {text_response[:100]}")
                    
                    return {"status": response.status, "raw_response": text_response}
        except ClientError as e:
            logger.debug(f"HTTP POST to {url} failed: {e}")
            return None
        except asyncio.TimeoutError:
            logger.debug(f"HTTP POST to {url} timed out after {self._timeout.total}s")
            return None
        except Exception as e:
            
            logger.error(f"Unexpected error during HTTP POST to {url}: {e}", exc_info=True)
            return None

    async def get(self, url: str) -> Optional[Dict[str, Any]]:
        if not self._session:
            logger.error("HTTP client session not started.")
            return None
            
        try:
            async with self._session.get(url) as response:
                response.raise_for_status()
                if response.content_type == 'application/json':
                     return await response.json()
                else:
                     text_response = await response.text()
                     logger.warning(f"Received non-JSON GET response from {url}: {response.status} {text_response[:100]}")
                     return {"status": response.status, "raw_response": text_response}
        except ClientError as e:
            logger.debug(f"HTTP GET from {url} failed: {e}")
            return None
        except asyncio.TimeoutError:
            logger.debug(f"HTTP GET from {url} timed out after {self._timeout.total}s")
            return None
        except Exception as e:
            logger.error(f"Unexpected error during HTTP GET from {url}: {e}", exc_info=True)
            return None