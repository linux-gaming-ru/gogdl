from multiprocessing.shared_memory import SharedMemory
import os
from queue import Empty
import shutil
import sys
import stat
import traceback
import time
import requests
import zlib
import hashlib
from io import BytesIO
from typing import Optional, Union
from copy import copy
from gogdl.dl import dl_utils
from dataclasses import dataclass
from enum import Enum, auto
from multiprocessing import Process, Queue, Lock
from urllib.parse import urlsplit
from gogdl.dl.objects.generic import MemorySegment, TaskFlag, TerminateWorker
import gogdl_xdelta3

CDN_SPEED_SMOOTHING = 0.5
CDN_RECHECK_INTERVAL = 30.0
CDN_FAILURE_COOLDOWN = 5.0


class FailReason(Enum):
    UNKNOWN = 0
    CHECKSUM = auto()
    CONNECTION = auto()
    UNAUTHORIZED = auto()

    MISSING_CHUNK = auto()
    INCOMPLETE_READ = auto()


@dataclass
class DownloadTask:
    product_id: str

@dataclass
class DownloadTask1(DownloadTask):
    offset: int
    size: int
    # This sum is not valid MD5 as it contains chunk id too
    # V1 doesn't support chunks, this is sort of forceful way to use them
    # in this algorithm
    compressed_sum: str
    memory_segment: MemorySegment

@dataclass
class DownloadTask2(DownloadTask):
    compressed_sum: str
    memory_segment: MemorySegment


@dataclass
class WriterTask:
    # Root directory of game files
    destination: str
    # File path from manifest
    file_path: str
    flags: TaskFlag

    hash: Optional[str] = None
    size: Optional[int] = None
    shared_memory: Optional[MemorySegment] = None
    old_destination: Optional[str] = None
    old_file: Optional[str] = None
    old_offset: Optional[int] = None

    patch_file: Optional[str] = None

@dataclass
class DownloadTaskResult:
    success: bool
    fail_reason: Optional[FailReason]
    task: Union[DownloadTask2, DownloadTask1]
    download_size: Optional[int] = None
    decompressed_size: Optional[int] = None

@dataclass
class WriterTaskResult:
    success: bool
    task: Union[WriterTask, TerminateWorker]
    written: int = 0


class CDNSelector:
    def __init__(self, state: dict, lock) -> None:
        self.state = state
        self.lock = lock

    def select(self, urls: list, excluded: set) -> int:
        now = time.monotonic()
        with self.lock:
            candidates = []
            for index, url in enumerate(urls):
                if index in excluded:
                    continue
                host = urlsplit(url).netloc
                stats = self.state.get(host, {
                    "speed": None, "active": 0, "checked": 0.0, "retry_after": 0.0,
                })
                candidates.append((index, host, stats))
            available = [item for item in candidates if item[2]["retry_after"] <= now] or candidates
            probes = [item for item in available if item[2]["active"] == 0 and (
                item[2]["speed"] is None or now - item[2]["checked"] >= CDN_RECHECK_INTERVAL
            )]
            measured = [item for item in available if item[2]["speed"] is not None]
            if probes:
                selected = min(probes, key=lambda item: item[2]["checked"])
            elif measured:
                selected = max(measured, key=lambda item: item[2]["speed"])
            else:
                selected = min(available, key=lambda item: item[2]["active"])
            index, host, stats = selected
            stats["active"] += 1
            self.state[host] = stats
            return index

    def complete(self, url: str, started: float, size: Optional[int]) -> None:
        # None marks a failed mirror; zero leaves authorization errors unscored.
        now = time.monotonic()
        host = urlsplit(url).netloc
        with self.lock:
            stats = self.state[host]
            stats["active"] -= 1
            if size is None:
                stats["retry_after"] = now + CDN_FAILURE_COOLDOWN
                stats["checked"] = now
            elif size > 0 and now > started:
                speed = size / (now - started)
                previous = stats["speed"] if stats["speed"] is not None else speed
                stats["speed"] = previous + CDN_SPEED_SMOOTHING * (speed - previous)
                stats["checked"] = now
                stats["retry_after"] = 0.0
            self.state[host] = stats


class Download(Process):
    def __init__(self, shared_memory, download_queue, results_queue, speed_queue, shared_secure_links, cdn_selector=None):
        self.shared_memory = SharedMemory(name=shared_memory)
        self.download_queue: Queue = download_queue
        self.results_queue: Queue = results_queue
        self.speed_queue: Queue = speed_queue
        self.secure_links: dict = shared_secure_links
        self.session = requests.session()
        self.cdn_selector = cdn_selector if cdn_selector is not None else CDNSelector({}, Lock())
        self.early_exit = False
        super().__init__()

    def run(self):
        while not self.early_exit:
            try:
                task: Union[DownloadTask1, DownloadTask2, TerminateWorker] = self.download_queue.get(timeout=1)
            except Empty:
               continue 

            if isinstance(task, TerminateWorker):
                break

            if type(task) == DownloadTask2:
                self.v2(task)
            elif type(task) == DownloadTask1:
                self.v1(task)

        self.session.close()
        self.shared_memory.close()

    def _get_download_url_v2(self, task, urls, index):
        if len(urls) <= index:
            index = len(urls) - 1
        endpoint = copy(urls[index])
        if task.product_id != 'redist':
            endpoint["parameters"] = copy(endpoint["parameters"])
            endpoint["parameters"]["path"] += f"/{dl_utils.galaxy_path(task.compressed_sum)}"
            url = dl_utils.merge_url_with_params(
                endpoint["url_format"], endpoint["parameters"]
            )
        else:
            endpoint["url"] += "/" + dl_utils.galaxy_path(task.compressed_sum)
            url = endpoint["url"]
        return url

    def _get_download_url_v1(self, urls, index=0):
        if type(urls) == str:
            url = urls
        else:
            endpoint = copy(urls[index])
            endpoint["parameters"] = copy(endpoint["parameters"])
            endpoint["parameters"]["path"] += "/main.bin"
            url = dl_utils.merge_url_with_params(
                endpoint["url_format"], endpoint["parameters"]
            )
        return url

    def v2(self, task: DownloadTask2):
        retries = 5 
        urls = self.secure_links[task.product_id]

        compressed_md5 = task.compressed_sum
        candidates = [self._get_download_url_v2(task, urls, index) for index in range(len(urls))]
        tried = set()

        fail_reason = None
        while retries > 0:
            endpoint = self.cdn_selector.select(candidates, tried)
            url = candidates[endpoint]
            result_size = None
            response = None
            buffer = bytearray()
            compressed_sum = hashlib.md5()
            download_size = 0
            decompressor = zlib.decompressobj()
            started = time.monotonic()
            try:
                response = self.session.get(url, stream=True, timeout=(5, 15))
                response.raise_for_status()
                for chunk in response.iter_content(1024 * 512):
                    download_size += len(chunk)
                    compressed_sum.update(chunk)
                    decompressed = decompressor.decompress(chunk)
                    buffer += decompressed
                    self.speed_queue.put((len(chunk), len(decompressed)))
                if compressed_sum.hexdigest() != compressed_md5:
                    raise ValueError("Compressed chunk checksum mismatch")
                result_size = download_size
            except ValueError:
                fail_reason = FailReason.CHECKSUM
            except (requests.exceptions.HTTPError, requests.exceptions.ConnectTimeout)  as e:
                print("Connection failed", e)
                if response is not None and response.status_code in [401, 403]:
                    result_size = 0
                    self.results_queue.put(DownloadTaskResult(False, FailReason.UNAUTHORIZED, task))
                    print("Connection failed, unauthorized")
                    return
                else:
                    fail_reason = FailReason.CONNECTION
            except requests.exceptions.RequestException as e:
                print("Connection failed", e)
                fail_reason = FailReason.INCOMPLETE_READ
            except Exception as e:
                print("Connection failed", e)
                fail_reason = FailReason.UNKNOWN
            else:
                break
            finally:
                self.cdn_selector.complete(url, started, result_size)
                if response is not None:
                    response.close()
            tried.add(endpoint)
            if len(tried) == len(candidates):
                tried.clear()
                time.sleep(2)
            retries -= 1
        else:
            self.results_queue.put(DownloadTaskResult(False, fail_reason, task))
            return

        decompressed_size = 0
        try:
            decompressed_size = len(buffer)
            self.shared_memory.buf[task.memory_segment.offset:decompressed_size+task.memory_segment.offset] = buffer

        except Exception as e:
            print("ERROR", e)
            self.results_queue.put(DownloadTaskResult(False, FailReason.UNKNOWN, task))
            return 

        self.results_queue.put(DownloadTaskResult(True, None, task, download_size=download_size, decompressed_size=decompressed_size))
    
    def v1(self, task: DownloadTask1):
        retries = 5
        urls = self.secure_links[task.product_id]

        response = None
        candidates = [urls] if isinstance(urls, str) else [
            self._get_download_url_v1(urls, index) for index in range(len(urls))
        ]
        tried = set()
        range_header = dl_utils.get_range_header(task.offset, task.size)

        buffer = bytearray()
        while retries > 0:
            endpoint = self.cdn_selector.select(candidates, tried)
            url = candidates[endpoint]
            result_size = None
            response = None
            buffer = bytearray()
            started = time.monotonic()
            try:
                response = self.session.get(url, stream=True, timeout=10, headers={'Range': range_header})
                response.raise_for_status()
                for chunk in response.iter_content(1024 * 512):
                    buffer += chunk 
                    self.speed_queue.put((len(chunk), len(chunk)))
                if len(buffer) != task.size:
                    raise ValueError("Downloaded chunk size mismatch")
                result_size = len(buffer)
            except Exception as e:
                print("Connection failed", e)
                #Handle exception
                if response is not None and response.status_code in [401, 403]:
                    result_size = 0
                    self.results_queue.put(DownloadTaskResult(False, FailReason.UNAUTHORIZED, task))
                    return
                tried.add(endpoint)
                if len(tried) == len(candidates):
                    tried.clear()
                retries -= 1
                time.sleep(2)
                continue
            finally:
                self.cdn_selector.complete(url, started, result_size)
                if response is not None:
                    response.close()
            break
        else:
            self.results_queue.put(DownloadTaskResult(False, FailReason.CHECKSUM, task))
            return
         
        download_size = 0
        try:
            download_size = len(buffer)
            self.shared_memory.buf[task.memory_segment.offset:download_size + task.memory_segment.offset] = buffer

        except Exception as e:
            print("ERROR", e)
            self.results_queue.put(DownloadTaskResult(False, FailReason.UNKNOWN, task))
            return 

        self.results_queue.put(DownloadTaskResult(True, None, task, download_size=download_size))

class Writer(Process):
    def __init__(self, shared_memory, writer_queue, results_queue, speed_queue, cache):
        self.shared_memory = SharedMemory(name=shared_memory)
        self.cache = cache
        self.writer_queue: Queue = writer_queue
        self.results_queue: Queue = results_queue
        self.speed_queue: Queue = speed_queue
        self.early_exit = False
        super().__init__()

    def run(self):
        file_handle = None
        current_file = ''

        while not self.early_exit:
            try:
                task: Union[WriterTask, TerminateWorker] = self.writer_queue.get(timeout=2)
            except Empty:
                continue

            if isinstance(task, TerminateWorker):
                self.results_queue.put(WriterTaskResult(True, task))
                break

            written = 0
            
            task_path = dl_utils.get_case_insensitive_name(os.path.join(task.destination, task.file_path))
            split_path = os.path.split(task_path)
            if split_path[0] and not os.path.exists(split_path[0]):
                dl_utils.prepare_location(split_path[0])

            if task.flags & TaskFlag.CREATE_FILE:
                open(task_path, 'a').close()
                self.results_queue.put(WriterTaskResult(True, task))
                continue

            elif task.flags & TaskFlag.CREATE_SYMLINK:
                dest = task.old_destination or task.destination
                # Windows will likely not have this ran ever
                if os.path.exists(task_path):
                    os.unlink(task_path)
                os.symlink(dl_utils.get_case_insensitive_name(os.path.join(dest, task.old_file)), task_path)
                self.results_queue.put(WriterTaskResult(True, task))
                continue

            elif task.flags & TaskFlag.OPEN_FILE:
                if file_handle:
                    print("Opening on unclosed file")
                    file_handle.close()
                file_handle = open(task_path, 'wb')
                current_file = task_path

                self.results_queue.put(WriterTaskResult(True, task))
                continue
            elif task.flags & TaskFlag.CLOSE_FILE:
                if file_handle:
                    file_handle.close()
                    file_handle = None
                self.results_queue.put(WriterTaskResult(True, task))
                continue
            
            elif task.flags & TaskFlag.COPY_FILE:
                if file_handle and task.file_path == current_file:
                    print("Copy on unclosed file")
                    file_handle.close()
                    file_handle = None

                if not task.old_file:
                    # if this ever happens....
                    self.results_queue.put(WriterTaskResult(False, task))
                    continue

                dest = task.old_destination or task.destination
                try:
                    shutil.copy(dl_utils.get_case_insensitive_name(os.path.join(dest, task.old_file)), task_path)
                except shutil.SameFileError:
                    pass
                except Exception:
                    self.results_queue.put(WriterTaskResult(False, task))
                    continue
                self.results_queue.put(WriterTaskResult(True, task))
                continue

            elif task.flags & TaskFlag.RENAME_FILE:
                if file_handle and task.file_path == current_file:
                    print("Renaming on unclosed file")
                    file_handle.close()
                    file_handle = None

                if not task.old_file:
                    # if this ever happens....
                    self.results_queue.put(WriterTaskResult(False, task))
                    continue
                
                if task.flags & TaskFlag.DELETE_FILE and os.path.exists(task_path):
                    try:
                        os.remove(task_path)
                    except OSError as e:
                        self.results_queue.put(WriterTaskResult(False, task))
                        continue
                dest = task.old_destination or task.destination
                try:
                    os.rename(dl_utils.get_case_insensitive_name(os.path.join(dest, task.old_file)), task_path)
                except OSError as e:
                    self.results_queue.put(WriterTaskResult(False, task))
                    continue

                self.results_queue.put(WriterTaskResult(True, task))
                continue
            
            elif task.flags & TaskFlag.PATCH:
                if file_handle and task.file_path == current_file:
                    print("Patching on unclosed file")
                    file_handle.close()
                    file_handle = None

                if not task.old_file or not task.patch_file:
                    # if this ever happens....
                    self.results_queue.put(WriterTaskResult(False, task))
                    continue

                try:
                    dest = task.old_destination or task.destination
                    source = os.path.join(dest, task.old_file)
                    source = dl_utils.get_case_insensitive_name(source)
                    patch = os.path.join(task.destination, task.patch_file)
                    patch = dl_utils.get_case_insensitive_name(patch)
                    target = task_path
                    gogdl_xdelta3.patch(source, patch, target, self.speed_queue)

                except Exception as e:
                    print("Patch failed", e)
                    print(traceback.format_exc())
                    self.results_queue.put(WriterTaskResult(False, task))
                    continue
                written = 0
                if os.path.exists(target):
                    written = os.path.getsize(target)
                self.results_queue.put(WriterTaskResult(True, task, written=written))
                continue
            
            elif task.flags & TaskFlag.DELETE_FILE:
                if file_handle and task.file_path == current_file:
                    print("Deleting on unclosed file")
                    file_handle.close()
                    file_handle = None
                try:
                    if os.path.exists(task_path):
                        os.remove(task_path)
                except OSError as e:
                    self.results_queue.put(WriterTaskResult(False, task))
                    continue

            elif task.flags & TaskFlag.MAKE_EXE:
                if file_handle and task.file_path == current_file:
                    print("Making exe on unclosed file")
                    file_handle.close()
                    file_handle = None
                if sys.platform != 'win32':
                    try:
                        st = os.stat(task_path)
                        os.chmod(task_path, st.st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
                    except Exception as e:
                        self.results_queue.put(WriterTaskResult(False, task))
                        continue
                self.results_queue.put(WriterTaskResult(True, task))
                continue

            try:
                if task.shared_memory:
                    if not task.size:
                        print("No size")
                        self.results_queue.put(WriterTaskResult(False, task))
                        continue
                    offset = task.shared_memory.offset
                    end = offset + task.size
                    left = task.size
                    buffer = BytesIO(self.shared_memory.buf[offset:end].tobytes())
                    while left > 0:
                        chunk = buffer.read(min(1024 * 1024, left))   
                        written += file_handle.write(chunk)
                        self.speed_queue.put((len(chunk), 0))
                        left -= len(chunk)
                        
                    if task.flags & TaskFlag.OFFLOAD_TO_CACHE and task.hash:
                        cache_file_path = os.path.join(self.cache, task.hash)
                        dl_utils.prepare_location(self.cache)
                        cache_file = open(cache_file_path, 'wb')
                        cache_file.write(self.shared_memory.buf[offset:end].tobytes())
                        self.speed_queue.put((task.size, 0))
                        cache_file.close()
                elif task.old_file:
                    if not task.size:
                        print("No size")
                        self.results_queue.put(WriterTaskResult(False, task))
                        continue
                    dest = task.old_destination or task.destination
                    old_file_path = dl_utils.get_case_insensitive_name(os.path.join(dest, task.old_file))
                    old_file_handle = open(old_file_path, "rb")
                    if task.old_offset:
                        old_file_handle.seek(task.old_offset)
                    left = task.size
                    if task.flags & TaskFlag.ZIP_DEC:
                        decompressor = zlib.decompressobj(-15)
                    else:
                        decompressor = None
                    while left > 0:
                        chunk = old_file_handle.read(min(1024*1024, left))
                        if decompressor:
                            data = decompressor.decompress(chunk)
                        else:
                            data = chunk
                        written += file_handle.write(data)
                        self.speed_queue.put((len(data), len(chunk)))
                        left -= len(chunk)
                    old_file_handle.close()
                    if task.flags & TaskFlag.ZIP_DEC:
                        written = written - task.size

            except Exception as e:
                print("Writer exception", e)
                self.results_queue.put(WriterTaskResult(False, task))
            else:
                self.results_queue.put(WriterTaskResult(True, task, written=written))

        
        self.shared_memory.close()
        shutil.rmtree(self.cache, ignore_errors=True)
