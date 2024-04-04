import os
import sys
import requests
import time
import asyncio
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import logging
from datetime import datetime
import argparse
import time
import os
from urllib.parse import urlparse
import concurrent.futures
def comma_separated_strings_to_set(str) -> list:
    return {s for s in str.split(',')}

def comma_separated_str_to_int_set(str) ->list[int]:
    return {int(i) for i in str.split(',')}

def init_request(target):
    # print("Checking if site is up...")
    try:
        response = requests.get(target)
        return response
    except requests.exceptions.ConnectionError:
        print("Site may be down...\nTry again later?")
        sys.exit()
    except ConnectionError:
        print("Site may be down...\nTry again later?")
        sys.exit()
    except ValueError as e:
        print(e)

def validate_target(target):
    """Simple check if protocol is specified"""
    if 'http://' in target or 'https://' in target:
        return True
    else:
        print('ERROR: Please specify "HTTP" or "HTTPS" in the target URL.')
        return False

async def fetch(url):
    '''since requests is a synchronous library we must spawn a new thread in order to execute a web request 
    in a non blocking way'''
    with concurrent.futures.ThreadPoolExecutor() as executor:
        try:
            request = asyncio.get_event_loop().run_in_executor(executor, requests.get, url)
            response = await request
            return response
        except requests.exceptions.ConnectionError:
            print(f"ERROR: Connection error for url {url}")
        except requests.exceptions.Timeout:
            print(f"ERROR: Timeout for url {url}")
        except Exception as e:
            print(f"ERROR: An error occurred for url {url}: {e}")

class Url:
    def __init__(self):
        self.id_: str = ''
        self.domain: str = ''
        self.subdomain: str = ''
        self.url: str = ''
        self.status: str = ''
        self.content: str = ''
        self.param: list[str] = ''
        self.timestamp: int = time.time()

    def build_from_response(self,response, save=True):
        if response:
            u = urlparse(response.url)
            self.url = response.url
            self.status = response.status_code
            self.timestamp = int(time.time())
        if save:
            self.content = response.content

    #Tools for extracting information from urls

    def get_parent(url_parser_obj: object) -> str:
        '''Pass in a url and return the path of its parent directory'''
        u = urlparse(url)
        path = u.path
        if path.endswith('/'):
            path = path[:-1]
        parent = os.path.dirname(path)
        return parent

    def get_path(url_parser_obj: str) -> str:
        '''Pass in a url and return only the directory path'''
        u = urlparse(url)
        path = u.path
        return path

    def get_domain(url_parser_obj: str) -> str:
        '''Pass in a url and return the domain'''
        u = urlparse(url)
        domain = f'{u.netloc}'
        return domain

    def get_qualified_domain(url_parser_obj):
        '''Pass in a url and return the fully qualified domain'''
        u = urlparse(url)
        domain = f'{u.scheme}://{u.netloc}'
        return domain

    def get_protocol(url_parser_obj: str) -> str:
        '''Pass in a url and return the protocol section'''
        u = urlparse(url)
        proto = u.scheme
        return proto

class Crawler:
    def __init__(self, args = None):
        self.args = args
        self.queue = asyncio.Queue()
        self.history = set()
        self.status = None
        self.logger = logging
        timestamp = datetime.now().strftime("%y-%m-%d-%H-%M-%S")
        logging.basicConfig(filename=f'crawler-{timestamp}.log',level=logging.INFO,filemode='w',format='%(message)s')

    async def crawl(self):
        await self.worker_handler()

    async def request_worker(self, target, semaphore):
        async with semaphore:
            response = await fetch(target)
            content_type = response.headers.get('Content-Type', '').split(';')[0]
            if content_type in self.args.a and len(response.content) > 0:
                url = Url()
                url.build_from_response(response, save=True)
                self.logger.info(f'{url.url}')

                for url in self.find_urls(url):
                    if url not in self.history:
                        await self.queue.put(url)

            await asyncio.sleep(self.args.t)

    async def worker_handler(self):
        print('workerhandler')
        semaphore = asyncio.Semaphore(self.args.w)
        if validate_target(self.args.u):
            response = init_request(self.args.u)
            if response.status_code not in self.args.ic:
                url = Url()
                url.build_from_response(response)
                self.queue.put_nowait(response.url)
            else:
                print("Url didnt return anything useful")
                return
        else:
            return

        tasks = []

        while self.queue.qsize() > 0:
            for _ in range(self.queue.qsize()):
                # await asyncio.sleep(.001)
                url = await self.queue.get()
                task = asyncio.create_task(self.request_worker(url, semaphore))
                tasks.append(task)
                self.history.add(url)

            await asyncio.gather(*tasks)

    def find_urls(self, url_obj):
        urls = []
        if url_obj.content:
            soup = BeautifulSoup(url_obj.content, 'html.parser')
            urls = []
            for a in soup.find_all('a', href=True):
                url = urljoin(url_obj.url, a['href'])
                urls.append(url)

        return urls


async def main():
    parser = argparse.ArgumentParser(description='Scratch')
    parser.add_argument('-u', type=str, help='URL to crawl')
    parser.add_argument('-w', type=int, default=16,help='Maximum number of simulataneus requests that can be made')
    parser.add_argument('-t', type=float, default=0, help='Specify timeout after a request has been made')
    parser.add_argument('-a', type=comma_separated_strings_to_set, default={'text/html'}, help='Page content types to accept as a comma seperated list. ie "application/json,text/html"')
    parser.add_argument('-ic', type=comma_separated_str_to_int_set, default={400,404}, help='Ignore pages based on status codes,seperated by commas. Ex: "400,404,500"')
    args = parser.parse_args()
    print(args)
    crawler = Crawler(args)
    await crawler.crawl()

if __name__ == '__main__':
    asyncio.run(main())