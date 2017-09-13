from time import sleep
import asyncio
from aiohttp import ClientSession
from lxml import html
import itertools


def get_url_page(count=1):
    """
    :param count: кількість сторінок
    :return: Список urls сторінок з темами
    """
    list_url_page = []
    list_url_page.append('https://www.houzz.com.au/professionals/s/Home-Builders')
    for i in range(count - 1):
        list_url_page.append('https://www.houzz.com.au/professionals/home-builders/p/' + str((i + 1) * 15))
    return list_url_page

class get_urls():
    def __init__(self, urls_page):
        self.result = []
# Сюда будем складывать результат.
        self.total_checked = 0
# Сколько всего страниц наша программа запарсила на данный момент.
        self.urls_page =  urls_page

    async def get_one(self, url, session):
        #global
        #self.total_checked
        #proxy = "http://10.24.100.210:3128"
        async with session.get(url) as response:
        # Ожидаем ответа и блокируем таск.
            page_content = await response.read()
            item = self.get_item(page_content, url)
            self.result.append(item)
            self.total_checked += 1
            print('Inserted: ' + url + '  - - - Total checked: ' + str(self.total_checked))

    async def bound_fetch(self, sm, url, session):
        try:
            async with sm:
                await self.get_one(url, session)
        except Exception as e:
            print(e)
            # Блокируем все таски на 30 секунд в случае ошибки 429.
            sleep(10)

    async def run(self, urls):
        tasks = []
        # Выбрал лок от балды. Можете поиграться.
        sm = asyncio.Semaphore(10)
        headers = {"User-Agent": "Mozilla/5.001 (windows; U; NT4.0; en-US; rv:1.0) Gecko/25250101"}
        # Опять же оставляем User-Agent, чтобы не получить ошибку от Metacritic
        async with ClientSession(
                headers=headers) as session:
            for url in urls:
                # Собираем таски и добавляем в лист для дальнейшего ожидания.
                task = asyncio.ensure_future(self.bound_fetch(sm, url, session))
                tasks.append(task)
        # Ожидаем завершения всех наших задач.
            await asyncio.gather(*tasks)


    def get_item(self, page_content, url):
       # Получаем корневой lxml элемент из html страницы.
        document = html.fromstring(page_content)

        def get(xpath):
            item = document.xpath(xpath)
            if item:
                return item
            # Если вдруг какая-либо часть информации на странице не найдена, то возвращаем None
            return None
        urls = get("//div[@class='pro-cover-photos']/a/@href")
        #urls_full.append(urls)
        return urls

    def main(self):
        # Запускаем наш парсер.
        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(self.run(self.urls_page))
        loop.run_until_complete(future)
        # Выводим результат. Можете сохранить его куда-нибудь в файл.
        list_urls = list(itertools.chain.from_iterable(self.result))
        return list_urls

class get_data():
    def __init__(self, urls_page):
        self.result = []
# Сюда будем складывать результат.
        self.total_checked = 0
# Сколько всего страниц наша программа запарсила на данный момент.
        self.urls_page =  urls_page
        self.list_data = []

    async def get_one(self, url, session):
        #global
        #self.total_checked
        #proxy = "http://10.24.100.210:3128"
        async with session.get(url) as response:
        # Ожидаем ответа и блокируем таск.
            page_content = await response.read()
            item = self.get_item(page_content, url)
            self.result.append(item)
            self.total_checked += 1
            print('Inserted: ' + url + '  - - - Total checked: ' + str(self.total_checked))

    async def bound_fetch(self, sm, url, session):
        try:
            async with sm:
                await self.get_one(url, session)
        except Exception as e:
            print(e)
            # Блокируем все таски на 30 секунд в случае ошибки 429.
            sleep(10)

    async def run(self, urls):
        tasks = []
        # Выбрал лок от балды. Можете поиграться.
        sm = asyncio.Semaphore(15)
        headers = {"User-Agent": "Mozilla/5.001 (windows; U; NT4.0; en-US; rv:1.0) Gecko/25250101"}
        # Опять же оставляем User-Agent, чтобы не получить ошибку от Metacritic
        async with ClientSession(
                headers=headers) as session:
            for url in urls:
                # Собираем таски и добавляем в лист для дальнейшего ожидания.
                task = asyncio.ensure_future(self.bound_fetch(sm, url, session))
                tasks.append(task)
        # Ожидаем завершения всех наших задач.
            await asyncio.gather(*tasks)


    def get_item(self, page_content, url):
       # Получаем корневой lxml элемент из html страницы.
        document = html.fromstring(page_content)

        def get(xpath):
            item = document.xpath(xpath)
            if item:
                return item
            # Если вдруг какая-либо часть информации на странице не найдена, то возвращаем None
            return None
        #urls = get("//div[@class='pro-cover-photos']/a/@href")

        print(url)
        try:
            Company_Name = get("//div[@class='pro-name']/text()")
        except (IndexError, UnboundLocalError):
            Company_Name = ""
        City = get("//span[@itemprop='addressLocality']/a/text()")
        if City == None:
            City = get("//span[@itemprop='addressLocality']/text()")
        if City == None:
            City = [""]


        try:
            State = get("//span[@itemprop='addressRegion']/text()")
        except (IndexError, UnboundLocalError):
           State = ""
        try:
           Postcode = get("//span[@itemprop='postalCode']/text()")
        except (IndexError, UnboundLocalError):
           Postcode = ""
        try:
           Telephone = get("//span[@class='pro-contact-text']/text()")
        except (IndexError, UnboundLocalError):
           Telephone = ""
        try:
           Website = get("//a[@class='proWebsiteLink']/@href")
        except (IndexError, UnboundLocalError):
           Website = ""
        dic = dict()
        dic['Company_Name'] = Company_Name[0]
        dic['City'] = City[0]
        dic['State'] = State[0]
        dic['Postcode'] = Postcode
        dic['Telephone'] = Telephone
        dic['Website'] = Website
        self.list_data.append(dic)
        # urls_full.append(urls)
        return urls


    def main(self):
        # Запускаем наш парсер.
        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(self.run(self.urls_page))
        loop.run_until_complete(future)
        # Выводим результат. Можете сохранить его куда-нибудь в файл.
        #list_urls = list(itertools.chain.from_iterable(self.result))
        data = self.list_data
        return data

if __name__ == "__main__":
    urls_page = get_url_page(50)
    S = get_urls(urls_page)
    urls = S.main()
    print(len(urls))
    set_urls = set(urls)
    urls = set_urls
    print(len(urls))

    #urls = ['https://www.houzz.com.au/pro/kelliedoolan/novara-homes',
            #'https://www.houzz.com.au/pro/67dallas/demax-constructions']
    SS = get_data(urls)
    data = SS.main()
    print(data)