import os
import glob
import asyncio
import aiohttp
from bs4 import BeautifulSoup
from urllib.parse import urlparse, unquote

OUTPUT_DIR = "downloaded"
os.makedirs(OUTPUT_DIR, exist_ok=True)


def make_filename(url: str) -> str:
    """Генерирует уникальное имя файла из URL с расширением .html"""
    parsed = urlparse(url)

    # Берём путь (без ведущего /)
    path = parsed.path.strip("/")
    if not path:
        path = "index"

    # Добавляем query (если есть)
    if parsed.query:
        path += "_" + parsed.query.replace("=", "-").replace("&", "_")

    # Делаем безопасное имя
    safe_name = unquote(path).replace("/", "_").replace("?", "_")

    # Если имя без расширения — добавляем .html
    if not safe_name.lower().endswith(".html"):
        safe_name += ".html"

    return safe_name


async def fetch(session, url):
    """Скачивает страницу асинхронно"""
    filename = make_filename(url)
    local_path = os.path.join(OUTPUT_DIR, filename)

    try:
        async with session.get(url, timeout=20) as resp:
            if resp.status == 200:
                text = await resp.text()
                with open(local_path, "w", encoding="utf-8") as f:
                    f.write(text)
                print(f"⬇️ {url} → {local_path}")
                return url, local_path
            else:
                print(f"⚠️ Ошибка {resp.status} для {url}")
                return url, None
    except Exception as e:
        print(f"❌ Ошибка скачивания {url}: {e}")
        return url, None


async def download_all(urls):
    """Асинхронно скачивает все ссылки"""
    async with aiohttp.ClientSession() as session:
        tasks = [fetch(session, url) for url in urls]
        results = await asyncio.gather(*tasks)
        return {url: path for url, path in results if path}


def extract_links(files):
    """Достаёт все ссылки из report*.html"""
    links = set()
    for filepath in files:
        with open(filepath, "r", encoding="utf-8") as f:
            soup = BeautifulSoup(f, "html.parser")
            for a in soup.find_all("a", href=True):
                links.add(a["href"])
    return links


def rewrite_reports(files, link_map):
    """Переписывает ссылки в локальные файлы"""
    for filepath in files:
        with open(filepath, "r", encoding="utf-8") as f:
            soup = BeautifulSoup(f, "html.parser")
            for a in soup.find_all("a", href=True):
                if a["href"] in link_map:
                    a["href"] = link_map[a["href"]]
        new_name = filepath.replace(".html", "_local.html")
        with open(new_name, "w", encoding="utf-8") as f:
            f.write(str(soup))
        print(f"🔗 Переписан {filepath} → {new_name}")


async def main():
    report_files = glob.glob("report*.html")
    links = extract_links(report_files)
    print(f"✅ Найдено {len(links)} уникальных ссылок")

    # Скачивание
    link_map = await download_all(links)
    print(f"✅ Успешно скачано {len(link_map)} файлов")

    # Перезапись ссылок
    rewrite_reports(report_files, link_map)


if __name__ == "__main__":
    asyncio.run(main())