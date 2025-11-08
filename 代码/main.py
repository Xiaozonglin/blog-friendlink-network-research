import requests
from bs4 import BeautifulSoup

html = requests.get("https://www.xiaozonglin.cn").text

soup = BeautifulSoup(html, 'html.parser')

for link in soup.find_all('a'):
    print(link['href'])