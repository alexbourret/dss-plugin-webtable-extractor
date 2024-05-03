import requests
from safe_logger import SafeLogger


logger = SafeLogger("webtable-extractor plugin")


class RecordsLimit():
    def __init__(self, records_limit=-1):
        self.has_no_limit = (records_limit == -1)
        self.records_limit = records_limit
        self.counter = 0

    def is_reached(self):
        if self.has_no_limit:
            return False
        self.counter += 1
        return self.counter > self.records_limit


def safe_get(url):
    error_message = None
    data = None
    try:
        logger.info("Trying to access {}".format(url))
        response = requests.get(url)
    except Exception as error:
        error_message = "Error: {}".format(error)
        logger.error("{} while accessing {}".format(error_message, url))
        return None, error_message

    if response.status_code >= 400:
        error_message = "Error {}".format(response.status_code)
        logger.error("{} while accessing {}.".format(error_message, url))
        logger.error("Dumping content: {}".format(response.content))
    else:
        logger.info("Success on {}".format(url))
        data = response.content
    return data, error_message

'''
find title:
table.find('caption').text.strip('\n')
if not:
soup.find('table').previousSibling.previousSibling
soup.find('table').previousSibling.previousSibling.text.strip('\n')
'''

'''
https://stackoverflow.com/questions/56757261/extract-href-using-pandas-read-html
option1: 
import pandas as pd
import requests
from bs4 import BeautifulSoup


url = 'http://www.vru.gov.ua/act_list'



response = requests.get(url)
soup = BeautifulSoup(response.text, 'html.parser')
table = soup.find('table')

records = []
columns = []
for tr in table.findAll("tr"):
    ths = tr.findAll("th")
    if ths != []:
        for each in ths:
            columns.append(each.text)
    else:
        trs = tr.findAll("td")
        record = []
        for each in trs:
            try:
                link = each.find('a')['href']
                text = each.text
                record.append(link)
                record.append(text)
            except:
                text = each.text
                record.append(text)
        records.append(record)

columns.insert(1, 'Link')
df = pd.DataFrame(data=records, columns = columns)

option 2:
import pandas as pd
import requests
from bs4 import BeautifulSoup

url = 'http://www.vru.gov.ua/act_list'
df = pd.read_html(url)[0]

response = requests.get(url)
soup = BeautifulSoup(response.text, 'html.parser')
table = soup.find('table')
tables = soup.findAll('table')

links = []
for tr in table.findAll("tr"):
    trs = tr.findAll("td")
    for each in trs:
        try:
            link = each.find('a')['href']
            links.append(link)
        except:
            pass

df['Link'] = links
'''