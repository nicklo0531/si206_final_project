from bs4 import BeautifulSoup
import re
import requests

def get_date_value(url):
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        # date (th)
        date_ls = []
        ths = soup.find_all('th', class_='pe-5')
        for th in ths:
            th = th.text
            date_ls.append(th)
        # value (td)
        value_ls = []
        tds = soup.find_all('td', class_='pe-5')
        for td in tds:
            td = td.text
            value_ls.append(td)
        combined_data = list(zip(date_ls, value_ls))
    return combined_data




# def get_date(url):
#     response = requests.get(url)
#     if response.status_code == 200:
#         soup = BeautifulSoup(response.content, 'html.parser')
#         # date (th)
#         date_ls = []
#         ths = soup.find_all('th', class_='pe-5')
#         for th in ths:
#             th = th.text
#             date_ls.append(th)
#     return date_ls



# def get_value(url):
#     response = requests.get(url)
#     if response.status_code == 200:
#         soup = BeautifulSoup(response.content, 'html.parser')
#         # value (td)
#         value_ls = []
#         tds = soup.find_all('td', class_='pe-5')
#         for td in tds:
#             td = td.text
#             value_ls.append(td)
#     return value_ls




# maybe combine the two into a tuple or dict
# limit to 25
# add db
def main (): 
    url = "https://fred.stlouisfed.org/data/WEI"
    print(get_date_value(url))

if __name__ == '__main__':
    main()