
import re
import html
import requests
from bs4 import BeautifulSoup


headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'accept-language': 'de,de-DE;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6,fr;q=0.5,de-CH;q=0.4,es;q=0.3',
    'cache-control': 'no-cache',
    'dnt': '1',
    'pragma': 'no-cache',
    'sec-ch-ua': '"Not_A Brand";v="99", "Microsoft Edge";v="109", "Chromium";v="109"',
    'sec-ch-ua-arch': '"x86"',
    'sec-ch-ua-bitness': '"64"',
    'sec-ch-ua-full-version': '"109.0.1518.78"',
    'sec-ch-ua-full-version-list': '"Not_A Brand";v="99.0.0.0", "Microsoft Edge";v="109.0.1518.78", "Chromium";v="109.0.5414.120"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-model': '""',
    'sec-ch-ua-platform': '"Windows"',
    'sec-ch-ua-platform-version': '"10.0.0"',
    'sec-ch-ua-wow64': '?0',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'none',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36 Edg/109.0.1518.78',
}


def get_website(url):
    """Fetches HTML content of site and returns it as a BeautifulSoup object.
    """
    response = requests.get(url, headers=headers)
    html_text = response.content
    soup = BeautifulSoup(html_text, 'lxml')
    return soup


def extract_company_name(url):
    """ Extract the company name from the website URL.

    Usage: 
        extract_company_name_batch("https://playlistpush.com/")
    Output:
        "playlistpush"
    """
    match = re.search(r'[\w]+://(?:www.)?(.+?).[\w]+/', url)
    if match:
        return match.group(1)
    else:
        return ""
    

def extract_company_name_batch(url_list):
    """Extract the company names from a list of urls."""
    extracted_names = []
    for url in url_list:
        name = extract_company_name(url)
        extracted_names.append(name)
    return extracted_names


def extract_domain(url):
    """ Extract the company domain from the website URL.

    Usage: 
        extract_company_name_batch("https://playlistpush.com/")
    Output:
        "playlistpush.com"
    """
    return re.findall(r'https?:\/\/(?:www\.)?([a-zA-Z0-9.-]+)',  url)[0]
    

def get_name_variations(name):
    """ Get variations of company name for more effective search queries."""
    ignore = ['one-submit']
    
    if name not in ignore:
        variations = [name, name.replace('-', ' ')] # Handle variations of the company name
        if name == 'playlistpush':
            variations += ['playlist push']
        elif name == 'omarimc':
            variations += ['omari mc']
        elif name == 'starlightpr1':
            variations += ['starlight pr']
        elif name == 'planetarygroup':
            variations += ['planetary group']
        elif name == 'indiemusicacademy':
            variations += ['indie music academy']
        elif name == 'submithub':
            variations += ['submit hub']
        elif name == 'soundcamps':
            variations += ['soundcampaign']
        return list(set(variations)) # Remove duplicates
    else:
        return list(name)
    

def clean_text(text):
    text = html.unescape(text) #Remove HTML escape characters
    text = re.sub(r'@[A-Za-z0–9]+', '', text) #Remove @mentions replace with blank
    text = re.sub(r'#', '', text) #Remove the '#' symbol, replace with blank
    text = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', text) #Remove the hyperlinks
    text = re.sub(r':', '', text) #Remove :
    text = re.sub(r'\n-', '', text) #Remove '\n-'
    text = re.sub(r'(?:\n)+', ' ', text) #Remove '\n\n'
    text = re.sub(r'\+', '', text) #Remove '\'
    text = re.sub(r'\[removed\]', '', text) #Remove '[removed]'
    text = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', text) #Remove markdown links
    text = re.sub(r'&x200b;', '', text) # Remove zero-width whitespace
    text = re.sub(r'[\[\]]+', '', text)
    return text


def remove_emoji(string):
    emoji_pattern = re.compile("["
    u"\U0001F600-\U0001F64F" # emoticons
    u"\U0001F300-\U0001F5FF" # symbols & pictographs
    u"\U0001F680-\U0001F6FF" # transport & map symbols
    u"\U0001F1E0-\U0001F1FF" # flags (iOS)
    u"\U00002500-\U00002BEF" # chinese char
    u"\U00002702-\U000027B0"
    u"\U00002702-\U000027B0"
    u"\U000024C2-\U0001F251"
    u"\U0001f926-\U0001f937"
    u"\U00010000-\U0010ffff"
    u"\u2640-\u2642"
    u"\u2600-\u2B55"
    u"\u200d"
    u"\u23cf"
    u"\u23e9"
    u"\u231a"
    u"\ufe0f" # dingbats
    u"\u3030"
    "]+", flags=re.UNICODE)
    return emoji_pattern.sub(r'', string)


def clean_phone_number(numbers):
    result = [re.sub(r'\D', '', number) for number in numbers]
    result = list(set(result))
    return result