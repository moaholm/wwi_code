import requests

proxies = {
    'http': 'http://51.158.154.173:3128',
    'https': 'https://51.158.154.173:3128'
}


try:
    response = requests.get('https://www.google.com', proxies=proxies, timeout=10)
    response.status_code
except requests.exceptions.RequestException as e:
    print("Proxy error:", e)
