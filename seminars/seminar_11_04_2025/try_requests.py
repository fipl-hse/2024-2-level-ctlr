"""
Listing for practice with requests library
"""

# pylint: disable=missing-timeout,line-too-long,invalid-name


try:
    import requests
except ImportError:
    print("No libraries installed. Failed to import.")

if __name__ == "__main__":
    # Step 1. GET request
    url = "https://www.nn.ru/"
    response = requests.get(url)
    print(f"Status code: {response.status_code}")
    print(f"First 500 chars:\n{response.text[:500]}...")

    # Step 2. Adding headers (simulate browser request) and get 200 status code
    # View my headers at: https://www.whatismybrowser.com/detect/what-http-headers-is-my-browser-sending/
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        "Cookie": "__ddg1_=55afR7QHikTcFO5Ne4CM; ngs_uid=wxPcC2PCmxi1f/SWDnpXAg==; ngs_analytics_uid=wxPcEmPCmxnAvyxjJG1xAg==; _ym_uid=1673698074914339686; SessIDNNRU=kg6jh26hmiki5s3n94qv7v7ous; jtnews_ab_24=A; jtnews_ab_29=A; jtnews_ab_main_page_4redesign=B; _ym_d=1693554806; _ga_TRSEKWR47P=GS1.1.1693554805.1.0.1693554805.0.0.0; _ga=GA1.1.1101686470.1693554806; _ym_isad=1; _ym_visorc=b; jtnews_a_template=true",
    }
    response = requests.get(url, headers=headers)
    print(f"With headers status: {response.status_code}")

    # Step 3. Using timeout (seconds) to lower website load
    try:
        response = requests.get(url, headers=headers, timeout=3)
        print("Request successful with timeout")
    except requests.exceptions.Timeout:
        print("Timeout: Server didn't respond in 3s")
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")

    # Step 4. Response handling
    if response.ok:
        with open("page.html", "w", encoding="utf-8") as f:
            f.write(response.text)
        print("Page saved successfully")
    else:
        print(f"Error: HTTP {response.status_code}")
