import requests
from bs4 import BeautifulSoup

if __name__ == '__main__':
    correct_url = "https://www.gorno-altaisk.info/news/177862"
    response = requests.get(correct_url)
    response.encoding = 'utf-8'
    print(response.text)

    soup = BeautifulSoup(response.text, features="lxml")
    print(soup.title)
    print(soup.title.text)
    print(soup.find("h1"))
    print(soup.find("h1").text)
    print(soup.find_all("h1"))

    # 3. Finding tags by their name
    all_spans = soup.find_all("span")
    print(f"Number of spans: {len(all_spans)}")

    # 4. Finding elements by their class
    header = soup.find_all(class_="leadParagraph_Gq8Rx")
    if header:
        print(f"Found a header: {header[0].text}")

    # 5. You can mix them all if you need
        additional_info = soup.find_all(
            "div", class_="articleRemarkAboutMistake_ilBSy text-style-ui-caption-3 mt-6 mb-8"
        )
        if additional_info:
            print(f"Found additional_info: {additional_info}")

# 6. Get all texts
all_body = soup.find_all("p")

texts = []
for p in all_body:
    texts.append(p.text)

print("All text from a page:")
print(" ".join(texts))
