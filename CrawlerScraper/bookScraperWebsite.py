import requests
from bs4 import BeautifulSoup
import json
import time

base_url = "http://books.toscrape.com/catalogue/"
starting_page = "http://books.toscrape.com/catalogue/page-1.html"


def scrape_book_details(book_url):
    """
    Funktion um die einzelnen Buchlinks zu scrapen und die Buchdetails zurückzugeben.
    :param book_url: URL des Buches das zu scrapen ist.
    :return: Die Buchdetails.
    """
    response = requests.get(book_url)
    response.encoding = 'utf-8'
    soup = BeautifulSoup(response.text, 'html.parser')

    title = soup.find('h1').get_text()

    product_info = soup.find('table', class_='table table-striped')
    price_incl_tax = product_info.find('th', text='Price (incl. tax)').find_next('td').get_text().strip()
    price_excl_tax = product_info.find('th', text='Price (excl. tax)').find_next('td').get_text().strip()
    tax = product_info.find('th', text='Tax').find_next('td').get_text().strip()
    availability = product_info.find('th', text='Availability').find_next('td').get_text().strip()

    availability_num = int(''.join(filter(str.isdigit, availability)))

    description_tag = soup.find('meta', {'name': 'description'})
    description = description_tag['content'].strip() if description_tag else "No description available"

    rating_tag = soup.find('p', class_='star-rating')
    rating = rating_tag['class'][1] if rating_tag else "No rating"

    genre = soup.find('ul', class_='breadcrumb').find_all('li')[2].get_text().strip()

    return {
        "title": title,
        "genre": genre,
        "price_with_tax": price_incl_tax,
        "price_without_tax": price_excl_tax,
        "tax": tax,
        "availability": availability_num,
        "rating": rating,
        "description": description
    }

def scrape_books(starting_url):
    """
    Die Funktion entnimmt alle Buch-Links die sich auf der start url befinden und sendet diese weiter and die Funktion
    scrape_book_details welche wiederrum die Bücherdaten der einzelnen Bücher. Zuletzt wird der Link auf die nächste
    Seite gesetzt oder die Schleife schließt ab.
    :param starting_url: String der URL die gescraped werden soll.
    :return: Liste aller Bücher.
    """

    books_data = []
    current_url = starting_url

    while current_url:
        response = requests.get(current_url)
        soup = BeautifulSoup(response.text, 'html.parser')

        books = soup.find_all('article', class_='product_pod')
        for book in books:
            book_url = base_url + book.find('h3').find('a')['href'].replace('../', '')
            book_details = scrape_book_details(book_url)
            books_data.append(book_details)
            print(f"Scraped: {book_details['title']}")

        next_button = soup.find('li', class_='next')
        if next_button:
            next_page = next_button.find('a')['href']
            current_url = base_url + next_page
        else:
            current_url = None

    return books_data


def main():
    books = scrape_books(starting_page)

    with open('books_data2.json', 'w') as json_file:
        json.dump(books, json_file, indent=4)

    print("Scraped data saved to 'books_data2.json'")


if __name__ == "__main__":
    start_time = time.time()
    main()
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Elapsed time: {elapsed_time} seconds")