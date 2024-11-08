import json
import os
from bs4 import BeautifulSoup
import time
import matplotlib.pyplot as plt
from graphCreator import createLineGraphSingleHTML
def scrapeMockData(input, output):
    """
    Funktion die Mockdaten aus einer HTML-Datei scraped.
    :param input: String vom Pfad der Datei, welche gescraped werden soll.
    :param output: String vom Pfad des Ordners in den die gescrapeten Daten in Form einer JSON-Datei gespeichert werden
    sollen.
    """

    html_file_path = f'C:\\Users\\mert-\\PycharmProjects\\bookCrawlerProject\\HTML Formulare\\{input}.html'
    output_folder = 'C:\\Users\\mert-\\PycharmProjects\\bookCrawlerProject\\JSON Formulare'


    os.makedirs(output_folder, exist_ok=True)

    output_json_file = os.path.join(output_folder, f'{output}.json')

    with open(html_file_path, 'r', encoding='utf-8') as file:
        html_content = file.read()

    soup = BeautifulSoup(html_content, 'html.parser')

    def convert_stars_to_rating(stars):
        rating_map = {
            "★☆☆☆☆": "One",
            "★★☆☆☆": "Two",
            "★★★☆☆": "Three",
            "★★★★☆": "Four",
            "★★★★★": "Five"
        }
        return rating_map.get(stars, "No rating")

    books = soup.find_all('li', class_='book')

    counter = 0
    book_data = []
    for book in books:
        title = book.find('div', class_='book-title').get_text(strip=True)
        genre = book.find('div', class_='book-genre').get_text(strip=True).replace('Genre: ', '')
        # Ab hier können die Daten in angemessene Datentypen konvertiert werden.
        # Ohne explizite Konvertierung werden alle Attribute als String in die JSON-Datei eingetragen.
        price_with_tax = book.find('div', class_='book-price').get_text(strip=True).replace('Price: ', '')
        price_without_tax = book.find('div', class_='book-tax').get_text(strip=True).replace('Price (without tax): ', '')
        availability = book.find('div', class_='book-availability').get_text(strip=True).replace('Availability: ', '')
        rating = convert_stars_to_rating(
            book.find('div', class_='book-rating').get_text(strip=True).replace('Rating: ', ''))
        description = book.find('div', class_='book-description').get_text(strip=True).replace('...', '')

        book_data.append({
            'title': title,
            'genre': genre,
            'price_with_tax': price_with_tax,
            'price_without_tax': price_without_tax,
            'availability': availability,
            'rating': rating,
            'description': description
        })
        counter = counter + 1
        print(counter)

    with open(output_json_file, 'w', encoding='utf-8') as json_file:
        json.dump(book_data, json_file, ensure_ascii=False, indent=4)

    print(f"Scraped data has been saved to {output_json_file}")



if __name__ == '__main__':
    startTime = time.time()

    zeitMessungen = []
    cumulative_time = 0
    i = 1
    while i <= 10:
        start_time = time.time()
        scrapeMockData(f'MockData{i}0K', f'testcrawlMockData{i}0k')
        end_time = time.time()
        elapsed_time = end_time - start_time
        zeitMessungen.append(elapsed_time)
        i += 1
    # X-Achse: Anzahl der Dateien (alle zwei Dateien)
    createLineGraphSingleHTML(zeitMessungen)