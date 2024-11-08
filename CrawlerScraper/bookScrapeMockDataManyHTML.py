import json
import os
import time
from graphCreator import createLineGraph
from bs4 import BeautifulSoup


def scrapeMockData(folder, input, output):
    """
    Funktion um Daten aus mehreren HTML-Dateien zu scrapen.
    :param folder: String vom Pfad des Ordners welcher die Input HTML-Dateien enthält.
    :param input: String des Namen der Input-HTML-Datei
    :param output: String des Namen der Output-JSON-Datei
    """

    page_counter = 1
    zeit_messungen = []
    kumulative_zeit = 0

    while page_counter < 1001:

        if page_counter%100 == 1:
            start_time = time.time()
        html_file_path = f'C:\\Users\\mert-\\PycharmProjects\\bookCrawlerProject\\HTML Formulare\\{folder}\\{input}{page_counter}.html'


        output_folder = 'C:\\Users\\mert-\\PycharmProjects\\bookCrawlerProject\\JSON Formulare'


        os.makedirs(output_folder, exist_ok=True)


        output_json_file = os.path.join(output_folder, f'{output}.json')
        print("ich lese")

        with open(html_file_path, 'r', encoding='utf-8') as file:
            html_content = file.read()
        print("ich soupe")

        soup = BeautifulSoup(html_content, 'html.parser')
        print("ich habe gesoupt")

        def convert_stars_to_rating(stars):
            """
            Konvertiert die Strings zu den entsprechenden Integer.
            :param stars: String welcher die Bewertung der Bücher wiedergibt.
            :return: Entsprechender Integer zum String.
            """
            rating_map = {
                "One": 1,
                "Two": 2,
                "Three": 3,
                "Four": 4,
                "Five": 5
            }
            return rating_map.get(stars, "No rating")

        books = soup.find_all('li', class_='book')

        counter = 0
        book_data = []
        for book in books:
            title = book.find('div', class_='book-title').get_text(strip=True)
            genre = book.find('div', class_='book-genre').get_text(strip=True).replace('Genre: ', '')
            #Ab hier werden die Daten in angemessene Datentypen konvertiert. Ohne die explizite Konvertierung werden
            #alle Attribute als String in die JSON-Datei eingetragen.
            price_with_tax = float(book.find('div', class_='book-price').get_text(strip=True).replace('Price: ', ''))
            price_without_tax = float(book.find('div', class_='book-tax').get_text(strip=True).replace('Price (without tax): ', ''))
            availability = int(book.find('div', class_='book-availability').
                                      get_text(strip=True).replace('Availability: ', '').replace('in stock', ''))
            rating = convert_stars_to_rating(book.find('div', class_='book-rating').get_text(strip=True).replace('Rating: ', ''))
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

        if page_counter == 1:
            with open(output_json_file, 'w', encoding='utf-8') as json_file:
                json.dump(book_data, json_file, ensure_ascii=False, indent=4)

        with open(output_json_file, 'r') as file:
            data = json.load(file)

        data.extend(book_data)

        with open(output_json_file, 'w', encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=False, indent=4)

        print(f"Scraped data of {input}{page_counter} has been saved to {output_json_file}")

        if page_counter%100 == 0:
            end_time = time.time()
            elapsed_time = end_time - start_time
            kumulative_zeit += elapsed_time
            zeit_messungen.append(kumulative_zeit)

        page_counter += 1

    createLineGraph(zeit_messungen)


if __name__ == '__main__':
    startTime = time.time()
    scrapeMockData('5Mioin5K', 'MockDataPage', 'test1')
    endTime = time.time()
    print(f"Scraping took {endTime - startTime} seconds")