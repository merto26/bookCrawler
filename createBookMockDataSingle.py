import random
from faker import Faker
import time

faker = Faker()

def createMockData(size, output):
    """
    Erstellt eine HTML-Datei mit generierten Büchern als Testdaten.
    :param size: Integer der die Menge an Büchern angibt.
    :param output: String des Pfads in dem die HTML-Datei erzeugt werden soll.
    """

    html_template = '''
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Books to Scrape</title>
        <style>
            body {{
                font-family: Arial, sans-serif;
                background-color: #f4f4f4;
                margin: 0;
                padding: 0;
            }}
            .container {{
                width: 80%;
                margin: 0 auto;
                padding: 20px;
            }}
            h1 {{
                text-align: center;
                color: #333;
            }}
            ul.book-list {{
                list-style-type: none;
                padding: 0;
            }}
            li.book {{
                background-color: white;
                border: 1px solid #ddd;
                width: 200px;
                padding: 10px;
                box-shadow: 0 2px 5px rgba(0, 0, 0, 0.1);
                margin-bottom: 20px;
                transition: transform 0.3s;
            }}
            li.book:hover {{
                transform: scale(1.05);
            }}
            li.book img {{
                width: 100%;
                height: auto;
            }}
            .book-title {{
                font-size: 16px;
                font-weight: bold;
                margin-top: 10px;
                height: 50px;
                overflow: hidden;
            }}
            .book-genre, .book-price, .book-availability, .book-tax {{
                font-size: 14px;
                margin: 5px 0;
            }}
            .book-rating {{
                font-size: 14px;
                color: #f39c12;
            }}
            .book-description {{
                font-size: 12px;
                color: #777;
                margin-top: 10px;
                height: 80px;
                overflow: hidden;
            }}
            .footer {{
                text-align: center;
                padding: 20px;
                background-color: #333;
                color: white;
                margin-top: 20px;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Books to Scrape</h1>
            <ul class="book-list">
                {books}
            </ul>
        </div>
        <div class="footer">
            <p>&copy; 2024 Books to Scrape</p>
        </div>
    </body>
    </html>
    '''

    genres = ["Fiction", "Nonfiction", "Poetry", "History", "Science Fiction", "Fantasy", "Mystery", "Young Adult",
              "Children's", "Romance"]
    ratings = ["One", "Two", "Three", "Four", "Five"]

    bookcounter = 1
    books_html = ''
    while bookcounter < size+1:
        price = round(random.uniform(10.0, 100.0), 2)
        tax = (price/100.0)*19
        book_html = f'''
        <li class="book">
            <img src="https://via.placeholder.com/150" alt="Book cover">
            <div class="book-title">Book {bookcounter}</div>
            <div class="book-genre">Genre: {random.choice(genres)}</div>
            <div class="book-price">Price: {price + tax}</div>
            <div class="book-tax">Price (without tax): {price}</div>
            <div class="book-availability">Availability: {random.randint(1, 100)} in stock</div>
            <div class="book-rating">Rating: {random.choice(ratings)}</div>
            <div class="book-description">Description of Book {bookcounter}. This is a randomly generated book description.</div>
        </li>
        '''
        books_html += book_html
        print(f"Buch {bookcounter} wurde grade gemocked! Gut gemacht Mert!")
        bookcounter += 1


    final_html = html_template.format(books=books_html)

    output_html_file = f'C:\\Users\\mert-\\PycharmProjects\\bookCrawlerProject\\HTML Formulare\\{output}.html'
    with open(output_html_file, 'w', encoding="utf-8") as file:
        file.write(final_html)

    print(f"HTML file has been generated: {output_html_file}")

if __name__ == '__main__':
    startTime = time.time()
    i = 1
    books = 10000
    while i <= 10:
        createMockData(books, f'MockData{i}0K')
        i += 1
        books = books + 10000
    endTime = time.time()
    print(f"Mocking took {endTime - startTime} seconds")