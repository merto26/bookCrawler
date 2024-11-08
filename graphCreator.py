import matplotlib.pyplot as plt

# ============================================================================
# || Programm zur Erstellung von Diagrammen für die Test/Scraping Prozesse  ||
# ============================================================================

# ||  Erstellung eines Balkendiagramms für Effizienztests auf einer festen Datenmenge  ||
def createStackedBarGraph(x, y1, y2):
    """
    Erstellt ein gestapeltes Balkendiagramm für .
    :param x: X-Achsen-Beschriftung für modifizierte und nicht modifizierte Datenbanken.
    :param y1: Liste mit MySQL-Zeiten, wobei der erste Wert die modifizierte und der zweite die nicht modizierte
    Datenbank darstellt.
    :param y2: Liste mit MongoDB-Zeiten, wobei der erste Wert die modifizierte und der zweite die nicht modizierte
    Datenbank darstellt.
    """
    fig, ax = plt.subplots(figsize=(10, 6))

# || Logik zum Sortieren der Graphen, so dass der kleinere Balken immer vor dem größeren steht. ||
    for i in range(len(x)):
        if y1[i] > y2[i]:
            ax.bar(x[i], y1[i], color='b')
            ax.bar(x[i], y2[i], color='r')
        else:
            ax.bar(x[i], y2[i], color='r')
            ax.bar(x[i], y1[i], color='b')


    # || Unsichtbare Balken --> dienen der korrekten Generierung der Legende. ||
    mysql_bar = plt.Rectangle((0, 0), 1, 1, color='b', label='MySQL')
    mongodb_bar = plt.Rectangle((0, 0), 1, 1, color='r', label='MongoDB')

    ax.set_ylabel('Zeit (Sekunden)')
    ax.legend(handles=[mysql_bar, mongodb_bar])
    ax.yaxis.grid(True)

    table_data = []
    for i in range(len(x)):
        table_data.append([f'{y1[i]:.2f}', f'{y2[i]:.2f}'])

    table = plt.table(cellText=table_data,
                      rowLabels=x,
                      colLabels=['MySQL (Sekunden)', 'MongoDB (Sekunden)'],
                      loc='bottom',
                      cellLoc='center',
                      bbox=[0, -0.35, 1, 0.2])

    table.scale(1, 1.5)
    plt.subplots_adjust(left=0.2, bottom=0.3)
    plt.show()

# ||  Erstellung eines Liniendiagramms für Effizienztests auf Datenmengen von 1-5 Millionen  ||

def createLineGraph(list1, list2, list3, list4, type):
    """
    Programm zur Erstellung eines Liniengraphen mit vier Linien, jede Linie steht für eine Datenbankart.
    :param list1: Liste mit Zeiten von den MySQL-Datenbanken die in den Graphen eingetragen werden sollen.
    :param list2: Liste mit Zeiten von den modifizierten MySQL-Datenbanken die in den Graphen eingetragen werden sollen.
    :param list3: Liste mit Zeiten von den MongoDB-Datenbanken die in den Graphen eingetragen werden sollen.
    :param list4: Liste mit Zeiten von den modifizierten MongoDB-Datenbanken die in den Graphen eingetragen werden sollen.
    :param type: Angabe des Modifikationsfaktor.
    """
    if not (len(list1) == len(list2) == len(list3) == len(list4)):
        print("All lists must be of the same length.")
        return

    x_values = range(1, len(list1) + 1)

    fig, ax = plt.subplots(figsize=(8, 5))

    max_y_value = max(max(list1), max(list2), max(list3), max(list4))
    ax.set_ylim(0, max_y_value)
    ax.set_xlim(1, len(x_values))
    ax.plot(x_values, list1, label='MySQL')
    ax.plot(x_values, list2, label=f'MySQL {type}')
    ax.plot(x_values, list3, label='MongoDB')
    ax.plot(x_values, list4, label=f'MongoDB {type}')

    ax.set_xlabel('Datensätze in Millionen')
    ax.set_ylabel('Zeit in Sekunden')
    ax.set_xticks(range(1, len(list1) + 1))
    ax.legend()
    ax.grid(True)

    table_data = []
    for i in range(len(list1)):
        table_data.append([f'{list1[i]:.2f}', f'{list2[i]:.2f}', f'{list3[i]:.2f}', f'{list4[i]:.2f}'])

    table = plt.table(cellText=table_data,
                      rowLabels=[f'{i+1} Mio' for i in range(len(list1))],
                      colLabels=['MySQL', f'MySQL {type}', 'MongoDB', f'MongoDB {type}'],
                      loc='bottom',
                      cellLoc='center',
                      bbox=[0, -0.6, 1, 0.4])

    table.scale(1, 2)
    plt.subplots_adjust(left=0.1, bottom=0.4, right = 0.95 , top = 0.95)
    plt.show()

# ||  Erstellung eines Liniendiagramms für den Scrapingprozess bei 5 Millionen Büchern  ||
def createLineGraph(times):
    """
    Programm zur Erstellung eines Liniengraphen mit einer Linie ohne y-Achsen-Limit.
    :param times: Liste mit Scraping-Zeiten die in den Graphen eingetragen werden sollen.
    """
    x_values = [i * 100 for i in range(1, len(times) + 1)]
    even_ticks = [x for x in x_values if x % 2 == 0]
    plt.xticks(even_ticks)
    plt.plot(x_values, times, marker='o')
    plt.xlabel("Anzahl HTML-Dateien mit 5000 Büchern")
    plt.ylabel("Zeit in Sekunden")
    plt.grid(True)
    plt.show()

def createLineGraphSingleHTML(times):
    """
    Programm zur Erstellung eines Liniengraphen mit einer Linie und y-Achsen-Limit für
    die Zeiten des Scrapers welcher jeweils eine große HTML-Datei scraped.
    :param times: Liste mit Scraping-Zeiten die in den Graphen eingetragen werden sollen.
    """
    x_values = [i * 2 for i in range(1, len(times) + 1)]
    even_ticks = [x for x in x_values if x % 2 == 0]
    plt.xticks(even_ticks)
    plt.ylim(0, 2100)
    plt.plot(x_values, times, marker='o')
    plt.xlabel("HTML-Datei mit x mal 5000 Büchern")
    plt.ylabel("Zeit in Sekunden")
    plt.grid(True)
    plt.show()

# ||  Erstellung eines Liniendiagramms für den Scrapingprozess  ||
def createLineGraphLimitManyHTML(times):
    """
    Programm zur Erstellung eines Liniengraphen mit einer Linie und y-Achsen-Limit für
    die Zeiten des Scrapers welcher jeweils kleine HTML-Datei a 5000 Bücher scraped.
    :param times: Liste mit Scraping-Zeiten die in den Graphen eingetragen werden sollen.
    """
    x_values = [i * 2 for i in range(1, len(times) + 1)]
    even_ticks = [x for x in x_values if x % 2 == 0]
    plt.xticks(even_ticks)
    plt.ylim(0, 2100)
    plt.plot(x_values, times, marker='o')
    plt.xlabel("Anzahl HTML-Dateien mit 5000 Büchern")
    plt.ylabel("Zeit in Sekunden")
    plt.grid(True)
    plt.show()