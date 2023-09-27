import mechanicalsoup
import pandas as pd
import sqlite3

url = "https://en.wikipedia.org/wiki/List_of_songs_by_Taylor_Swift"
browser = mechanicalsoup.StatefulBrowser()
browser.open(url)

th = browser.page.find_all("th")
songs = [value.text.replace("\n", "") for value in th]
songs = songs[7:259]

td = browser.page.find_all("td")
columns = [value.text.replace("\n", "") for value in td]
columns = columns[3:1262]
# inserting a value because I was getting an error when creating the dataframe,
# so I looked back at the table in wikipedia and one row did not have a value for Ref.
# so I am inserting it here
columns.insert(144, '[20]')

column_names = ["Artist",
                "Writer",
                "Album",
                "Year",
                "Reference"]

dictionary = {"Song": songs}

for idx, key in enumerate(column_names):
    dictionary[key] = columns[idx:][::5]

# print(dictionary)

df = pd.DataFrame(data=dictionary)
print(df.head())
print(df.tail())

connection = sqlite3.connect("songs.db")
cursor = connection.cursor()
cursor.execute("create table songs (Song, " + ",".join(column_names) + ")")

for i in range(len(df)):
    cursor.execute(
        "insert into songs values (?,?,?,?,?,?)", df.iloc[i])

connection.commit()

connection.close()
