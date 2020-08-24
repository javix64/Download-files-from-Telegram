from telethon import TelegramClient, events,errors
import asyncio
import psycopg2
import time
import logging
import random
from telethon.tl.types import InputMessagesFilterDocument
from tqdm import tqdm

logging.basicConfig(level=logging.WARNING)

async def Main():
    # Use your own values from my.telegram.org
    api_id = XXXXX
    api_hash = 'XXXXXX'

    # The first parameter is the .session file name (absolute paths allowed)
    client = TelegramClient('test1', api_id, api_hash)
   


    await client.start()

    #await client.get_entity(PeerChat(218931158))
    s = await client.get_dialogs()
    s = await client.get_entity("Amigos de las Letras")
    async for message in client.iter_messages(1022216648, filter=InputMessagesFilterDocument):

        doc_id = message.document.id

        s = selec_Books(doc_id)

        if s ==False:
            name = message.file.name
            size = message.document.size
            path ="F:\Libros" + "\{}".format(name)
            try:
                book = await client.download_file(message,path)
            except errors.FloodWaitError as e:
                num = random.randint(1,100)
                final_int =e.seconds+num
                for i in tqdm(range(final_int)):
                    await asyncio.sleep(1)
                    continue
            except OSError as e:
                print("No podemos descargar el libro {}".format(name))
                size = 0
                path = ""
                continue


            insert_Book(doc_id,name,path,size)


    print("Todos los libros descargados")
    await client.run_until_disconnected()


def selec_Books(id):

    try:

        # connect to the PostgreSQL server
        conn = psycopg2.connect(user="postgres",
                                password="XXXXXXXXX",
                                host="127.0.0.1",
                                port="5432",
                                database="books")
        cur = conn.cursor()
        # create table one by one
        query = """ SELECT * FROM books_info where {}=(doc_id)""".format(id)

        cur.execute(query)
        records = cur.fetchall()
        if not records:
            return False
        if records:
            return True

        # commit the changes
    except (Exception, psycopg2.Error) as error:
        if conn:
            print("Failed to insert record into mobile table", error)

    finally:
        # closing database connection.
        if (conn):
            cur.close()
            conn.close()

def insert_Book(doc_id,name,path,size):

    try:

        # connect to the PostgreSQL server
        conn = psycopg2.connect(user="postgres",
                                password="XXXXXXXX",
                                host="127.0.0.1",
                                port="5432",
                                database="books")
        cur = conn.cursor()
        # create table one by one
        postgres_insert_query = """ INSERT INTO books_info (doc_id, name, path,size_kb) VALUES (%s,%s,%s,%s)"""
        record_to_insert = (doc_id, name, path,size)
        cur.execute(postgres_insert_query, record_to_insert)
        conn.commit()
        cur.close()
        # commit the changes
    except (Exception, psycopg2.Error) as error:
        if (conn):
            print("Failed to insert record into mobile table", error)

    finally:
        # closing database connection.
        if (conn):
            cur.close()
            conn.close()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(Main())
    loop.close()