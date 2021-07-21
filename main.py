import asyncio
import os
import sqlite3
from email.message import EmailMessage
import aiosmtplib


connect = sqlite3.connect('contacts.db')
cursor = connect.cursor()
emails = [(first_name, email) for first_name, email in cursor.execute("SELECT first_name, email FROM contacts").
          fetchall()]


async def send_messages(work_queue):

    while not work_queue.empty():
        data = await work_queue.get()
        message = EmailMessage()
        message["From"] = os.getenv('MY_EMAIL')
        message["To"] = data[1]
        message["Subject"] = "Благодарность"
        message.set_content(f"Уважаемый {data[0]}!\n"
                            "Спасибо, что пользуетесь нашим сервисом объявлений.")
        try:
            await aiosmtplib.send(message,
                                  hostname="smtp.mail.ru",
                                  port=465, use_tls=True,
                                  password=os.getenv('MY_PASSWORD'),
                                  username=os.getenv('MY_EMAIL'))
        except:
            pass
        print(f"Message for {data[0]} was send on {data[1]}")


async def main():
    work_queue = asyncio.Queue()
    for data in emails:
        await work_queue.put(data)
    await asyncio.gather(*[(asyncio.create_task(send_messages(work_queue))) for _ in range(6)])

if __name__ == "__main__":
    asyncio.run(main())
