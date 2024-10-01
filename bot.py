import logging
logging.basicConfig(format='[%(levelname) 5s/%(asctime)s] %(name)s: %(message)s',
                    level=logging.WARNING)

import asyncpg

from telethon import TelegramClient, errors
from telethon.tl.types import InputPhoneContact
import asyncio
from telethon.tl.functions.contacts import ImportContactsRequest, DeleteContactsRequest, GetContactsRequest
import time
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import database
from datetime import datetime, timedelta
import re
import threading

from credentials import accounts
import concurrent.futures
import asyncio
clients = {}

scheduler = AsyncIOScheduler()

async def initialize_clients():
    for account in accounts:
        client = TelegramClient(f'sessions/dil_{account["responsible_id"]}', account['api_id'], account['api_hash'])
        await client.start(account['phone_number'])

        clients[account['responsible_id']] = client


DATABASE_CONFIG = {
    'host': "hostname",
    'database': "database-name",
    'user': "username",
    'password': "password",
    'port': "5432"
}


async def get_db_connection():
    conn = await asyncpg.connect(
        user=DATABASE_CONFIG['user'],
        password=DATABASE_CONFIG['password'],
        database=DATABASE_CONFIG['database'],
        host=DATABASE_CONFIG['host'],
        port=DATABASE_CONFIG['port']
    )
    return conn


async def fetch_query(query, params=None):
    conn = await get_db_connection()
    result = await conn.fetch(query, *params if params else [])
    await conn.close()
    return result

async def execute_query(query, params=None):
    conn = await get_db_connection()
    await conn.execute(query, *params if params else [])
    await conn.close()

is_stop = True
lock = asyncio.Lock()

async def add_contact(name, number, respon_id):
    client = clients.get(respon_id)
    try:
        contact = InputPhoneContact(client_id=0, phone=number, first_name=name, last_name='')
        result = await client(ImportContactsRequest([contact]))
        print(f"Contact added: {contact}")
    except errors.FloodWaitError as e:
        # Handle flood wait error
        print(f"Rate limit exceeded. Please wait for {e.seconds} seconds.")
    except errors.UserAlreadyParticipantError:
        # Contact is already added
        print("User is already a contact.")
    except errors.RPCError as e:
        # Handle other API errors
        print(f"An error occurred: {e}")


    contacts = await client(GetContactsRequest(hash=0))
    target_user = next((user for user in contacts.users if user.phone == number), None)
    if target_user:
        contact_user_id = target_user.id if hasattr(target_user, 'id') else None
        username = target_user.username if hasattr(target_user, 'username') else None
        username = f"@{username}" if username else None
        phone = target_user.phone if hasattr(target_user, 'phone') else None
        
        print(f"Target user found: ID={contact_user_id}, Username={username}, Phone={phone}")
    else:
        print(f"No target user found for number: {number}")
        contact_user_id = None
        username = None
        phone = None
    
    if contact_user_id and phone:
        await execute_query("INSERT INTO user_additions (phone_number, username, user_id) VALUES ($1, $2, $3);", (phone, username, str(contact_user_id)))
    elif phone:
        await execute_query("INSERT INTO user_additions (phone_number) VALUES ($1);", (phone,))
    
    return target_user


async def send_message_and_forward_book(user_id, message_text, channel_id, message_id, respon_id,name,phone):
    try:
        client = clients.get(respon_id)
        await client.send_message(entity=user_id, message=message_text)

        await client.forward_messages(entity=user_id, messages=message_id, from_peer=channel_id, drop_author=True)
        
        print(f"Message and book forwarded to user")
        alert_msg = f"message sent to {name} ({phone}) 24h"

        group = await client.get_entity(-1111111) # replace with report group id
        await client.send_message(group, alert_msg)
    except Exception as e:
        print(f"Error sending message to user ID {user_id}: {e}")

async def forward_message_4(user_id,message_text, channel_id, message_id, respon_id):
    try:
        client = clients.get(respon_id)
        await client.send_message(user_id, message_text)

        await client.forward_messages(user_id, message_id, from_peer=channel_id, drop_author=True)
        await asyncio.sleep(1)
        await client.forward_messages(user_id, 18, from_peer=channel_id, drop_author=True)
        await asyncio.sleep(1)
        await client.forward_messages(user_id, 19, from_peer=channel_id, drop_author=True)
        
        print(f"Message and book forwarded to user")
    except Exception as e:
        print(f"Error sending message to user ID {user_id}: {e}")

async def get_operators_count():
    operators = await fetch_query('SELECT responsible_id FROM operators;')
    return operators

async def get_unassigned_users():
    unassigned_users = await fetch_query("SELECT id FROM users WHERE responsible_id IS NULL;")
    return unassigned_users
    
async def get_operator_user_counts():
    operator_users = await fetch_query("SELECT responsible_id, COUNT(id) FROM users WHERE responsible_id IS NULL GROUP BY responsible_id;")
    count = {}
    for row in operator_users:
        count[row['responsible_id']] = row['count']
    return count


async def assign_task_to_operator():
    operators = await get_operators_count()
    operator_users_count = await get_operator_user_counts()
    users = await get_unassigned_users()

    for operator in operators:
        if operator['responsible_id'] not in operator_users_count:
            operator_users_count[operator['responsible_id']] = 0
    
    operator_count = len(operators)
    index = 0

    for user_id in users:
        min_count = min(operator_users_count.values())
        for operator in operators:
            if operator_users_count[operator['responsible_id']] == min_count:
                selected_operator = operator
                break
        
        await execute_query("UPDATE users SET responsible_id = $1 WHERE id = $2;", (selected_operator['responsible_id'], user_id['id']))
        print(f"User {user_id} assigned to operator {selected_operator['responsible_id']}")
        operator_users_count[selected_operator['responsible_id']] += 1
        await asyncio.sleep(1)

async def send_twenty_hour():
    try:
        users = await fetch_query("SELECT u.id, u.name, u.phone_number, u.responsible_id, m.msg_link, ua.username, ua.user_id FROM users as u, messages as m, user_additions as ua WHERE u.phone_number = ua.phone_number AND u.cur_msg_id = m.msg_id AND is_qualitative = true")
        pattern = r"https://t\.me/c/1111111/(\d+)" # replace 111111 with channel id, this means, we save books in private channel.

        for user in users:
            name = user['name']
            responsible_id = user['responsible_id']
            username = user['username']
            user_id = user['user_id']
            url = user['msg_link']
            await asyncio.sleep(1)

            match = re.search(pattern, url)
            if match:
                msg_id = int(match.group(1))

            if username is not None:
                try:
                    message_text = f"Assalomu alaykum, hurmatli {str(name)}!"
                    await send_message_and_forward_book(username, message_text, -100$11111111, msg_id, responsible_id, name, user['phone_number']) #replace $111111 with your channel that is save data
                except Exception as e:
                    print(f"Error sending message to user ID {user['user_id']}: {e}")
            elif username is None and user_id is not None:
                try:
                    user_id = int(user_id)
                    message_text = f"Assalomu alaykum, hurmatli {str(name)}!"
                    await send_message_and_forward_book(user_id, message_text, -100$111111111, msg_id, respon_id=responsible_id, name=name, phone=user['phone_number'])
                except Exception as e:
                    print(f"Error sending message to user ID {user['user_id']}: {e}")
            await asyncio.sleep(2)
        

            current_time = datetime.now()
            await asyncio.sleep(1)
            
            await execute_query('UPDATE users SET updated_at = $1, cur_msg_id = 2 WHERE id = $2', (current_time, user['id']))
            await asyncio.sleep(60)

                    
    except Exception as e:
        print(f"Error sending messages: {e}")


async def send_books():

    try:
        user_count = await fetch_query('SELECT COUNT(id) FROM users WHERE sent = false;')
        cnt_users = user_count[0]['count']

        if cnt_users > 0:
            rows = await fetch_query('SELECT u.id, u.phone_number, u.name, b.telegram_channel_msg_link , u.responsible_id FROM users as u, books as b WHERE u.book_id = b.book_id and u.sent = false;')
            pattern = r"https://t\.me/c/111111111/(\d+)"

            for row in rows:
                print(row)
                contact = await add_contact(row['name'], row['phone_number'], row['responsible_id'])
                res_id = row['responsible_id']
                print(contact)
                print(row['phone_number'])
                print(row['name'])

                url = row[3]
                await asyncio.sleep(1)

                match = re.search(pattern, url)
                if match:
                    msg_id = int(match.group(1))
                if not contact:
                    await execute_query('UPDATE users SET is_qualitative = false WHERE id = $1', (row['id'],))
                    await execute_query('UPDATE users SET sent = true WHERE id = $1', (row['id'],))
                    client = clients.get(res_id)
                    group = await client.get_entity(-111111111)
                    await client.send_message(group, f"I cannot add this user to contacts: {row['phone_number']} ({row['name']})")
                    continue

                else:
                    await execute_query('UPDATE users SET is_qualitative = true WHERE id = $1', (row['id'],))
                
                await asyncio.sleep(1)
                client = clients.get(res_id)
                message_text = f"Assalomu alaykum {str(row['name'])}"
                await send_message_and_forward_book(contact, message_text, -100$11111111, msg_id, res_id)
                
                alert_msg = f"Book sent to {row['name']} ({row['phone_number']})"

                group = await client.get_entity(-11111111) # replace 111111 with report group id, bot notice about process
                await client.send_message(group, alert_msg)
                
                print(alert_msg)

                await asyncio.sleep(1)
                await execute_query("UPDATE users SET sent = true WHERE id = $1", (row['id'],))
                await client(DeleteContactsRequest([contact]))
                await asyncio.sleep(60)
        else:
            client = clients.get(res_id)
            group = await client.get_entity(-1111111111)  # replace 111111 with report group id, bot notice about processx
            await client.send_message(group, f"No users to send books to.")
    except Exception as e:
        print(f"Error adding contacts: {e}")


async def forward_messages_to_users():
    try:
        get_cnt = await fetch_query("SELECT COUNT(id) FROM users WHERE created_at >= NOW() - INTERVAL '24 hours';")
        count = get_cnt[0]['count']
        print(count)
        if int(count) > 0:

            rows = await fetch_query("SELECT u.id, u.phone_number, u.name, u.created_at, u.is_qualitative, u.cur_msg_id, u.updated_at, u.responsible_id, m.msg_link, ch.phone_num FROM users as u, messages as m, checked_users as ch WHERE u.cur_msg_id = m.msg_id and u.is_qualitative = true and u.phone_number != ch.phone_num;")
            pattern = r"https://t\.me/c/11111111/(\d+)" #replace 1111111 with channel_id where saving books or something
            current_time = datetime.now()
            print(current_time)
            for row in rows:
                
                name = row['name']
                phone_number = row['phone_number']
                cur_msg_id = row['cur_msg_id']
                created_at = row['created_at']
                updated_at = row['updated_at']
                msg_link = row['msg_link']
                responsible_id = row['responsible_id']
                client = clients.get(responsible_id)

                
                await asyncio.sleep(1)
                print(f"Current time: {current_time}")
                print(f"Created at: {created_at}")
                print(f"difference: {current_time - created_at} and {timedelta(hours=24)}")
                if current_time - created_at >= timedelta(hours=24) and  current_time - created_at <= timedelta(hours=30) and cur_msg_id == 1:

                    contact = await fetch_query("SELECT username, user_id FROM user_additions WHERE phone_number = $1;", (str(phone_number),))
                    url = msg_link
                    await asyncio.sleep(1)

                    match = re.search(pattern, url)
                    if match:
                        msg_id = int(match.group(1))

                    if contact['username'] is not None:
                        try:
                            message_text = f"Assalomu alaykum, hurmatli {str(name)}!"
                            await send_message_and_forward_book(contact, message_text, -1002151076535, msg_id)
                        except Exception as e:
                            print(f"Error sending message to user ID {contact['user_id']}: {e}")
                    elif contact['username'] is None and contact['user_id'] is not None:
                        try:
                            message_text = f"Assalomu alaykum, hurmatli {str(name)}!"
                            await send_message_and_forward_book(contact['user_id'], message_text, -1002151076535, msg_id)
                        except Exception as e:
                            print(f"Error sending message to user ID {contact['user_id']}: {e}")
                    elif contact['username'] is None and contact['user_id'] is None:
                        message_text = f"Assalomu alaykum, hurmatli {str(name)}!"
                        contact = await add_contact(str(name), str(phone_number))
                        await send_message_and_forward_book(contact, message_text, -100$1111111, msg_id) # $11111111 is channel id(take book from this private channel)


                    
                    alert_msg = f"message sent to {phone_number} ({name}) 24h"

                    group = await client.get_entity(-4218215589) # report group id
                    await client.send_message(group, alert_msg)
                    
                    await execute_query('UPDATE users SET updated_at = $1, cur_msg_id = 2 WHERE id = $2', (current_time, row['id']))

                    print(alert_msg)
                    try:
                        await client(DeleteContactsRequest([contact]))
                    except Exception as e:
                        print(f"Error deleting contact: {e}")

                elif updated_at is not None and current_time - updated_at >= timedelta(hours=24) and  current_time - updated_at <= timedelta(hours=30) and cur_msg_id == 2:

                    contact = await add_contact(str(name), str(phone_number))
                    url = msg_link
                    await asyncio.sleep(1)

                    match = re.search(pattern, url)
                    if match:
                        msg_id = int(match.group(1))

                    if contact['username'] is not None:
                        try:
                            message_text = f"Assalomu alaykum, hurmatli {str(name)}!"
                            await send_message_and_forward_book(contact, message_text, -1002151076535, msg_id)
                        except Exception as e:
                            print(f"Error sending message to user ID {contact['user_id']}: {e}")
                    elif contact['username'] is None and contact['user_id'] is not None:
                        try:
                            message_text = f"Assalomu alaykum, hurmatli {str(name)}!"
                            await send_message_and_forward_book(contact['user_id'], message_text, -1002151076535, msg_id)
                        except Exception as e:
                            print(f"Error sending message to user ID {contact['user_id']}: {e}")
                    elif contact['username'] is None and contact['user_id'] is None:
                        message_text = f"Assalomu alaykum, hurmatli {str(name)}!"
                        contact = await add_contact(str(name), str(phone_number))
                        await send_message_and_forward_book(contact, message_text, -1002151076535, msg_id)
                
                    alert_msg = f"message sent to {name} ({phone_number}) 24h"

                    group = await client.get_entity(-4218215589)
                    await client.send_message(group, alert_msg)
                    
                    await execute_query('UPDATE users SET updated_at = $1, cur_msg_id = 3 WHERE id = $2', (current_time, row['id']))

                    print(alert_msg)
                    try:
                        await client(DeleteContactsRequest([contact]))
                    except Exception as e:
                        print(f"Error deleting contact: {e}")

                elif updated_at is not None and current_time - updated_at >= timedelta(hours=48) and current_time - updated_at <= timedelta(hours=54) and cur_msg_id == 3:

                    contact = await add_contact(str(name), str(phone_number))
                    url = msg_link
                    await asyncio.sleep(1)

                    match = re.search(pattern, url)
                    if match:
                        msg_id = int(match.group(1))

                    if contact['username'] is not None:
                        try:
                            message_text = f"Assalomu alaykum, hurmatli {str(name)}!"
                            await send_message_and_forward_book(contact, message_text, -1002151076535, msg_id)
                        except Exception as e:
                            print(f"Error sending message to user ID {contact['user_id']}: {e}")
                    elif contact['username'] is None and contact['user_id'] is not None:
                        try:
                            message_text = f"Assalomu alaykum, hurmatli {str(name)}!"
                            await send_message_and_forward_book(contact['user_id'], message_text, -1002151076535, msg_id)
                        except Exception as e:
                            print(f"Error sending message to user ID {contact['user_id']}: {e}")
                    elif contact['username'] is None and contact['user_id'] is None:
                        message_text = f"Assalomu alaykum, hurmatli {str(name)}!"
                        contact = await add_contact(str(name), str(phone_number))
                        await send_message_and_forward_book(contact, message_text, -1002151076535, msg_id)
                
                    alert_msg = f"message sent to {name} ({phone_number}) 48h"

                    group = await client.get_entity(-4218215589)
                    await client.send_message(group, alert_msg)

                    await execute_query('UPDATE users SET updated_at = $1, cur_msg_id = 4 WHERE id = $2', (current_time, row['id']))

                    print(alert_msg)
                    try:
                        await client(DeleteContactsRequest([contact]))
                    except Exception as e:
                        print(f"Error deleting contact: {e}")

                elif updated_at is not None and current_time - updated_at >= timedelta(hours=24) and current_time - updated_at <= timedelta(hours=30) and cur_msg_id == 4:

                    contact = await add_contact(str(name), str(phone_number))
                    url = msg_link
                    await asyncio.sleep(1)

                    match = re.search(pattern, url)
                    if match:
                        msg_id = int(match.group(1))

                    if contact['username'] is not None:
                        try:
                            message_text = f"Assalomu alaykum, hurmatli {str(name)}!"
                            await send_message_and_forward_book(contact, message_text, -1002151076535, msg_id)
                        except Exception as e:
                            print(f"Error sending message to user ID {contact['user_id']}: {e}")
                    elif contact['username'] is None and contact['user_id'] is not None:
                        try:
                            message_text = f"Assalomu alaykum, hurmatli {str(name)}!"
                            await send_message_and_forward_book(contact['user_id'], message_text, -1002151076535, msg_id)
                        except Exception as e:
                            print(f"Error sending message to user ID {contact['user_id']}: {e}")
                    elif contact['username'] is None and contact['user_id'] is None:
                        message_text = f"Assalomu alaykum, hurmatli {str(name)}!"
                        contact = await add_contact(str(name), str(phone_number))
                        await send_message_and_forward_book(contact, message_text, -1002151076535, msg_id)
                
                    alert_msg = f"message sent to {name} ({phone_number}) 24h"

                    group = await client.get_entity(-4218215589)
                    await client.send_message(group, alert_msg)

                    await execute_query('UPDATE users SET updated_at = $1, cur_msg_id = 5 WHERE id = $2', (current_time, row['id']))

                    print(alert_msg)
                    try:
                        await client(DeleteContactsRequest([contact]))
                    except Exception as e:
                        print(f"Error deleting contact: {e}")

                elif updated_at is not None and current_time - updated_at >= timedelta(hours=24) and current_time - updated_at <= timedelta(hours=30) and cur_msg_id == 5:

                    contact = await add_contact(str(name), str(phone_number))
                    url = msg_link
                    await asyncio.sleep(1)

                    match = re.search(pattern, url)
                    if match:
                        msg_id = int(match.group(1))

                    if contact['username'] is not None:
                        try:
                            message_text = f"Assalomu alaykum, hurmatli {str(name)}!"
                            await send_message_and_forward_book(contact, message_text, -1002151076535, msg_id)
                        except Exception as e:
                            print(f"Error sending message to user ID {contact['user_id']}: {e}")
                    elif contact['username'] is None and contact['user_id'] is not None:
                        try:
                            message_text = f"Assalomu alaykum, hurmatli {str(name)}!"
                            await send_message_and_forward_book(contact['user_id'], message_text, -1002151076535, msg_id)
                        except Exception as e:
                            print(f"Error sending message to user ID {contact['user_id']}: {e}")
                    elif contact['username'] is None and contact['user_id'] is None:
                        message_text = f"Assalomu alaykum, hurmatli {str(name)}!"
                        contact = await add_contact(str(name), str(phone_number))
                        await send_message_and_forward_book(contact, message_text, -1002151076535, msg_id)
                
                    alert_msg = f"message sent to {name} ({phone_number}) 24h"

                    group = await client.get_entity(-4218215589)
                    await client.send_message(group, alert_msg)

                    await execute_query('UPDATE users SET updated_at = $1, cur_msg_id = 6 WHERE id = $2', (current_time, row['id']))

                    print(alert_msg)
                    try:
                        await client(DeleteContactsRequest([contact]))
                    except Exception as e:
                        print(f"Error deleting contact: {e}")

                elif updated_at is not None and current_time - updated_at >= timedelta(hours=24) and current_time - updated_at <= timedelta(hours=30) and cur_msg_id == 6:

                    contact = await add_contact(str(name), str(phone_number))
                    url = msg_link
                    await asyncio.sleep(1)

                    match = re.search(pattern, url)
                    if match:
                        msg_id = int(match.group(1))

                    if contact['username'] is not None:
                        try:
                            message_text = f"Assalomu alaykum, hurmatli {str(name)}!"
                            await send_message_and_forward_book(contact, message_text, -1002151076535, msg_id)
                        except Exception as e:
                            print(f"Error sending message to user ID {contact['user_id']}: {e}")
                    elif contact['username'] is None and contact['user_id'] is not None:
                        try:
                            message_text = f"Assalomu alaykum, hurmatli {str(name)}!"
                            await send_message_and_forward_book(contact['user_id'], message_text, -1002151076535, msg_id)
                        except Exception as e:
                            print(f"Error sending message to user ID {contact['user_id']}: {e}")
                    elif contact['username'] is None and contact['user_id'] is None:
                        message_text = f"Assalomu alaykum, hurmatli {str(name)}!"
                        contact = await add_contact(str(name), str(phone_number))
                        await send_message_and_forward_book(contact, message_text, -1002151076535, msg_id)
                
                    alert_msg = f"message sent to {name} ({phone_number}) 24h"

                    group = await client.get_entity(-4218215589)
                    await client.send_message(group, alert_msg)
                
                    await execute_query('UPDATE users SET updated_at = $1, cur_msg_id = 7 WHERE id = $2', (current_time, row['id']))

                    print(alert_msg)
                    try:
                        await client(DeleteContactsRequest([contact]))
                    except Exception as e:
                        print(f"Error deleting contact: {e}")
                
                elif updated_at is not None and current_time - updated_at >= timedelta(hours=48) and current_time - updated_at <= timedelta(hours=54) and cur_msg_id == 7:

                    contact = await add_contact(str(name), str(phone_number))
                    url = msg_link
                    await asyncio.sleep(1)

                    match = re.search(pattern, url)
                    if match:
                        msg_id = int(match.group(1))

                    if contact['username'] is not None:
                        try:
                            message_text = f"Assalomu alaykum, hurmatli {str(name)}!"
                            await send_message_and_forward_book(contact, message_text, -1002151076535, msg_id)
                        except Exception as e:
                            print(f"Error sending message to user ID {contact['user_id']}: {e}")
                    elif contact['username'] is None and contact['user_id'] is not None:
                        try:
                            message_text = f"Assalomu alaykum, hurmatli {str(name)}!"
                            await send_message_and_forward_book(contact['user_id'], message_text, -1002151076535, msg_id)
                        except Exception as e:
                            print(f"Error sending message to user ID {contact['user_id']}: {e}")
                    elif contact['username'] is None and contact['user_id'] is None:
                        message_text = f"Assalomu alaykum, hurmatli {str(name)}!"
                        contact = await add_contact(str(name), str(phone_number))
                        await send_message_and_forward_book(contact, message_text, -1002151076535, msg_id)
                
                    alert_msg = f"message sent to {name} ({phone_number}) 48h"

                    group = await client.get_entity(-4218215589)
                    await client.send_message(group, alert_msg)

                    await execute_query('UPDATE users SET updated_at = $1, cur_msg_id = 8 WHERE id = $2', (current_time, row['id']))

                    print(alert_msg)
                    try:
                        await client(DeleteContactsRequest([contact]))
                    except Exception as e:
                        print(f"Error deleting contact: {e}")

                elif updated_at is not None and current_time - updated_at >= timedelta(hours=24) and current_time - updated_at <= timedelta(hours=30) and cur_msg_id == 8:
                    
                    contact = await add_contact(str(name), str(phone_number))
                    url = msg_link
                    await asyncio.sleep(1)

                    match = re.search(pattern, url)
                    if match:
                        msg_id = int(match.group(1))

                    if contact['username'] is not None:
                        try:
                            message_text = f"Assalomu alaykum, hurmatli {str(name)}!"
                            await send_message_and_forward_book(contact, message_text, -1002151076535, msg_id)
                        except Exception as e:
                            print(f"Error sending message to user ID {contact['user_id']}: {e}")
                    elif contact['username'] is None and contact['user_id'] is not None:
                        try:
                            message_text = f"Assalomu alaykum, hurmatli {str(name)}!"
                            await send_message_and_forward_book(contact['user_id'], message_text, -1002151076535, msg_id)
                        except Exception as e:
                            print(f"Error sending message to user ID {contact['user_id']}: {e}")
                    elif contact['username'] is None and contact['user_id'] is None:
                        message_text = f"Assalomu alaykum, hurmatli {str(name)}!"
                        contact = await add_contact(str(name), str(phone_number))
                        await send_message_and_forward_book(contact, message_text, -1002151076535, msg_id)
                
                    alert_msg = f"message sent to {name} ({phone_number}) 24h"

                    group = await client.get_entity(-4218215589)
                    await client.send_message(group, alert_msg)
                
                    await execute_query('UPDATE users SET updated_at = $1, cur_msg_id = 9 WHERE id = $2', (current_time, row['id']))

                    print(alert_msg)
                    try:
                        await client(DeleteContactsRequest([contact]))
                    except Exception as e:
                        print(f"Error deleting contact: {e}")
                

                await asyncio.sleep(30)
        else:
            group = await client.get_entity(-4218215589)
            await client.send_message(group, f"No users to send messages to.")
    except Exception as e:
        print(f"Error sending messages: {e}")
    





async def main():
    await initialize_clients()
    await send_twenty_hour()
    await asyncio.Event().wait()

def start_event_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()

def run_async_function(coro):
    asyncio.run_coroutine_threadsafe(coro, loop)

loop = asyncio.new_event_loop()


loop_thread = threading.Thread(target=start_event_loop, args=(loop,), daemon=True)
loop_thread.start()

with concurrent.futures.ThreadPoolExecutor() as executor:
    future = executor.submit(run_async_function, main())

loop_thread.join()

