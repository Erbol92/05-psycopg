import psycopg2


def search_user_from_email(email: str):
    # получаем пользователя по почте
    find_sql = """SELECT * FROM Users WHERE email=%s"""
    cur.execute(find_sql, (email,))
    data = cur.fetchone()
    return data


def connect_to_db(password: str = 'postgres', host: str = 'localhost', port: int = 5432):
    try:
        # коннектримся к бд постгри (по умолчанию)
        conn = psycopg2.connect(
            dbname='postgres', user="postgres", password=password, host=host, port=port)
        conn.autocommit = True
        cur = conn.cursor()
        # проверяем ниличие бд clients
        cur.execute("""
                    SELECT 1 FROM pg_database WHERE datname = %s;
                    """, ('clients',))
        exists = cur.fetchall()
        if not exists:
            print("База данных 'clients' не существует. Создаем...")
            cur.execute('CREATE DATABASE clients')
        else:
            print("База данных 'clients' сущетсвует. Подключемся... ")
        cur.close()
        conn.close()
        # подключаемся в бд clients
        conn = psycopg2.connect(
            dbname='clients', user="postgres", password=password, host=host, port=port)
        print('подключение успешно')
        return conn
    except Exception as e:
        print('ошибка при подключении', e)
        return False


# Функция, создающая структуру БД (таблицы).
# читаем скрипт из файла и выполняем
def create_tables():
    with open(file='scripts\\create_script.sql', mode='r') as f:
        text = f.read()
    try:
        cur.execute(text)
    except Exception as e:
        print(e)
    conn.commit()


# Функция, позволяющая добавить нового клиента.
def create_new_client(first_name: str, last_name: str, email: str):
    sql = """
            INSERT INTO Users(first_name,last_name,email) VALUES(%s,%s,%s);
            """
    cur.execute(sql, (first_name, last_name, email))
    conn.commit()
    print(f'клиент {first_name} {last_name} добавлен')


# Функция, позволяющая добавить телефон для существующего клиента.
def add_phone_client(email: str, phones: list):
    data = search_user_from_email(email)
    print(data)
    if data:
        user_id, user_name = data[0], f'{data[1]} {
            data[2]}'
        insert_sql = """
                    INSERT INTO Phone(phone,user_id) VALUES(%s,%s);
                    """
        for phone in phones:
            cur.execute(insert_sql, (int(phone[1:]), user_id))
            print(f'клиенту {user_name} добавлен номер тел. {phone}')
        conn.commit()

# Функция, позволяющая изменить данные о клиенте.


def update_client(email: str, new_email: str = None, first_name: str = None, last_name: str = None, phones: list = None):
    # получаем пользователя по почте
    data = search_user_from_email(email)
    # находим какие столбцы будем менять и запоминаем данные
    if data:
        user_id = data[0]
        updates, params = [], []
        if first_name:
            updates.append('first_name=%s')
            params.append(first_name)
        if last_name:
            updates.append('last_name=%s')
            params.append(last_name)
        if new_email:
            updates.append('email=%s')
            params.append(new_email)
        # в номерах находим только новые номера и добавляем их в таблицу
        if phones:
            phones = [a[1:] for a in phones]
            cur.execute(
                """
                SELECT phone FROM Phone WHERE user_id=%s;
                """, (user_id,))
            user_phones = [str(phone[0]) for phone in cur.fetchall()]
            dif_user_phones = set(phones)-set(user_phones)
            if dif_user_phones:
                print('добавляем отличающиеся номера')
                for phone in dif_user_phones:
                    cur.execute("""
                                INSERT INTO Phone(phone,user_id) VALUES(%s,%s);
                                """, (int(phone), user_id))
            else:
                print('номера уже есть у клиента')

        if params:
            params.append(user_id)
            update_sql = f"UPDATE Users SET {', '.join(updates)} WHERE id=%s"
            cur.execute(update_sql, params)

        conn.commit()

# Функция, позволяющая удалить телефон для существующего клиента.


def delete_phone(email: str, phones: list):
    data = search_user_from_email(email)
    if data and phones:
        user_id = data[0]
        phones = [a[1:] for a in phones]
        cur.execute(
            """
                SELECT phone FROM Phone WHERE user_id=%s;
                """, (user_id,)
        )
        user_phones = [str(phone[0]) for phone in cur.fetchall()]
        # ищем совпадение номеров которые хотим удалить у данного пользователя
        dif_user_phones = set(phones) & set(user_phones)
        if dif_user_phones:
            for phone in dif_user_phones:
                cur.execute("DELETE FROM Phone WHERE phone=%s;", (phone,))
        conn.commit()
        print('удалены телефоны ', dif_user_phones)

# Функция, позволяющая удалить существующего клиента.


def delete_user(email: str):
    cur.execute(""" DELETE FROM Users WHERE email=%s""", (email,))
    conn.commit()
    print('удаление успешно')

# Функция, позволяющая найти клиента по его данным: имени, фамилии, email или телефону


def find_user(email: str = None,  first_name: str = None, last_name: str = None, phone: str = None):

    if phone:
        phone = int(phone[1:])
        sql = """
                SELECT * FROM Users U
                JOIN Phone P ON P.user_id=U.id
                WHERE P.phone=%s
            """
        cur.execute(sql, (phone,))
        return print(cur.fetchone())
    data, params = [], []
    if email:
        params.append('email=%s')
        data.append(email)

    if first_name:
        params.append('first_name=%s')
        data.append(first_name)

    if last_name:
        params.append('last_name=%s')
        data.append(last_name)
    sql = f"""SELECT * FROM Users WHERE {' OR '.join(params)}"""
    cur.execute(sql, data)
    print(cur.fetchone())

    return print(sql)


conn = connect_to_db(password='postgres', host='localhost', port=5432)

if conn:
    cur = conn.cursor()

    create_tables()

    create_new_client('Erbol', 'Rustemov', 'erbolbaik@mail.ru')

    add_phone_client('erbolbaik@mail.ru',
                     ['+71234567890', '+77775558888', '+77776668899'])

    update_client('erbolbaik@mail.ru', first_name='Bobby', new_email='baikerbol@yandex.ru',
                  phones=['+71234567890', '+77775558888', '+77776668898'])
    delete_phone('baikerbol@yandex.ru',
                 ['+71223567890', '+77775558888', '+77776668898'])

    delete_user('baikerbol@yandex.ru')

    find_user('erbolbaik@mail.ru', first_name='Bobby', last_name='Rustemov')

    find_user(phone='+77776668899')

    cur.close()
    conn.close()
