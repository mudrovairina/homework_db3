import psycopg2

conn = psycopg2.connect(
    database='netology_db',
    user='postgres',
    password='admin'
)


# Функция, создающая структуру БД (таблицы).
def create_table(connection):
    sql_query = """
        CREATE TABLE IF NOT EXISTS persons(
            id SERIAL PRIMARY KEY,
            name VARCHAR(60) NOT NULL,
            surname VARCHAR(60) NOT NULL,
            email VARCHAR(80) UNIQUE NOT NULL
        );
        CREATE TABLE IF NOT EXISTS phones(
            id SERIAL PRIMARY KEY,
            id_person INTEGER NOT NULL REFERENCES persons(id) 
            ON DELETE CASCADE,
            phone_number TEXT UNIQUE NOT NULL
        );
    """
    with connection.cursor() as cur:
        cur.execute(sql_query)
        connection.commit()


# Функция, позволяющая добавить нового клиента и телефон
def add_client(connection, name, surname, email, phone=None):
    sql_insert_person = """
        INSERT INTO persons (name, surname, email) 
        VALUES (%s, %s, %s);
    """
    sql_select_id_person = """
        SELECT id FROM persons WHERE email = %s;
    """
    sql_insert_phone = """
        INSERT INTO phones (id_person, phone_number)  
        VALUES (%s, %s);
    """

    with connection.cursor() as cur:
        cur.execute(sql_insert_person, (name, surname, email))
        cur.execute(sql_select_id_person, (email,))
        id_person = cur.fetchone()
        if phone is None:
            pass
        else:
            for tel in phone:
                cur.execute(sql_insert_phone, (id_person, tel))
        connection.commit()


# Функция, позволяющая добавить телефон для существующего клиента.
def add_phone(connection, email, phone):
    sql_select_id_person = """
        SELECT id FROM persons WHERE email = %s;
    """
    sql_insert_phone = """
        INSERT INTO phones (id_person, phone_number)  
        VALUES (%s, %s);
    """
    with connection.cursor() as cur:
        cur.execute(sql_select_id_person, (email,))
        id_person = cur.fetchone()
        if id_person:
            cur.execute(sql_insert_phone, (id_person, phone))
        else:
            print(f"Клиента c email {email} не существует")
        connection.commit()


# Функция, позволяющая изменить данные о клиенте и телефоне.
def update_data(
        connection,
        required_email,
        changed_name=None,
        changed_surname=None,
        changed_email=None,
        changed_phone=None
):
    sql_update_name = """
        UPDATE persons
        SET name = %s
        WHERE email = %s;
    """
    sql_update_surname = """
        UPDATE persons
        SET surname = %s
        WHERE email = %s;
    """
    sql_update_email = """
        UPDATE persons
        SET email = %s
        WHERE email = %s;
    """
    sql_select_id_person = """
        SELECT id FROM persons WHERE email = %s;
    """

    with connection.cursor() as cur:
        if changed_name:
            cur.execute(sql_update_name, (changed_name, required_email))
        if changed_surname:
            cur.execute(sql_update_surname, (changed_surname, required_email))
        if changed_email:
            cur.execute(sql_update_email, (changed_email, required_email))
        if changed_phone:
            cur.execute(sql_select_id_person, (required_email,))
            id_person = cur.fetchone()
            cur.execute("""
                DELETE FROM phones where id_person = %s;
            """, (id_person,))
            for tel in changed_phone:
                add_phone(connection, required_email, tel)
        connection.commit()


# Функция, позволяющая удалить телефон для существующего клиента.
def delete_phone(connection, email, phone_number):
    sql_select_id_person = """
        SELECT id FROM persons WHERE email = %s;
    """
    sql_delete_phone = """
        DELETE FROM phones 
        WHERE id_person = %s and phone_number = %s;
    """
    with connection.cursor() as cur:
        cur.execute(sql_select_id_person, (email,))
        id_person = cur.fetchone()
        cur.execute(sql_delete_phone, (id_person, phone_number))

    connection.commit()


# Функция, позволяющая удалить существующего клиента.
def delete_client(connection, email):
    sql_delete_client = """
        DELETE FROM persons 
        WHERE email = %s ;
    """
    with connection.cursor() as cur:
        cur.execute(sql_delete_client, (email,))

    connection.commit()


# Функция, позволяющая найти клиента по его данным:
# имени, фамилии, email или телефону.
def find_person(connection, word):
    sql_select = """
        SELECT name, surname, email, phone_number 
        FROM persons AS p 
        LEFT JOIN phones AS ph ON p.id = ph.id_person  
        WHERE name = %s OR surname = %s OR email = %s OR
        phone_number = %s
    """
    with connection.cursor() as cur:
        cur.execute(sql_select, (word, word, word, word))
        print(cur.fetchall())


# create_table(conn)
# add_client(conn, 'Ira', 'Sidorova', 'sidorovaira1@yandex.ru')
# add_client(conn, 'Kate', 'Petrova', 'petrovakate2@yandex.ru', phone=[89092156789])
# add_client(conn, 'Vasya', 'Popov', 'popovvasya3@gmail.com', phone=[89267891234, 89091112233])
# add_phone(conn, 'sidorovaira1@yandex.ru', phone=89109998877)
# update_data(conn, changed_name='Anna', required_email='sidorovaira1@yandex.ru')
# update_data(conn, changed_phone=['79081112233', '89998887766'], required_email='sidorovaira1@yandex.ru')
# delete_phone(conn, 'sidorovaira1@yandex.ru', '79081112233')
# delete_client(conn, 'sidorovaira1@yandex.ru')
# find_person(conn, 'Kate')
# find_person(conn, 'Petrova')
# find_person(conn, 'petrovakate2@yandex.ru')
# find_person(conn, '89267891234')


