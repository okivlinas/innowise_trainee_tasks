import psycopg2
import json
import xml.etree.ElementTree as ET

studentsPath = input('Enter STUDENTS path: ')
roomsPath = input('Enter ROOMS path: ')
format = input('Choose FORMAT(json/xml): ')
res = input('Do you want to create new DataBase(y/n): ')
host = input('Enter your host: ')
port = input('Enter your port: ')
user = input('Enter your user: ')
password = input('Enter your password: ')
database = input('Enter your database: ')

connection = psycopg2.connect(
    host = host,
    port = port,
    user = user,
    password = password,
    database = database
)

with open(roomsPath) as rooms_file:
    rooms_data = json.load(rooms_file)
with open(studentsPath) as students_file:
    students_data = json.load(students_file)

if res == 'y':
    create_rooms_table_query = """
    CREATE TABLE rooms (
        room_id INT PRIMARY KEY,
        room_name VARCHAR(255)
    )
    """
    with connection.cursor() as cursor:
        cursor.execute(create_rooms_table_query)

    create_students_query_table = """
    CREATE TABLE students (
        student_birthday TIMESTAMP, 
        student_id SERIAL PRIMARY KEY,
        student_name VARCHAR(255),
        room_id INT,
        student_sex CHARACTER(1),
        FOREIGN KEY (room_id) REFERENCES rooms (room_id)
    )
    """
    with connection.cursor() as cursor:
        cursor.execute(create_students_query_table)

    for index, room in enumerate(rooms_data):
        room_id = index
        room_name = room["name"]
        insert_query = "INSERT INTO rooms (room_id, room_name) VALUES (%s, %s)"
        with connection.cursor() as cursor:
            cursor.execute(insert_query, (room_id, room_name))

    for student in students_data:
        student_birthday = student["birthday"]
        student_name = student["name"]
        room_id = student["room"]
        student_sex = student["sex"]
        insert_query = "INSERT INTO students (student_birthday, student_name, room_id, student_sex) VALUES (%s,%s,%s,%s)"
        with connection.cursor() as cursor:
            cursor.execute(insert_query, (student_birthday, student_name, room_id, student_sex))

if format == 'json':
    first_query = """
    select rooms.room_name, count(students.student_id) as student_count
    from rooms left join students 
    on rooms.room_id = students.room_id 
    group by rooms.room_name, rooms.room_id 
    order by rooms.room_id
    """
    with connection.cursor() as cursor:
        cursor.execute(first_query)
        first_query_result = cursor.fetchall()

    first_result_data = []
    for row1 in first_query_result:
        room_name, student_count = row1
        first_result_data.append({"room_name": room_name, "student_count": student_count})

    first_result_file_path = "result1.json"
    with open(first_result_file_path, "w") as first_result_file:
        json.dump(first_result_data, first_result_file)

    second_query = """
    SELECT rooms.room_name, AVG(DATE_PART('year', current_date) - DATE_PART('year', students.student_birthday)) AS average_age
    FROM rooms LEFT JOIN students 
    ON rooms.room_id = students.room_id
    GROUP BY rooms.room_name
    ORDER BY average_age
    limit 5; 
    """
    with connection.cursor() as cursor:
        cursor.execute(second_query)
        second_query_result = cursor.fetchall()

    second_result_data = []
    for row2 in second_query_result:
        room_name, average_age = row2
        second_result_data.append({"room_name": room_name, "average_age": average_age})

    second_result_file_path = "result2.json"
    with open(second_result_file_path, "w") as second_result_file:
        json.dump(second_result_data, second_result_file)

    third_query = """
    select rooms.room_name, max(DATE_PART('year', current_date) - DATE_PART('year', students.student_birthday)) - min(DATE_PART('year', current_date) - DATE_PART('year', students.student_birthday)) as age_diff
    from rooms left join students
    on rooms.room_id = students.room_id
    group by rooms.room_id
    order by age_diff desc
    limit 5; 
    """
    with connection.cursor() as cursor:
        cursor.execute(third_query)
        third_query_result = cursor.fetchall()

    third_result_data = []
    for row3 in third_query_result:
        room_name, age_diff = row3
        third_result_data.append({"room_name": room_name, "age_diff": age_diff})

    third_result_file_path = "result3.json"
    with open(third_result_file_path, "w") as third_result_file:
        json.dump(third_result_data, third_result_file)

    fourth_query = """
    select rooms.room_id, rooms.room_name
    from rooms 
    inner join students as s1 on rooms.room_id = s1.room_id
    inner join students as s2 on rooms.room_id = s2.room_id
    and s1.student_sex <> s2.student_sex
    group by rooms.room_id
    order by rooms.room_id;
    """
    with connection.cursor() as cursor:
        cursor.execute(fourth_query)
        fourth_query_result = cursor.fetchall()

    fourth_result_data = []
    for row4 in third_query_result:
        room_name = row4
        fourth_result_data.append({"room_name": room_name})

    fourth_result_file_path = "result4.json"
    with open(fourth_result_file_path, "w") as fourth_result_file:
        json.dump(fourth_result_data, fourth_result_file)
else:
    first_query = """
    SELECT rooms.room_name, COUNT(students.student_id) AS student_count
    FROM rooms LEFT JOIN students ON rooms.room_id = students.room_id
    GROUP BY rooms.room_name, rooms.room_id
    ORDER BY rooms.room_id
    """

    with connection.cursor() as cursor:
        cursor.execute(first_query)
        first_query_result = cursor.fetchall()

    first_result_data = []
    for row1 in first_query_result:
        room_name, student_count = row1
        first_result_data.append({"room_name": room_name, "student_count": student_count})

    first_result_file_path = "result1.xml"
    root = ET.Element("results")
    for row in first_result_data:
        result_element = ET.SubElement(root, "result")
        room_name_element = ET.SubElement(result_element, "room_name")
        room_name_element.text = row["room_name"]
        student_count_element = ET.SubElement(result_element, "student_count")
        student_count_element.text = str(row["student_count"])

    tree = ET.ElementTree(root)
    tree.write(first_result_file_path)

    second_query = """
    SELECT rooms.room_name, AVG(DATE_PART('year', current_date) - DATE_PART('year', students.student_birthday)) AS average_age
    FROM rooms LEFT JOIN students ON rooms.room_id = students.room_id
    GROUP BY rooms.room_name
    ORDER BY average_age
    LIMIT 5;
    """

    with connection.cursor() as cursor:
        cursor.execute(second_query)
        second_query_result = cursor.fetchall()

    second_result_data = []
    for row2 in second_query_result:
        room_name, average_age = row2
        second_result_data.append({"room_name": room_name, "average_age": average_age})

    second_result_file_path = "result2.xml"
    root = ET.Element("results")
    for row in second_result_data:
        result_element = ET.SubElement(root, "result")
        room_name_element = ET.SubElement(result_element, "room_name")
        room_name_element.text = row["room_name"]
        average_age_element = ET.SubElement(result_element, "average_age")
        average_age_element.text = str(row["average_age"])

    tree = ET.ElementTree(root)
    tree.write(second_result_file_path)

    third_query = """
    SELECT rooms.room_name, MAX(DATE_PART('year', current_date) - DATE_PART('year', students.student_birthday)) - MIN(DATE_PART('year', current_date) - DATE_PART('year', students.student_birthday)) AS age_diff
    FROM rooms LEFT JOIN students ON rooms.room_id = students.room_id
    GROUP BY rooms.room_id
    ORDER BY age_diff DESC
    LIMIT 5;
    """

    with connection.cursor() as cursor:
        cursor.execute(third_query)
        third_query_result = cursor.fetchall()

    third_result_data = []
    for row3 in third_query_result:
        room_name, age_diff = row3
        third_result_data.append({"room_name": room_name, "age_diff": age_diff})

    third_result_file_path = "result3.xml"
    root = ET.Element("results")
    for row in third_result_data:
        result_element = ET.SubElement(root, "result")
        room_name_element = ET.SubElement(result_element, "room_name")
        room_name_element.text = row["room_name"]
        age_diff_element = ET.SubElement(result_element, "age_diff")
        age_diff_element.text = str(row["age_diff"])

    tree = ET.ElementTree(root)
    tree.write(third_result_file_path)

    fourth_query = """
    SELECT rooms.room_id, rooms.room_name
    FROM rooms
    INNER JOIN students AS s1 ON rooms.room_id = s1.room_id
    INNER JOIN students AS s2 ON rooms.room_id = s2.room_id AND s1.student_sex <> s2.student_sex
    GROUP BY rooms.room_id
    ORDER BY rooms.room_id;
    """

    with connection.cursor() as cursor:
        cursor.execute(fourth_query)
        fourth_query_result = cursor.fetchall()

    fourth_result_data = []
    for row4 in fourth_query_result:
        room_name = row4[1]
        fourth_result_data.append({"room_name": room_name})

    fourth_result_file_path = "result4.xml"
    root = ET.Element("results")
    for row in fourth_result_data:
        result_element = ET.SubElement(root, "result")
        room_name_element = ET.SubElement(result_element, "room_name")
        room_name_element.text = row["room_name"]

    tree = ET.ElementTree(root)
    tree.write(fourth_result_file_path)

resultMessage = input("Do you want to DROP your TABLES(y/n): ")
if resultMessage == "y":
    drop_rooms_table_query = """
    DROP TABLE rooms
    """
    with connection.cursor() as cursor:
        cursor.execute(drop_rooms_table_query)

    drop_students_table_query = """
    DROP TABLE students
    """
    with connection.cursor() as cursor:
        cursor.execute(drop_students_table_query)

connection.commit()
connection.close()

