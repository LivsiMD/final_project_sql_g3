import psycopg2


def start_planning(year, quarter, user, pwd):
    quarterid = f"{year}.{quarter}"  # Форматирование квартала по шаблону "YYYY.Q"

    con = psycopg2.connect(database="plan_test_ip", user=user, password=pwd, host="localhost",
                           port="5432")
    cur = con.cursor()

    try:
        # Шаг 1: Удаление плановых данных из plan_data таблицы
        cur.execute("DELETE FROM plan_data WHERE quarterid=%s", (quarterid,))

        # Шаг 2: Удаление записей из таблицы plan_status
        cur.execute("DELETE FROM plan_status WHERE quarterid=%s", (quarterid,))

        # Шаг 3: Создание записей статуса планирования
        cur.execute("SELECT DISTINCT countrycode FROM company")
        countries = cur.fetchall()
        print(countries)
        for country in countries:
            print(quarterid)
            cur.execute(
                "INSERT INTO plan_status (quarterid, country, status, modifieddatetime, author) VALUES (%s, %s, 'R', current_timestamp, current_user)",
                (quarterid, country[0]))

    except Exception as error:
        con.rollback()  # Откатываем транзакцию в случае возникновения ошибки
        print("Произошла ошибка:", error)

    finally:
        cur.close()
        con.close()

# Пример использования функции
start_planning(2014, 1, "ivan", "test1762")