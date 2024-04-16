import psycopg2


def start_planning(year, quarter, user, pwd):
    quarterid = f"{year}.{quarter}"  # Форматирование квартала по шаблону "YYYY.Q"
    print(year-1)

    con = psycopg2.connect(database="plan_test_ip", user=user, password=pwd, host="localhost", port="5432")
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
            cur.execute("INSERT INTO plan_status (quarterid, country, status, modifieddatetime, author) VALUES (%s, %s, 'R', current_timestamp, current_user)",(quarterid, country[0]))
            print("Good!")

        # Шаг 4: Вставка данных в таблицу plan_data
        cur.execute("""
        INSERT INTO plan_data (versionid, country, quarterid, pcid, salesamt)
            SELECT DISTINCT
                'N' AS versionid,
                c.countrycode AS country,
                concat_ws('.', %s, %s) AS quarterid,
                cs.categoryid AS pcid,
                avg(sum(cs.salesamt)) over(PARTITION BY c.countrycode, cs.categoryid) AS salesamt
            FROM company c 
                LEFT JOIN company_sales cs ON c.id = cs.cid 
            WHERE 
                cs.ccls IN ('A', 'B')  
                AND cs."year"  IN (%s, %s)
                AND cs.quarter_yr = %s 
            GROUP BY 
                c.countrycode,
                cs.categoryid,
                cs.qr
            ORDER BY 
                c.countrycode,
                cs.categoryid;
        """, [year, quarter, year-1, year-2, quarter])

        # Шаг 5 Копирование данных из версии N в версию P в таблице plan_data
        cur.execute("""
        INSERT INTO plan_data (versionid, country, quarterid, pcid, salesamt)
        SELECT 'P' as versionid, country, quarterid, pcid, salesamt
        FROM plan_data
        WHERE 1 = 1 AND versionid = 'N' AND quarterid = concat_ws('.', %s, %s)
        """, [year, quarter])

        # Шаг 6 Сохранение имени пользователя, выполняющего изменения, в записях plan_status
        cur.execute("UPDATE plan_status SET author = %s WHERE quarterid  = concat_ws('.', %s, %s);" , (user,year, quarter))

        con.commit()

    except Exception as error:
        con.rollback()  # Откатываем транзакцию в случае возникновения ошибки
        print("Произошла ошибка:", error)

    finally:
        cur.close()
        con.close()

# Пример использования функции
start_planning(2014, 1, "ivan", "test1762")