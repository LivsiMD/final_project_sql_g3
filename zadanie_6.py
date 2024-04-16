import psycopg2


def start_planning(year, quarter, user, pwd):
    quarterid = f"{year}.{quarter}"  # Форматирование квартала по шаблону "YYYY.Q"
    quarterid_1 = f"{year-1}.{quarter}"
    quarterid_2 = f"{year - 2}.{quarter}"

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
        select distinct 'N' as versionid, countrycode as country, %s as quarterid, categoryid as pcid,  
        case
        WHEN COUNT(qr) over (partition by countrycode, categoryid) = 1 then sum(csalesqr) over (partition by countrycode, categoryid)
        else sum(csalesqr) over (partition by countrycode, categoryid) /2
        END AS salesamt
        from (
        select distinct cs.categoryid, c.countrycode, cs.qr, sum(cs.salesamt) over (partition by cs.categoryid order by cs.qr, c.countrycode) as csalesqr
        from company_sales cs
        join company c on c.id = cs.cid 
        where cs.ccls in ('A', 'B') and  cs.qr in (%s, %s)
        order by cs.categoryid
        ) AS sub
        GROUP BY 
            countrycode, categoryid, qr, csalesqr
        order by countrycode
        """, [quarterid, quarterid_2, quarterid_1])

        # Шаг 5 Копирование данных из версии N в версию P в таблице plan_data
        cur.execute("""
        INSERT INTO plan_data (versionid, country, quarterid, pcid, salesamt)
        SELECT 'P', country, quarterid, pcid, salesamt
        FROM plan_data
        """)

        # Шаг 6 Сохранение имени пользователя, выполняющего изменения, в записях plan_status
        cur.execute("UPDATE plan_status SET author = %s", (user,))

        con.commit()

    except Exception as error:
        con.rollback()  # Откатываем транзакцию в случае возникновения ошибки
        print("Произошла ошибка:", error)

    finally:
        cur.close()
        con.close()

# Пример использования функции
start_planning(2014, 1, "ivan", "test1762")