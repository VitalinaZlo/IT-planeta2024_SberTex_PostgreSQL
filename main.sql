WITH 

-- 1. Получаем максимальный номер станции для каждого поезда
search_max_station AS (
    SELECT 
        r1.train_code, 
        MAX(r2.station_nr) AS max_station_nr
    FROM 
        routes r1
    JOIN
        routes r2 ON r1.train_code = r2.train_code
    GROUP BY 
        r1.train_code
),

-- 2. Извлекаем данные о расписании поездов и разбиваем годовое расписание поездов на интервалы
date_ranges AS (
    SELECT 
        train_code,
        station_from,
        station_to,
        distance_hours,
        range AS schedule_range,
        schedule_even,
        schedule_dow
    FROM 
        trains,
        unnest(schedule_interval) AS range
),

-- 3. Генерируем все возможные даты для каждого поезда в заданных интервалах
expanded_dates AS (
    SELECT 
        train_code,
        station_from,
        station_to,
        distance_hours,
        generate_series(lower(schedule_range), upper(schedule_range) - interval '1 day', interval '1 day') AS schedule_date,
        schedule_even,
        schedule_dow
    FROM 
        date_ranges 
),

-- 4. Фильтруем даты по заданному диапазону (с 05-11-2024 по 17-11-2024 включительно)
filtered_dates AS (
    SELECT 
        train_code,
        station_from,
        station_to,
        distance_hours,
        schedule_date,
        schedule_even,
        schedule_dow
    FROM 
        expanded_dates
    WHERE 
        schedule_date >= '2024-11-05' AND schedule_date <= '2024-11-17'
),

-- 5. Вычисляем даты отправления и прибытия для каждого поезда
dates_departure_and_arrival AS (
    SELECT 
        fd.train_code,
        fd.station_from,
        fd.station_to,
        fd.schedule_date + r.departure_time AS start_date,
        fd.schedule_date + r.departure_time + fd.distance_hours AS end_date
    FROM 
        filtered_dates fd
    JOIN
        routes r ON fd.train_code = r.train_code AND r.station_nr = 1
    WHERE 
        (fd.schedule_even IS NULL OR 
        (fd.schedule_even = 1 AND EXTRACT(DAY FROM fd.schedule_date) % 2 != 0) OR 
        (fd.schedule_even = 0 AND EXTRACT(DAY FROM fd.schedule_date) % 2 = 0))
        AND 
        (fd.schedule_dow IS NULL OR 
        (fd.schedule_dow @> ARRAY[EXTRACT(DOW FROM fd.schedule_date)]) 
        )
),

-- 6. Объединяем данные о датах с максимальными номерами станций
joining_tables AS (
    SELECT
        dfar.*,
        sms.max_station_nr AS max_station_nr_train
    FROM
        dates_departure_and_arrival dfar
    JOIN
        search_max_station sms ON dfar.train_code = sms.train_code
),

-- 7. Находим поезда, на которые можно пересесть, учитывая станцию пересадки и время ожидания следующего рейса
matching_dates AS (
    SELECT 
        jt1.*,
        jt2.train_code AS transfer_codes,
		jt2.max_station_nr_train AS max_station_nr_transfer,
        jt1.max_station_nr_train + jt2.max_station_nr_train - 1 AS gemeral_max_station
    FROM 
        joining_tables jt1
    JOIN 
        joining_tables jt2 ON (jt1.station_to = jt2.station_from
        AND (jt2.start_date - jt1.end_date) >= interval '40 minutes'
        AND (jt2.start_date - jt1.end_date) <= interval '8 hours'
        )
),

-- 8. Отсеиваем только те пары поездов, у которых промежуточные станции, кроме станции пересадки, не совпадают
non_coincidence_intermediate_stations AS (
    SELECT 
        md.train_code,
		md.max_station_nr_train,
        md.start_date,
        md.transfer_codes,
		md.max_station_nr_transfer,
        md.gemeral_max_station
    FROM 
        matching_dates md
    WHERE
        NOT EXISTS (
            SELECT 1
            FROM 
                routes r1
            JOIN 
                routes r2 ON (
				r1.station_code = r2.station_code 
                AND r1.train_code = md.train_code AND r2.train_code = md.transfer_codes)
            WHERE 
                (r1.station_nr <> 1 AND r2.station_nr <> md.max_station_nr_transfer) AND 
                (r1.station_nr <> md.max_station_nr_train AND r2.station_nr <> 1)
        )
),

-- 9. Выбираем маршрут с максимальным общим количеством станций
best_route AS (
	SELECT 
		train_code  || ', ' || transfer_codes AS train_array,
		start_date,
		gemeral_max_station
	FROM 
		non_coincidence_intermediate_stations
	WHERE 
		gemeral_max_station = (SELECT MAX(gemeral_max_station) FROM non_coincidence_intermediate_stations)
)

SELECT *
FROM best_route