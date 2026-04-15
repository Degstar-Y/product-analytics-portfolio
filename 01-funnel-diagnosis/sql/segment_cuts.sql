-- =========================================================
-- Файл: segment_cuts.sql
-- Задача:
-- Построить сегментные срезы session-level воронки
-- для device, source, category и price bucket.
--
-- Логика:
-- - сначала строится session-level таблица с флагами воронки;
-- - затем к ней присоединяются признаки сегментов;
-- - далее считаются conversion rates и вклад в потери.
-- =========================================================


-- =========================================================
-- Шаг 1. Нормализация событий
-- =========================================================
WITH cleaned_events AS (
    SELECT
        event_id,
        session_id,
        product_id,
        timestamp,
        LOWER(TRIM(event_type)) AS event_type_norm
    FROM events
    WHERE session_id IS NOT NULL
),


-- =========================================================
-- Шаг 2. Session-level флаги шагов
-- =========================================================
session_flags AS (
    SELECT
        session_id,

        MAX(CASE WHEN event_type_norm = 'page_view' THEN 1 ELSE 0 END) AS page_view_flag,
        MAX(CASE WHEN event_type_norm = 'add_to_cart' THEN 1 ELSE 0 END) AS add_to_cart_flag,
        MAX(CASE WHEN event_type_norm = 'checkout' THEN 1 ELSE 0 END) AS checkout_flag,
        MAX(CASE WHEN event_type_norm = 'purchase' THEN 1 ELSE 0 END) AS purchase_flag

    FROM cleaned_events
    GROUP BY session_id
),


-- =========================================================
-- Шаг 3. Первый товар в сессии
-- Используем как приближение для category / price bucket
-- =========================================================
first_product_in_session AS (
    SELECT
        x.session_id,
        x.product_id
    FROM (
        SELECT
            ce.session_id,
            ce.product_id,
            ce.timestamp,
            ROW_NUMBER() OVER (
                PARTITION BY ce.session_id
                ORDER BY ce.timestamp
            ) AS rn
        FROM cleaned_events ce
        WHERE ce.product_id IS NOT NULL
    ) x
    WHERE x.rn = 1
),


-- =========================================================
-- Шаг 4. Добавление категории и цены
-- =========================================================
session_product_features AS (
    SELECT
        fps.session_id,
        fps.product_id,
        p.category,
        p.price_usd,

        CASE
            WHEN p.price_usd <= 25 THEN '<=25'
            WHEN p.price_usd > 25 AND p.price_usd <= 50 THEN '25-50'
            WHEN p.price_usd > 50 AND p.price_usd <= 100 THEN '50-100'
            WHEN p.price_usd > 100 THEN '100+'
            ELSE NULL
        END AS price_bucket

    FROM first_product_in_session fps
    LEFT JOIN products p
        ON fps.product_id = p.product_id
),


-- =========================================================
-- Шаг 5. Полная session-level таблица для сегментов
-- =========================================================
session_level_dataset AS (
    SELECT
        s.session_id,
        s.customer_id,
        s.device,
        s.source,
        s.country,
        s.start_time,

        1 AS session_flag,
        COALESCE(sf.page_view_flag, 0) AS page_view_flag,
        COALESCE(sf.add_to_cart_flag, 0) AS add_to_cart_flag,
        COALESCE(sf.checkout_flag, 0) AS checkout_flag,
        COALESCE(sf.purchase_flag, 0) AS purchase_flag,

        spf.category,
        spf.price_usd,
        spf.price_bucket

    FROM sessions s
    LEFT JOIN session_flags sf
        ON s.session_id = sf.session_id
    LEFT JOIN session_product_features spf
        ON s.session_id = spf.session_id
),


-- =========================================================
-- Пример 1. Срез по device
-- =========================================================
device_funnel AS (
    SELECT
        'device' AS segment_name,
        device AS segment_value,

        COUNT(*) AS sessions_cnt,
        SUM(page_view_flag) AS page_view_sessions,
        SUM(add_to_cart_flag) AS add_to_cart_sessions,
        SUM(checkout_flag) AS checkout_sessions,
        SUM(purchase_flag) AS purchase_sessions,

        1.0 * SUM(purchase_flag) / COUNT(*) AS session_to_purchase_cr,
        1.0 * SUM(add_to_cart_flag) / NULLIF(SUM(page_view_flag), 0) AS view_to_cart_cr,
        1.0 * SUM(checkout_flag) / NULLIF(SUM(add_to_cart_flag), 0) AS cart_to_checkout_cr,
        1.0 * SUM(purchase_flag) / NULLIF(SUM(checkout_flag), 0) AS checkout_to_purchase_cr,

        COUNT(*) - SUM(purchase_flag) AS non_purchase_sessions

    FROM session_level_dataset
    GROUP BY device
),


-- =========================================================
-- Пример 2. Срез по source
-- =========================================================
source_funnel AS (
    SELECT
        'source' AS segment_name,
        source AS segment_value,

        COUNT(*) AS sessions_cnt,
        SUM(page_view_flag) AS page_view_sessions,
        SUM(add_to_cart_flag) AS add_to_cart_sessions,
        SUM(checkout_flag) AS checkout_sessions,
        SUM(purchase_flag) AS purchase_sessions,

        1.0 * SUM(purchase_flag) / COUNT(*) AS session_to_purchase_cr,
        1.0 * SUM(add_to_cart_flag) / NULLIF(SUM(page_view_flag), 0) AS view_to_cart_cr,
        1.0 * SUM(checkout_flag) / NULLIF(SUM(add_to_cart_flag), 0) AS cart_to_checkout_cr,
        1.0 * SUM(purchase_flag) / NULLIF(SUM(checkout_flag), 0) AS checkout_to_purchase_cr,

        COUNT(*) - SUM(purchase_flag) AS non_purchase_sessions

    FROM session_level_dataset
    GROUP BY source
),


-- =========================================================
-- Пример 3. Срез по category
-- =========================================================
category_funnel AS (
    SELECT
        'category' AS segment_name,
        category AS segment_value,

        COUNT(*) AS sessions_cnt,
        SUM(page_view_flag) AS page_view_sessions,
        SUM(add_to_cart_flag) AS add_to_cart_sessions,
        SUM(checkout_flag) AS checkout_sessions,
        SUM(purchase_flag) AS purchase_sessions,

        1.0 * SUM(purchase_flag) / COUNT(*) AS session_to_purchase_cr,
        1.0 * SUM(add_to_cart_flag) / NULLIF(SUM(page_view_flag), 0) AS view_to_cart_cr,
        1.0 * SUM(checkout_flag) / NULLIF(SUM(add_to_cart_flag), 0) AS cart_to_checkout_cr,
        1.0 * SUM(purchase_flag) / NULLIF(SUM(checkout_flag), 0) AS checkout_to_purchase_cr,

        COUNT(*) - SUM(purchase_flag) AS non_purchase_sessions

    FROM session_level_dataset
    WHERE category IS NOT NULL
    GROUP BY category
),


-- =========================================================
-- Пример 4. Срез по price_bucket
-- =========================================================
price_bucket_funnel AS (
    SELECT
        'price_bucket' AS segment_name,
        price_bucket AS segment_value,

        COUNT(*) AS sessions_cnt,
        SUM(page_view_flag) AS page_view_sessions,
        SUM(add_to_cart_flag) AS add_to_cart_sessions,
        SUM(checkout_flag) AS checkout_sessions,
        SUM(purchase_flag) AS purchase_sessions,

        1.0 * SUM(purchase_flag) / COUNT(*) AS session_to_purchase_cr,
        1.0 * SUM(add_to_cart_flag) / NULLIF(SUM(page_view_flag), 0) AS view_to_cart_cr,
        1.0 * SUM(checkout_flag) / NULLIF(SUM(add_to_cart_flag), 0) AS cart_to_checkout_cr,
        1.0 * SUM(purchase_flag) / NULLIF(SUM(checkout_flag), 0) AS checkout_to_purchase_cr,

        COUNT(*) - SUM(purchase_flag) AS non_purchase_sessions

    FROM session_level_dataset
    WHERE price_bucket IS NOT NULL
    GROUP BY price_bucket
)

-- =========================================================
-- Финальный вывод:
-- для примера объединяем несколько сегментов в один результат
-- =========================================================
SELECT * FROM device_funnel
UNION ALL
SELECT * FROM source_funnel
UNION ALL
SELECT * FROM category_funnel
UNION ALL
SELECT * FROM price_bucket_funnel
ORDER BY segment_name, session_to_purchase_cr;