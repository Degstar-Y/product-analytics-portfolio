-- =========================================================
-- Файл: funnel_definition.sql
-- Задача:
-- Построить основную продуктовую воронку на уровне session_id
-- для e-commerce кейса.
--
-- Шаги воронки:
-- 1. session
-- 2. page_view
-- 3. add_to_cart
-- 4. checkout
-- 5. purchase
--
-- Логика:
-- - базой являются все сессии из sessions;
-- - шаг считается достигнутым, если внутри session_id
--   был хотя бы один event соответствующего типа;
-- - одна сессия считается не более одного раза на шаге.
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
-- Шаг 2. Построение session-level флагов воронки
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
-- Шаг 3. Полная session-level таблица
-- База = все сессии из sessions
-- =========================================================
session_funnel AS (
    SELECT
        s.session_id,

        1 AS session_flag,

        COALESCE(f.page_view_flag, 0) AS page_view_flag,
        COALESCE(f.add_to_cart_flag, 0) AS add_to_cart_flag,
        COALESCE(f.checkout_flag, 0) AS checkout_flag,
        COALESCE(f.purchase_flag, 0) AS purchase_flag

    FROM sessions s
    LEFT JOIN session_flags f
        ON s.session_id = f.session_id
),


-- =========================================================
-- Шаг 4. Подсчет количества сессий на каждом шаге
-- =========================================================
funnel_counts AS (
    SELECT 'session' AS step, COUNT(*) AS sessions_cnt
    FROM session_funnel

    UNION ALL

    SELECT 'page_view' AS step, SUM(page_view_flag) AS sessions_cnt
    FROM session_funnel

    UNION ALL

    SELECT 'add_to_cart' AS step, SUM(add_to_cart_flag) AS sessions_cnt
    FROM session_funnel

    UNION ALL

    SELECT 'checkout' AS step, SUM(checkout_flag) AS sessions_cnt
    FROM session_funnel

    UNION ALL

    SELECT 'purchase' AS step, SUM(purchase_flag) AS sessions_cnt
    FROM session_funnel
),


-- =========================================================
-- Шаг 5. Добавление порядка шагов
-- =========================================================
ordered_funnel AS (
    SELECT
        step,
        sessions_cnt,
        CASE
            WHEN step = 'session' THEN 1
            WHEN step = 'page_view' THEN 2
            WHEN step = 'add_to_cart' THEN 3
            WHEN step = 'checkout' THEN 4
            WHEN step = 'purchase' THEN 5
        END AS step_order
    FROM funnel_counts
),


-- =========================================================
-- Шаг 6. Расчет конверсий и потерь
-- =========================================================
final_funnel AS (
    SELECT
        step,
        sessions_cnt,
        step_order,

        LAG(sessions_cnt) OVER (ORDER BY step_order) AS prev_step_sessions,

        CASE
            WHEN LAG(sessions_cnt) OVER (ORDER BY step_order) IS NULL THEN 1.0
            ELSE 1.0 * sessions_cnt / LAG(sessions_cnt) OVER (ORDER BY step_order)
        END AS conversion_from_prev,

        1.0 * sessions_cnt / MAX(CASE WHEN step = 'session' THEN sessions_cnt END) OVER ()
            AS conversion_from_start,

        CASE
            WHEN LAG(sessions_cnt) OVER (ORDER BY step_order) IS NULL THEN 0
            ELSE LAG(sessions_cnt) OVER (ORDER BY step_order) - sessions_cnt
        END AS drop_from_prev_abs,

        CASE
            WHEN LAG(sessions_cnt) OVER (ORDER BY step_order) IS NULL THEN 0
            ELSE 1.0 - (1.0 * sessions_cnt / LAG(sessions_cnt) OVER (ORDER BY step_order))
        END AS drop_from_prev_pct

    FROM ordered_funnel
)

SELECT
    step,
    sessions_cnt,
    conversion_from_prev,
    conversion_from_start,
    drop_from_prev_abs,
    drop_from_prev_pct
FROM final_funnel
ORDER BY step_order;