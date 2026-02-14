/* Проект «Разработка витрины и решение ad-hoc задач»
 * Цель проекта: подготовка витрины данных маркетплейса «ВсёТут»
 * и решение четырех ad hoc задач на её основе
 * 
 * Автор: Камальдинов Максим
 * Дата: 29.12.2025
*/



/*
 * В подзапросе preprocessed_installment_promo было ошибочное выражение поиска наличия рассрочки.
 * Я понадеялся, что поле payment_installments (в таблице order_payments) может быть 1 или больше.
 * В итоге обнаружил что один пользователь из Москвы имеет следующие записи:
 *  order_id = 744bade1fcf9ff3f31d860ace076d422
 *	order_status = Доставлено
 *	user_id = f54cea27c80dc09bfe07b1cf1e01b845
 *	region = Москва
 *	payment_installments = 0 !!!
 * Заменил выражение поиска заказов с рассрочкой с:
 * FALSE: CASE WHEN payment_installments <> 1 THEN 1 ELSE 0 END AS rass_order_signal
 * на
 * TRUE:  CASE WHEN payment_installments > 1 THEN 1 ELSE 0 END AS rass_order_signal
 * 
 * Добавил фильтрацию по статусу заказа, при определении топ 3 регионов по количеству заказов.
 * 
 * ad hoc Задача 1
 * Для подсчета числа уникальных клиентов, в подзапросе, сгруппировал user_id, 
 * вне зависимости от региона, обединил (+) число заказов и их стоимость,
 * для пользователей, которые совершили покупки в разных регионах.
 */










/* Часть 1. Разработка витрины данных */

-- Создаем подзапрос формирующий основную таблицу с заказами и пользователями 
-- из первых трех регионов с наибольшим количеством заказов и статусами Доставлен или Отменен.
-- Последующий анализ данных будет опираться на эту таблицу.
WITH main_orders_users AS (
-- Основная таблица с нужными регионами и статусами заказов, к ней будем присоединять необходимые таблицы
SELECT *
FROM ds_ecom.orders	-- выбираем таблицу с заказами
LEFT JOIN ds_ecom.users USING(buyer_id)		-- к каждому заказу присоединяем данные о клиентах, далее фильтруем по региону и статусу заказа
WHERE ( order_status = 'Отменено' OR order_status = 'Доставлено') AND region IN (-- оставляем заказы только со статусами Отменено или Доставлено
	SELECT region FROM ds_ecom.orders	-- Первые 3 региона с наибольшим количеством заказов
	LEFT JOIN ds_ecom.users USING(buyer_id)
	WHERE order_status = 'Отменено' OR order_status = 'Доставлено'
	GROUP BY region
	ORDER BY COUNT(*) DESC LIMIT 3
	)
-- COUNT(DISTINCT order_id)=64682 и COUNT(order_id)=64682 значения в поле order_id уникальны и является первичным ключем, 
),

preprocessed_order_reviews AS (
-- Подзапрос позволит исправить ошибки оценок и вычислить среднее при множестве оценок в заказе
SELECT DISTINCT order_id, AVG(preprocessed_review_score) AS preprocessed_review_score
FROM (
	SELECT review_id, order_id, review_creation_date, review_answer_timestamp, 
	CASE 
		WHEN review_score > 5 THEN review_score/10 ELSE review_score -- исправляем ошибки рейтинга
	END AS preprocessed_review_score
	FROM ds_ecom.order_reviews
) AS p_o_r
GROUP BY order_id
-- Таблица ds_ecom.order_reviews содержит более одной оценки для некоторых заказов:
-- COUNT(DISTINCT review_id)=COUNT(review_id)=78034 и COUNT(DISTINCT order_id)=77851, COUNT(order_id)=78034
-- Количество review_id больше чем количество order_id в таблице ds_ecom.order_reviews
-- Исправили некорректные значения рейтингов для заказа и вычислили среднию оценку, если заказ оценили более одного раза.
),

step_1_2 AS (
-- Подзапрос вычисляющий дату первого и последнего заказа, жизненный цикл клиента и статистические данные
SELECT m.user_id, m.region,					-- клиент и регион
MIN(m.order_purchase_ts) AS first_order_ts,	-- дата первого заказа
MAX(m.order_purchase_ts) AS last_order_ts,	-- дата последнего заказа
(MAX(m.order_purchase_ts) - MIN(m.order_purchase_ts)) AS lifetime,	-- жизненный цикл клиента
COUNT(m.order_id) AS total_orders,			-- всего заказов у клиента в регионе
AVG(r.preprocessed_review_score) AS avg_order_rating,	-- средний рейтинг заказов оцененный клиентом
COUNT(r.preprocessed_review_score) AS num_orders_with_rating,	-- количество оцененных заказов
COUNT(m.order_status) FILTER(WHERE m.order_status = 'Отменено') AS num_canceled_orders,	-- количество отмененных заказов
(COUNT(m.order_id) FILTER(WHERE m.order_status = 'Отменено') * 100 / COUNT(m.order_id)::numeric) AS canceled_orders_ratio -- доля отмененных заказов по отношению к общему количеству заказов
FROM main_orders_users AS m		-- выбираем данные из подзапроса
LEFT JOIN preprocessed_order_reviews AS r ON m.order_id = r.order_id	--присоединяем подзапрос с исправленными рейтингами заказов
GROUP BY m.user_id, m.region		-- группируем по клиенту и региону, далее вычисляем необходимые метрики
),

preprocessed_order_cost AS (
-- вспомогательный подзапрос для вычисления метрик для подзапроса step_3
SELECT order_id, (SUM(price) + SUM(delivery_cost)) AS order_cost	-- полная стоимость заказа
FROM ds_ecom.order_items
WHERE order_id IN (SELECT order_id FROM main_orders_users)	-- оставляем только те заказы, которые удовлетворяют требованию по региону и статусу заказа
GROUP BY order_id
),

preprocessed_installment_promo AS (
-- вспомогательный подзапрос для вычисления наличия рассрочки и промокода в заказе для подзапроса step_3
SELECT order_id, 
CASE WHEN rass_order > 0 THEN 1 ELSE 0 END AS installment_orders, --3. Упрощаем сигнал до 0 или 1 в заказе
CASE WHEN promo_order > 0 THEN 1 ELSE 0 END AS promo_orders --3. Упрощаем сигнал до 0 или 1 в заказе
FROM (
	SELECT order_id, 	--2. уникальные order_id
	SUM(rass_order_signal) AS rass_order, --2. При наличии рассрочки сигнал будет больше 0
	SUM(promo_order_signal) AS promo_order	--2. При промо сигнал будет больше 0
	FROM (
		SELECT order_id, --1. дублирование order_id с разными payment_sequential
		CASE WHEN payment_installments > 1 THEN 1 ELSE 0 END AS rass_order_signal, --1. для каждого payment_sequential фиксируем наличие рассрочки
		CASE WHEN payment_type = 'промокод' THEN 1 ELSE 0 END AS promo_order_signal --1. для каждого payment_sequential фиксируем наличие промо
		FROM ds_ecom.order_payments
		WHERE order_id IN (SELECT order_id FROM main_orders_users)
	) AS t1
	GROUP BY order_id
) AS t2
),

step_3 AS (		-- подзапрос вычисляет ряд метрик:
SELECT m.user_id, m.region, 	-- клиент и его регион
SUM(CASE WHEN m.order_status = 'Доставлено' THEN p.order_cost ELSE NULL END) AS total_order_costs,		-- стоимость всех заказов
ROUND(AVG(CASE 
			WHEN m.order_status = 'Доставлено' THEN p.order_cost ELSE NULL 
		  END), 2) AS avg_order_cost,	-- средняя стоимость заказов
SUM(r.installment_orders) AS num_installment_orders,	-- количество заказов оплаченных в рассрочку 
SUM(r.promo_orders) AS num_orders_with_promo	-- количество заказов купленных с использованием промокодов
FROM main_orders_users AS m
LEFT JOIN preprocessed_order_cost AS p USING(order_id)	-- присоединяем вспомогательные подзапрос
LEFT JOIN preprocessed_installment_promo AS r USING(order_id)	-- присоединяем вспомогательные подзапрос
GROUP BY m.user_id, m.region
),

step_4 AS (
SELECT m.user_id, m.region,
MAX(CASE WHEN order_id IN (		-- оплата заказа денежным переводом в первую последовательность
	SELECT order_id
	FROM (	--ds_ecom.order_payments	-- из таблицы о способах оплаты заказов выбираем заказы согласно условию:
		SELECT order_id,
		FIRST_VALUE(payment_type) OVER(PARTITION BY order_id ORDER BY payment_sequential) AS first_payment_type
		FROM ds_ecom.order_payments
	) AS t
	WHERE first_payment_type = 'денежный перевод' AND order_id IN (SELECT order_id FROM main_orders_users))
	THEN 1 ELSE 0 END
	) AS used_money_transfer,
MAX(t4.order_installments) AS used_installments, --3.есть ли у клиента заказы с рассрочками
MAX(CASE WHEN m.order_status = 'Отменено' THEN 1 ELSE 0 END) AS used_cancel	--отменял ли клиент заказы
FROM main_orders_users AS m
LEFT JOIN (
	SELECT order_id,
	CASE WHEN order_payment_installments > 1 THEN 1 ELSE 0 END AS order_installments --2.если рассрочка > 1, поднимаем флаг наличия рассрочки
	FROM (
		SELECT order_id,  -- 1.группируем по заказам и определяем максимальное количество рассрочек в заказе
		MAX(payment_installments) AS order_payment_installments
		FROM ds_ecom.order_payments
		WHERE order_id IN (SELECT order_id FROM main_orders_users)
		GROUP BY order_id
	) AS order_id_used_installments
) AS t4 USING(order_id)
GROUP BY user_id, region
)
--Витрина данных, соединяем подзапросы по user_id и region:
SELECT *
FROM step_1_2 AS s12
LEFT JOIN step_3 AS t3 USING(user_id, region)
LEFT JOIN step_4 AS t4 USING(user_id, region)
ORDER BY total_orders DESC;











/* Часть 2. Решение ad hoc задач
*/


/* Задача 1. Сегментация пользователей 
 * Разделите пользователей на группы по количеству совершённых ими заказов.
 * Подсчитайте для каждой группы общее количество пользователей,
 * среднее количество заказов, среднюю стоимость заказа.
 * 
 * Выделите такие сегменты:
 * - 1 заказ — сегмент 1 заказ
 * - от 2 до 5 заказов — сегмент 2-5 заказов
 * - от 6 до 10 заказов — сегмент 6-10 заказов
 * - 11 и более заказов — сегмент 11 и более заказов
*/
SELECT segment,
COUNT(user_id) AS count_users,
ROUND(AVG(u_total_orders), 2) AS avg_orders,
ROUND(SUM(u_total_order_costs) / SUM(u_total_orders), 2) AS avg_cost
FROM (	-- Подзапрос приписывающий каждому пользователю сегмент.
	SELECT
	CASE
		WHEN u_total_orders = 1 THEN '1 заказ'
		WHEN u_total_orders BETWEEN 2 AND 5 THEN '2 — 5 заказов'
		WHEN u_total_orders BETWEEN 6 AND 10 THEN '6 — 10 заказов'
		WHEN u_total_orders > 11 THEN '11 и более заказов'
		ELSE 'Неизвестно'
	END  AS segment, user_id, u_total_orders, u_total_order_costs
	FROM (-- Подзапрос суммирующий стоимость и количество заказов по пользователям, вне зависимости от региона!
		SELECT user_id,
		SUM(total_orders) AS u_total_orders, 
		SUM(total_order_costs) AS u_total_order_costs
		FROM ds_ecom.product_user_features
		GROUP BY user_id
	) AS t
) AS Ad_hoc_1_preprocessed
GROUP BY segment
ORDER BY count_users DESC;
/*
 * Результирующая таблица показывет наибольшее количество пользователей в сегменте с количеством заказов 1
 * и уменьшается с увеличением количества заказов.
 * Наблюдается уменьшение средней стоимости заказа при увеличении количества заказов на пользователя. 
 */





/* Задача 2. Ранжирование пользователей 
 * Отсортируйте пользователей, сделавших 3 заказа и более, по убыванию среднего чека покупки.  
 * Выведите 15 пользователей с самым большим средним чеком среди указанной группы.
*/
SELECT *,
DENSE_RANK() OVER (ORDER BY avg_order_cost DESC) AS order_cost_rank
FROM ds_ecom.product_user_features
WHERE total_orders >= 3
ORDER BY order_cost_rank
LIMIT 15;
/*
 * Первые 15 пользователей выведенные в таблицу полностью находятся в сегменте 2-5 заказов в предыдущей таблице.
 * Минимальные и максимальные значения среднего чека находятся в диапозоне от 5526.67 до 14716.67 р.
 * что значительно отличается от среднего чека предыдущей таблицы в сегменте '2-5 заказов' = 3058.39
 * Присутствуют пользователи из всех наиболее крупных регионах по количеству заказов.
 */






/* Задача 3. Статистика по регионам. 
 * Для каждого региона подсчитайте:
 * - общее число клиентов и заказов;
 * - среднюю стоимость одного заказа;
 * - долю заказов, которые были куплены в рассрочку;
 * - долю заказов, которые были куплены с использованием промокодов;
 * - долю пользователей, совершивших отмену заказа хотя бы один раз.
*/
SELECT region,
COUNT(user_id) AS count_users,
SUM(total_orders) AS sum_orders,
ROUND( AVG(avg_order_cost), 2) AS avg_cost,
ROUND( (SUM(num_installment_orders)::numeric / SUM(total_orders) * 100), 2) || '%' AS part_installments,
ROUND( (SUM(num_orders_with_promo)::numeric / SUM(total_orders) * 100), 2) || '%' AS part_promo,
--ROUND( (SUM(used_cancel)::numeric / COUNT(user_id) * 100), 2) || '%' AS part_cancel
ROUND( AVG(used_cancel) * 100, 2) || '%' AS part_cancel
FROM ds_ecom.product_user_features
GROUP BY region;
/*
 * Наибольшее количество пользователей и количество их заказов в Москве, 
 * Санкт-Петербург и Новосибирская область обладают приблизительно одинаковыми значеними по этим параметрам.
 * Но средняя стоимость заказов имеет противоположные значения - 
 * в Москве средний чек меньше (3167.5), чем в Санк-Петербурге (3620.16) и Новосибирской области (3519.28), 
 * при этом, значения среднего чека в этих двух регионах приблизительно одинаковые.
 * Рассрочку больше берут в Санкт-Петербурге и Новосибирской области, 
 * значения которых так же приблизительно одинаковые 54.66% и 54.14% соответственно, 
 * в отличии от Москвы, где значение заказов купленных в рассрочку меньше, и составляет 47.73%.
 * По количеству оплат по промоакциям лидирует Санкт-Петербург, затем Москва, менше всего у Новосибирской области.
 * Количество отмененных заказов больше всего у Москвы, затем у Санкт-Петербурга, сеньше всего у Новосибирской области.
 */





/* Задача 4. Активность пользователей по первому месяцу заказа в 2023 году
 * Разбейте пользователей на группы в зависимости от того, в какой месяц 2023 года они совершили первый заказ.
 * Для каждой группы посчитайте:
 * - общее количество клиентов, число заказов и среднюю стоимость одного заказа;
 * - средний рейтинг заказа;
 * - долю пользователей, использующих денежные переводы при оплате;
 * - среднюю продолжительность активности пользователя.
*/
set lc_time = 'ru_RU';
SELECT 
TO_CHAR(num_month, 'TMMonth') AS month,
COUNT(user_id) AS count_user,
SUM(total_orders) AS sum_orders,
ROUND(AVG(avg_order_cost), 2) AS avg_cost,
ROUND(AVG(avg_order_rating), 2) AS avg_rating,
ROUND( (( SUM(used_money_transfer)::numeric / COUNT(user_id) ) * 100), 2) AS part_money_transfer,
DATE_TRUNC('minute', AVG(lifetime)) AS avg_lifetime
FROM (
	SELECT DATE_TRUNC('month', first_order_ts::timestamp) AS num_month, 
	user_id, total_orders, avg_order_cost, avg_order_rating, used_money_transfer, lifetime
	FROM ds_ecom.product_user_features
	WHERE EXTRACT(YEAR FROM first_order_ts) = 2023
) AS t
GROUP BY num_month
ORDER BY num_month;
/*
 * Если отсортировать таблицу по полю count_user - пользователям совершивших первый заказ в месяце Х,
 * то можно заметить рост числа пользователей и количества заказов с каждым следующим месяцем, 
 * из данной закономерности лишь выбивается 11 месяц где количество пользователей и заказов резко увеличилось,
 * в 12 месяце рост стабилизировался.
 * По параметру средней стоимости заказов аналогичной закономерности ненаблюдается, 
 * в течении года средняя стоимость заказа колеблется от 2581.28 до 3311.92.
 * Средний рейтинг колеблется в диапозоне от 4 до 4.32 
 * с наименьшим его значением 4 в 11 месяце в котором было наибольшее количество заказов и новых пользователей.
 * Поле part_money_transfer - доли пользователей, использующих денежные переводы при оплате,
 * так же имеет наименьшее значение в 11 месяце.
 * При сортировке по полю avg_lifetime продолжительность активности пользователя имеет тенденцию к уменьшению в течении года.
 */