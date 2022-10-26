
WITH 
	ten_oldests_groups AS (
		SELECT hg.hk_group_id, hg.registration_dt
		FROM TELEGRAMTELEGRAMYANDEXRU__DWH.h_groups hg 
		ORDER BY hg.registration_dt
		LIMIT 10),
	
	user_group_messages AS (
		SELECT lgd.hk_group_id, COUNT (DISTINCT hu.hk_user_id) AS cnt_users_in_group_with_messages
		FROM TELEGRAMTELEGRAMYANDEXRU__DWH.h_users  hu
		INNER JOIN TELEGRAMTELEGRAMYANDEXRU__DWH.l_user_message lum ON hu.hk_user_id = lum.hk_user_id
		INNER JOIN TELEGRAMTELEGRAMYANDEXRU__DWH.h_dialogs hd ON hd.hk_message_id = lum.hk_message_id 
		INNER JOIN TELEGRAMTELEGRAMYANDEXRU__DWH.l_groups_dialogs lgd ON lgd.hk_message_id = hd.hk_message_id 
		GROUP BY 1),
		
	user_group_log AS (
		SELECT hg.hk_group_id, COUNT (DISTINCT hu.user_id) AS cnt_added_users
		FROM TELEGRAMTELEGRAMYANDEXRU__DWH.h_groups hg 
		INNER JOIN TELEGRAMTELEGRAMYANDEXRU__DWH.l_user_group_activity luga ON hg.hk_group_id = luga.hk_group_id 
		INNER JOIN TELEGRAMTELEGRAMYANDEXRU__DWH.h_users hu ON hu.hk_user_id = luga.hk_user_id 
		INNER JOIN TELEGRAMTELEGRAMYANDEXRU__DWH.s_auth_history sah ON sah.hk_l_user_group_activity = luga.hk_l_user_group_activity
		WHERE event = 'add'
		GROUP BY 1)
		
SELECT 
	tog.hk_group_id, 
	cnt_users_in_group_with_messages,
	cnt_added_users, 
	ROUND (cnt_users_in_group_with_messages / cnt_added_users * 100, 2) AS "group_conversion, %"
FROM
ten_oldests_groups tog
INNER JOIN user_group_messages ugm ON tog.hk_group_id = ugm.hk_group_id
INNER JOIN user_group_log ugl ON tog.hk_group_id = ugl.hk_group_id
ORDER BY 4 DESC;