INSERT INTO TELEGRAMTELEGRAMYANDEXRU__DWH.l_user_group_activity
(hk_l_user_group_activity, hk_user_id,hk_group_id,load_dt,load_src)

SELECT DISTINCT

	hash(hu.hk_user_id, hg.hk_group_id),
	hu.hk_user_id,
	hg.hk_group_id,
	now() as load_dt,
	's3' as load_src

FROM TELEGRAMTELEGRAMYANDEXRU__STAGING.group_log as gl
LEFT JOIN TELEGRAMTELEGRAMYANDEXRU__DWH.h_users hu ON gl.user_id = hu.user_id 
LEFT JOIN TELEGRAMTELEGRAMYANDEXRU__DWH.h_groups hg ON gl.group_id = hg.group_id 
WHERE hash(hu.hk_user_id, hg.hk_group_id) NOT IN (
	SELECT hk_l_user_group_activity 
	FROM TELEGRAMTELEGRAMYANDEXRU__DWH.l_user_group_activity
	); 
