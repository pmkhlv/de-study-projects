-- TELEGRAMTELEGRAMYANDEXRU__STAGING.group_log definition

CREATE TABLE TELEGRAMTELEGRAMYANDEXRU__STAGING.group_log
(
    group_id int NOT NULL,
    user_id int,
    user_id_from int,
    event varchar(30),
    group_log_dt timestamp,
    CONSTRAINT C_PRIMARY PRIMARY KEY (group_id) DISABLED
);


CREATE PROJECTION TELEGRAMTELEGRAMYANDEXRU__STAGING.group_log /*+createtype(P)*/ 
(
 group_id,
 user_id,
 user_id_from,
 event,
 group_log_dt
)
AS
 SELECT group_log.group_id,
        group_log.user_id,
        group_log.user_id_from,
        group_log.event,
        group_log.group_log_dt
 FROM TELEGRAMTELEGRAMYANDEXRU__STAGING.group_log
 ORDER BY group_log.group_id,
          group_log.user_id
SEGMENTED BY group_log.group_id ALL NODES KSAFE 1;


SELECT MARK_DESIGN_KSAFE(1);