-- TELEGRAMTELEGRAMYANDEXRU__DWH.h_dialogs definition

CREATE TABLE TELEGRAMTELEGRAMYANDEXRU__DWH.h_dialogs
(
    hk_message_id int NOT NULL,
    message_id int,
    "datetime" timestamp,
    load_dt timestamp,
    load_src varchar(20),
    CONSTRAINT C_PRIMARY PRIMARY KEY (hk_message_id) DISABLED
)
PARTITION BY ((h_dialogs.load_dt)::date) GROUP BY (CASE WHEN ("datediff"('year', (h_dialogs.load_dt)::date, ((now())::timestamptz(6))::date) >= 2) THEN (date_trunc('year', (h_dialogs.load_dt)::date))::date WHEN ("datediff"('month', (h_dialogs.load_dt)::date, ((now())::timestamptz(6))::date) >= 3) THEN (date_trunc('month', (h_dialogs.load_dt)::date))::date ELSE (h_dialogs.load_dt)::date END);


CREATE PROJECTION TELEGRAMTELEGRAMYANDEXRU__DWH.h_dialogs /*+createtype(P)*/ 
(
 hk_message_id,
 message_id,
 "datetime",
 load_dt,
 load_src
)
AS
 SELECT h_dialogs.hk_message_id,
        h_dialogs.message_id,
        h_dialogs."datetime",
        h_dialogs.load_dt,
        h_dialogs.load_src
 FROM TELEGRAMTELEGRAMYANDEXRU__DWH.h_dialogs
 ORDER BY h_dialogs.load_dt
SEGMENTED BY h_dialogs.hk_message_id ALL NODES KSAFE 1;


SELECT MARK_DESIGN_KSAFE(1);


-- TELEGRAMTELEGRAMYANDEXRU__DWH.h_groups definition

CREATE TABLE TELEGRAMTELEGRAMYANDEXRU__DWH.h_groups
(
    hk_group_id int NOT NULL,
    group_id int,
    registration_dt timestamp,
    load_dt timestamp,
    load_src varchar(20),
    CONSTRAINT C_PRIMARY PRIMARY KEY (hk_group_id) DISABLED
)
PARTITION BY ((h_groups.load_dt)::date) GROUP BY (CASE WHEN ("datediff"('year', (h_groups.load_dt)::date, ((now())::timestamptz(6))::date) >= 2) THEN (date_trunc('year', (h_groups.load_dt)::date))::date WHEN ("datediff"('month', (h_groups.load_dt)::date, ((now())::timestamptz(6))::date) >= 3) THEN (date_trunc('month', (h_groups.load_dt)::date))::date ELSE (h_groups.load_dt)::date END);


CREATE PROJECTION TELEGRAMTELEGRAMYANDEXRU__DWH.h_groups /*+createtype(P)*/ 
(
 hk_group_id,
 group_id,
 registration_dt,
 load_dt,
 load_src
)
AS
 SELECT h_groups.hk_group_id,
        h_groups.group_id,
        h_groups.registration_dt,
        h_groups.load_dt,
        h_groups.load_src
 FROM TELEGRAMTELEGRAMYANDEXRU__DWH.h_groups
 ORDER BY h_groups.load_dt
SEGMENTED BY h_groups.hk_group_id ALL NODES KSAFE 1;


SELECT MARK_DESIGN_KSAFE(1);


-- TELEGRAMTELEGRAMYANDEXRU__DWH.h_users definition

CREATE TABLE TELEGRAMTELEGRAMYANDEXRU__DWH.h_users
(
    hk_user_id int NOT NULL,
    user_id int,
    registration_dt timestamp,
    load_dt timestamp,
    load_src varchar(20),
    CONSTRAINT C_PRIMARY PRIMARY KEY (hk_user_id) DISABLED
)
PARTITION BY ((h_users.load_dt)::date) GROUP BY (CASE WHEN ("datediff"('year', (h_users.load_dt)::date, ((now())::timestamptz(6))::date) >= 2) THEN (date_trunc('year', (h_users.load_dt)::date))::date WHEN ("datediff"('month', (h_users.load_dt)::date, ((now())::timestamptz(6))::date) >= 3) THEN (date_trunc('month', (h_users.load_dt)::date))::date ELSE (h_users.load_dt)::date END);


CREATE PROJECTION TELEGRAMTELEGRAMYANDEXRU__DWH.h_users /*+createtype(P)*/ 
(
 hk_user_id,
 user_id,
 registration_dt,
 load_dt,
 load_src
)
AS
 SELECT h_users.hk_user_id,
        h_users.user_id,
        h_users.registration_dt,
        h_users.load_dt,
        h_users.load_src
 FROM TELEGRAMTELEGRAMYANDEXRU__DWH.h_users
 ORDER BY h_users.load_dt
SEGMENTED BY h_users.hk_user_id ALL NODES KSAFE 1;


SELECT MARK_DESIGN_KSAFE(1);


-- TELEGRAMTELEGRAMYANDEXRU__DWH.l_admins definition

CREATE TABLE TELEGRAMTELEGRAMYANDEXRU__DWH.l_admins
(
    hk_l_admin_id int NOT NULL,
    hk_user_id int NOT NULL,
    hk_group_id int NOT NULL,
    load_dt timestamp,
    load_src varchar(20),
    CONSTRAINT C_PRIMARY PRIMARY KEY (hk_l_admin_id) DISABLED
)
PARTITION BY ((l_admins.load_dt)::date) GROUP BY (CASE WHEN ("datediff"('year', (l_admins.load_dt)::date, ((now())::timestamptz(6))::date) >= 2) THEN (date_trunc('year', (l_admins.load_dt)::date))::date WHEN ("datediff"('month', (l_admins.load_dt)::date, ((now())::timestamptz(6))::date) >= 3) THEN (date_trunc('month', (l_admins.load_dt)::date))::date ELSE (l_admins.load_dt)::date END);


ALTER TABLE TELEGRAMTELEGRAMYANDEXRU__DWH.l_admins ADD CONSTRAINT fk_l_user_admin FOREIGN KEY (hk_user_id) references TELEGRAMTELEGRAMYANDEXRU__DWH.h_users (hk_user_id);
ALTER TABLE TELEGRAMTELEGRAMYANDEXRU__DWH.l_admins ADD CONSTRAINT fk_l_admin_group FOREIGN KEY (hk_group_id) references TELEGRAMTELEGRAMYANDEXRU__DWH.h_groups (hk_group_id);

CREATE PROJECTION TELEGRAMTELEGRAMYANDEXRU__DWH.l_admins /*+createtype(P)*/ 
(
 hk_l_admin_id,
 hk_user_id,
 hk_group_id,
 load_dt,
 load_src
)
AS
 SELECT l_admins.hk_l_admin_id,
        l_admins.hk_user_id,
        l_admins.hk_group_id,
        l_admins.load_dt,
        l_admins.load_src
 FROM TELEGRAMTELEGRAMYANDEXRU__DWH.l_admins
 ORDER BY l_admins.load_dt
SEGMENTED BY l_admins.hk_l_admin_id ALL NODES KSAFE 1;


SELECT MARK_DESIGN_KSAFE(1);


-- TELEGRAMTELEGRAMYANDEXRU__DWH.l_groups_dialogs definition

CREATE TABLE TELEGRAMTELEGRAMYANDEXRU__DWH.l_groups_dialogs
(
    hk_l_groups_dialogs int NOT NULL,
    hk_message_id int NOT NULL,
    hk_group_id int NOT NULL,
    load_dt timestamp,
    load_src varchar(20),
    CONSTRAINT C_PRIMARY PRIMARY KEY (hk_l_groups_dialogs) DISABLED
)
PARTITION BY ((l_groups_dialogs.load_dt)::date) GROUP BY (CASE WHEN ("datediff"('year', (l_groups_dialogs.load_dt)::date, ((now())::timestamptz(6))::date) >= 2) THEN (date_trunc('year', (l_groups_dialogs.load_dt)::date))::date WHEN ("datediff"('month', (l_groups_dialogs.load_dt)::date, ((now())::timestamptz(6))::date) >= 3) THEN (date_trunc('month', (l_groups_dialogs.load_dt)::date))::date ELSE (l_groups_dialogs.load_dt)::date END);


ALTER TABLE TELEGRAMTELEGRAMYANDEXRU__DWH.l_groups_dialogs ADD CONSTRAINT fk_l_groups_dialogs_message FOREIGN KEY (hk_message_id) references TELEGRAMTELEGRAMYANDEXRU__DWH.h_dialogs (hk_message_id);
ALTER TABLE TELEGRAMTELEGRAMYANDEXRU__DWH.l_groups_dialogs ADD CONSTRAINT fk_l_groups_dialogs_group FOREIGN KEY (hk_group_id) references TELEGRAMTELEGRAMYANDEXRU__DWH.h_groups (hk_group_id);

CREATE PROJECTION TELEGRAMTELEGRAMYANDEXRU__DWH.l_groups_dialogs /*+createtype(P)*/ 
(
 hk_l_groups_dialogs,
 hk_message_id,
 hk_group_id,
 load_dt,
 load_src
)
AS
 SELECT l_groups_dialogs.hk_l_groups_dialogs,
        l_groups_dialogs.hk_message_id,
        l_groups_dialogs.hk_group_id,
        l_groups_dialogs.load_dt,
        l_groups_dialogs.load_src
 FROM TELEGRAMTELEGRAMYANDEXRU__DWH.l_groups_dialogs
 ORDER BY l_groups_dialogs.load_dt
SEGMENTED BY l_groups_dialogs.hk_l_groups_dialogs ALL NODES KSAFE 1;


SELECT MARK_DESIGN_KSAFE(1);


-- TELEGRAMTELEGRAMYANDEXRU__DWH.l_user_group_activity definition

CREATE TABLE TELEGRAMTELEGRAMYANDEXRU__DWH.l_user_group_activity
(
    hk_l_user_group_activity int NOT NULL,
    hk_user_id int NOT NULL,
    hk_group_id int NOT NULL,
    load_dt timestamp,
    load_src varchar(20),
    CONSTRAINT C_PRIMARY PRIMARY KEY (hk_l_user_group_activity) DISABLED
)
PARTITION BY ((l_user_group_activity.load_dt)::date);


ALTER TABLE TELEGRAMTELEGRAMYANDEXRU__DWH.l_user_group_activity ADD CONSTRAINT fk_l_user_group_activity_h_users FOREIGN KEY (hk_user_id) references TELEGRAMTELEGRAMYANDEXRU__DWH.h_users (hk_user_id);
ALTER TABLE TELEGRAMTELEGRAMYANDEXRU__DWH.l_user_group_activity ADD CONSTRAINT fk_l_user_group_activity_h_groups FOREIGN KEY (hk_group_id) references TELEGRAMTELEGRAMYANDEXRU__DWH.h_groups (hk_group_id);

CREATE PROJECTION TELEGRAMTELEGRAMYANDEXRU__DWH.l_user_group_activity /*+createtype(P)*/ 
(
 hk_l_user_group_activity,
 hk_user_id,
 hk_group_id,
 load_dt,
 load_src
)
AS
 SELECT l_user_group_activity.hk_l_user_group_activity,
        l_user_group_activity.hk_user_id,
        l_user_group_activity.hk_group_id,
        l_user_group_activity.load_dt,
        l_user_group_activity.load_src
 FROM TELEGRAMTELEGRAMYANDEXRU__DWH.l_user_group_activity
 ORDER BY l_user_group_activity.load_dt
SEGMENTED BY l_user_group_activity.hk_l_user_group_activity ALL NODES KSAFE 1;


SELECT MARK_DESIGN_KSAFE(1);


-- TELEGRAMTELEGRAMYANDEXRU__DWH.l_user_message definition

CREATE TABLE TELEGRAMTELEGRAMYANDEXRU__DWH.l_user_message
(
    hk_l_user_message int NOT NULL,
    hk_user_id int NOT NULL,
    hk_message_id int NOT NULL,
    load_dt timestamp,
    load_src varchar(20),
    CONSTRAINT C_PRIMARY PRIMARY KEY (hk_l_user_message) DISABLED
)
PARTITION BY ((l_user_message.load_dt)::date) GROUP BY (CASE WHEN ("datediff"('year', (l_user_message.load_dt)::date, ((now())::timestamptz(6))::date) >= 2) THEN (date_trunc('year', (l_user_message.load_dt)::date))::date WHEN ("datediff"('month', (l_user_message.load_dt)::date, ((now())::timestamptz(6))::date) >= 3) THEN (date_trunc('month', (l_user_message.load_dt)::date))::date ELSE (l_user_message.load_dt)::date END);


ALTER TABLE TELEGRAMTELEGRAMYANDEXRU__DWH.l_user_message ADD CONSTRAINT fk_l_user_message_user FOREIGN KEY (hk_user_id) references TELEGRAMTELEGRAMYANDEXRU__DWH.h_users (hk_user_id);
ALTER TABLE TELEGRAMTELEGRAMYANDEXRU__DWH.l_user_message ADD CONSTRAINT fk_l_user_message_message FOREIGN KEY (hk_message_id) references TELEGRAMTELEGRAMYANDEXRU__DWH.h_dialogs (hk_message_id);

CREATE PROJECTION TELEGRAMTELEGRAMYANDEXRU__DWH.l_user_message /*+createtype(P)*/ 
(
 hk_l_user_message,
 hk_user_id,
 hk_message_id,
 load_dt,
 load_src
)
AS
 SELECT l_user_message.hk_l_user_message,
        l_user_message.hk_user_id,
        l_user_message.hk_message_id,
        l_user_message.load_dt,
        l_user_message.load_src
 FROM TELEGRAMTELEGRAMYANDEXRU__DWH.l_user_message
 ORDER BY l_user_message.load_dt
SEGMENTED BY l_user_message.hk_user_id ALL NODES KSAFE 1;


SELECT MARK_DESIGN_KSAFE(1);


-- TELEGRAMTELEGRAMYANDEXRU__DWH.s_admins definition

CREATE TABLE TELEGRAMTELEGRAMYANDEXRU__DWH.s_admins
(
    hk_admin_id int NOT NULL,
    is_admin boolean,
    admin_from timestamp,
    load_dt timestamp,
    load_src varchar(20)
)
PARTITION BY ((s_admins.load_dt)::date) GROUP BY (CASE WHEN ("datediff"('year', (s_admins.load_dt)::date, ((now())::timestamptz(6))::date) >= 2) THEN (date_trunc('year', (s_admins.load_dt)::date))::date WHEN ("datediff"('month', (s_admins.load_dt)::date, ((now())::timestamptz(6))::date) >= 3) THEN (date_trunc('month', (s_admins.load_dt)::date))::date ELSE (s_admins.load_dt)::date END);


ALTER TABLE TELEGRAMTELEGRAMYANDEXRU__DWH.s_admins ADD CONSTRAINT fk_s_admins_l_admins FOREIGN KEY (hk_admin_id) references TELEGRAMTELEGRAMYANDEXRU__DWH.l_admins (hk_l_admin_id);

CREATE PROJECTION TELEGRAMTELEGRAMYANDEXRU__DWH.s_admins /*+createtype(P)*/ 
(
 hk_admin_id,
 is_admin,
 admin_from,
 load_dt,
 load_src
)
AS
 SELECT s_admins.hk_admin_id,
        s_admins.is_admin,
        s_admins.admin_from,
        s_admins.load_dt,
        s_admins.load_src
 FROM TELEGRAMTELEGRAMYANDEXRU__DWH.s_admins
 ORDER BY s_admins.load_dt
SEGMENTED BY s_admins.hk_admin_id ALL NODES KSAFE 1;


SELECT MARK_DESIGN_KSAFE(1);


-- TELEGRAMTELEGRAMYANDEXRU__DWH.s_auth_history definition

CREATE TABLE TELEGRAMTELEGRAMYANDEXRU__DWH.s_auth_history
(
    hk_l_user_group_activity int NOT NULL,
    user_id_from int,
    event varchar(30),
    event_dt timestamp,
    load_dt timestamp,
    load_src varchar(30)
)
PARTITION BY ((s_auth_history.load_dt)::date);


ALTER TABLE TELEGRAMTELEGRAMYANDEXRU__DWH.s_auth_history ADD CONSTRAINT fk_s_auth_history_l_user_group_activity FOREIGN KEY (hk_l_user_group_activity) references TELEGRAMTELEGRAMYANDEXRU__DWH.l_user_group_activity (hk_l_user_group_activity);

CREATE PROJECTION TELEGRAMTELEGRAMYANDEXRU__DWH.s_auth_history /*+createtype(P)*/ 
(
 hk_l_user_group_activity,
 user_id_from,
 event,
 event_dt,
 load_dt,
 load_src
)
AS
 SELECT s_auth_history.hk_l_user_group_activity,
        s_auth_history.user_id_from,
        s_auth_history.event,
        s_auth_history.event_dt,
        s_auth_history.load_dt,
        s_auth_history.load_src
 FROM TELEGRAMTELEGRAMYANDEXRU__DWH.s_auth_history
 ORDER BY s_auth_history.load_dt
SEGMENTED BY s_auth_history.hk_l_user_group_activity ALL NODES KSAFE 1;


SELECT MARK_DESIGN_KSAFE(1);


-- TELEGRAMTELEGRAMYANDEXRU__DWH.s_dialog_info definition

CREATE TABLE TELEGRAMTELEGRAMYANDEXRU__DWH.s_dialog_info
(
    hk_message_id int NOT NULL,
    message varchar(1000),
    message_from int,
    message_to int,
    load_dt timestamp,
    load_src varchar(20)
)
PARTITION BY ((s_dialog_info.load_dt)::date) GROUP BY (CASE WHEN ("datediff"('year', (s_dialog_info.load_dt)::date, ((now())::timestamptz(6))::date) >= 2) THEN (date_trunc('year', (s_dialog_info.load_dt)::date))::date WHEN ("datediff"('month', (s_dialog_info.load_dt)::date, ((now())::timestamptz(6))::date) >= 3) THEN (date_trunc('month', (s_dialog_info.load_dt)::date))::date ELSE (s_dialog_info.load_dt)::date END);


ALTER TABLE TELEGRAMTELEGRAMYANDEXRU__DWH.s_dialog_info ADD CONSTRAINT fk_s_dialog_info FOREIGN KEY (hk_message_id) references TELEGRAMTELEGRAMYANDEXRU__DWH.h_dialogs (hk_message_id);

CREATE PROJECTION TELEGRAMTELEGRAMYANDEXRU__DWH.s_dialog_info /*+createtype(P)*/ 
(
 hk_message_id,
 message,
 message_from,
 message_to,
 load_dt,
 load_src
)
AS
 SELECT s_dialog_info.hk_message_id,
        s_dialog_info.message,
        s_dialog_info.message_from,
        s_dialog_info.message_to,
        s_dialog_info.load_dt,
        s_dialog_info.load_src
 FROM TELEGRAMTELEGRAMYANDEXRU__DWH.s_dialog_info
 ORDER BY s_dialog_info.load_dt
SEGMENTED BY s_dialog_info.hk_message_id ALL NODES KSAFE 1;


SELECT MARK_DESIGN_KSAFE(1);


-- TELEGRAMTELEGRAMYANDEXRU__DWH.s_group_name definition

CREATE TABLE TELEGRAMTELEGRAMYANDEXRU__DWH.s_group_name
(
    hk_group_id int NOT NULL,
    group_name varchar(100),
    load_dt timestamp,
    load_src varchar(20)
)
PARTITION BY ((s_group_name.load_dt)::date) GROUP BY (CASE WHEN ("datediff"('year', (s_group_name.load_dt)::date, ((now())::timestamptz(6))::date) >= 2) THEN (date_trunc('year', (s_group_name.load_dt)::date))::date WHEN ("datediff"('month', (s_group_name.load_dt)::date, ((now())::timestamptz(6))::date) >= 3) THEN (date_trunc('month', (s_group_name.load_dt)::date))::date ELSE (s_group_name.load_dt)::date END);


ALTER TABLE TELEGRAMTELEGRAMYANDEXRU__DWH.s_group_name ADD CONSTRAINT fk_s_group_name FOREIGN KEY (hk_group_id) references TELEGRAMTELEGRAMYANDEXRU__DWH.h_groups (hk_group_id);

CREATE PROJECTION TELEGRAMTELEGRAMYANDEXRU__DWH.s_group_name /*+createtype(P)*/ 
(
 hk_group_id,
 group_name,
 load_dt,
 load_src
)
AS
 SELECT s_group_name.hk_group_id,
        s_group_name.group_name,
        s_group_name.load_dt,
        s_group_name.load_src
 FROM TELEGRAMTELEGRAMYANDEXRU__DWH.s_group_name
 ORDER BY s_group_name.load_dt
SEGMENTED BY s_group_name.hk_group_id ALL NODES KSAFE 1;


SELECT MARK_DESIGN_KSAFE(1);


-- TELEGRAMTELEGRAMYANDEXRU__DWH.s_group_private_status definition

CREATE TABLE TELEGRAMTELEGRAMYANDEXRU__DWH.s_group_private_status
(
    hk_group_id int NOT NULL,
    is_private boolean,
    load_dt timestamp,
    load_src varchar(20)
)
PARTITION BY ((s_group_private_status.load_dt)::date) GROUP BY (CASE WHEN ("datediff"('year', (s_group_private_status.load_dt)::date, ((now())::timestamptz(6))::date) >= 2) THEN (date_trunc('year', (s_group_private_status.load_dt)::date))::date WHEN ("datediff"('month', (s_group_private_status.load_dt)::date, ((now())::timestamptz(6))::date) >= 3) THEN (date_trunc('month', (s_group_private_status.load_dt)::date))::date ELSE (s_group_private_status.load_dt)::date END);


ALTER TABLE TELEGRAMTELEGRAMYANDEXRU__DWH.s_group_private_status ADD CONSTRAINT fk_s_group_name FOREIGN KEY (hk_group_id) references TELEGRAMTELEGRAMYANDEXRU__DWH.h_groups (hk_group_id);

CREATE PROJECTION TELEGRAMTELEGRAMYANDEXRU__DWH.s_group_private_status /*+createtype(P)*/ 
(
 hk_group_id,
 is_private,
 load_dt,
 load_src
)
AS
 SELECT s_group_private_status.hk_group_id,
        s_group_private_status.is_private,
        s_group_private_status.load_dt,
        s_group_private_status.load_src
 FROM TELEGRAMTELEGRAMYANDEXRU__DWH.s_group_private_status
 ORDER BY s_group_private_status.load_dt
SEGMENTED BY s_group_private_status.hk_group_id ALL NODES KSAFE 1;


SELECT MARK_DESIGN_KSAFE(1);


-- TELEGRAMTELEGRAMYANDEXRU__DWH.s_user_chatinfo definition

CREATE TABLE TELEGRAMTELEGRAMYANDEXRU__DWH.s_user_chatinfo
(
    hk_user_id int NOT NULL,
    chat_name varchar(1000),
    load_dt timestamp,
    load_src varchar(20)
)
PARTITION BY ((s_user_chatinfo.load_dt)::date) GROUP BY (CASE WHEN ("datediff"('year', (s_user_chatinfo.load_dt)::date, ((now())::timestamptz(6))::date) >= 2) THEN (date_trunc('year', (s_user_chatinfo.load_dt)::date))::date WHEN ("datediff"('month', (s_user_chatinfo.load_dt)::date, ((now())::timestamptz(6))::date) >= 3) THEN (date_trunc('month', (s_user_chatinfo.load_dt)::date))::date ELSE (s_user_chatinfo.load_dt)::date END);


ALTER TABLE TELEGRAMTELEGRAMYANDEXRU__DWH.s_user_chatinfo ADD CONSTRAINT fk_s_user_chainfo FOREIGN KEY (hk_user_id) references TELEGRAMTELEGRAMYANDEXRU__DWH.h_users (hk_user_id);

CREATE PROJECTION TELEGRAMTELEGRAMYANDEXRU__DWH.s_user_chatinfo /*+createtype(P)*/ 
(
 hk_user_id,
 chat_name,
 load_dt,
 load_src
)
AS
 SELECT s_user_chatinfo.hk_user_id,
        s_user_chatinfo.chat_name,
        s_user_chatinfo.load_dt,
        s_user_chatinfo.load_src
 FROM TELEGRAMTELEGRAMYANDEXRU__DWH.s_user_chatinfo
 ORDER BY s_user_chatinfo.load_dt
SEGMENTED BY s_user_chatinfo.hk_user_id ALL NODES KSAFE 1;


SELECT MARK_DESIGN_KSAFE(1);


-- TELEGRAMTELEGRAMYANDEXRU__DWH.s_user_socdem definition

CREATE TABLE TELEGRAMTELEGRAMYANDEXRU__DWH.s_user_socdem
(
    hk_user_id int NOT NULL,
    country varchar(100),
    age int,
    load_dt timestamp,
    load_src varchar(20)
)
PARTITION BY ((s_user_socdem.load_dt)::date) GROUP BY (CASE WHEN ("datediff"('year', (s_user_socdem.load_dt)::date, ((now())::timestamptz(6))::date) >= 2) THEN (date_trunc('year', (s_user_socdem.load_dt)::date))::date WHEN ("datediff"('month', (s_user_socdem.load_dt)::date, ((now())::timestamptz(6))::date) >= 3) THEN (date_trunc('month', (s_user_socdem.load_dt)::date))::date ELSE (s_user_socdem.load_dt)::date END);


ALTER TABLE TELEGRAMTELEGRAMYANDEXRU__DWH.s_user_socdem ADD CONSTRAINT fk_s_user_socdem FOREIGN KEY (hk_user_id) references TELEGRAMTELEGRAMYANDEXRU__DWH.h_users (hk_user_id);

CREATE PROJECTION TELEGRAMTELEGRAMYANDEXRU__DWH.s_user_socdem /*+createtype(P)*/ 
(
 hk_user_id,
 country,
 age,
 load_dt,
 load_src
)
AS
 SELECT s_user_socdem.hk_user_id,
        s_user_socdem.country,
        s_user_socdem.age,
        s_user_socdem.load_dt,
        s_user_socdem.load_src
 FROM TELEGRAMTELEGRAMYANDEXRU__DWH.s_user_socdem
 ORDER BY s_user_socdem.load_dt
SEGMENTED BY s_user_socdem.hk_user_id ALL NODES KSAFE 1;


SELECT MARK_DESIGN_KSAFE(1);