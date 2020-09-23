set hive.vectorized.execution.enabled=true;
set hive.exec.parallel=true;
set hive.auto.convert.join=false;
set hive.txn.manager = org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

drop table if exists datahub_test.iacre04_acr_cls_temp purge; 

create table datahub_test.iacre04_acr_cls_temp like dsc10715_acr_lz_db.iacre04_acr_cls;

insert	overwrite table datahub_test.iacre04_acr_cls_temp 
SELECT	q1.acr552_id,q1.acre01_acr_class_c,q1.acre02_acr_class_level_c,q1.acre04_create_user_c,q1.acre04_create_s,q1.acre04_update_user_c,q1.acre04_update_s
FROM 
( 
SELECT	'INSERT' AS `header__operation`, '0' AS `header__change_seq`,acr552_id,acre01_acr_class_c,acre02_acr_class_level_c, acre04_create_user_c,acre04_create_s,acre04_update_user_c,acre04_update_s FROM	dsc10715_acr_lz_db.iacre04_acr_cls WHERE acr552_id = 31372
UNION	ALL 
SELECT	header__operation,header__change_seq,acr552_id,acre01_acr_class_c,acre02_acr_class_level_c,acre04_create_user_c,acre04_create_s,acre04_update_user_c,acre04_update_s FROM dsc10715_acr_lz_db.iacre04_acr_cls__ct WHERE ( header__operation = 'INSERT' OR header__operation = 'UPDATE' ) and acr552_id = 31372 
) q1 
JOIN ( 
SELECT	q2.acr552_id,q2.acre01_acr_class_c, Max(q2.header__change_seq) as max_seq
FROM	( 
SELECT	'0' AS header__change_seq,acr552_id,acre01_acr_class_c,acre02_acr_class_level_c,acre04_create_user_c,acre04_create_s,acre04_update_user_c,acre04_update_s FROM	dsc10715_acr_lz_db.iacre04_acr_cls WHERE acr552_id = 31372 
UNION	ALL 
SELECT	header__change_seq,acr552_id,acre01_acr_class_c,acre02_acr_class_level_c, acre04_create_user_c,acre04_create_s,acre04_update_user_c,acre04_update_s FROM dsc10715_acr_lz_db.iacre04_acr_cls__ct WHERE acr552_id = 31372 ) AS q2 GROUP BY q2.acr552_id,q2.acre01_acr_class_c) AS a ON q1.acr552_id = a.acr552_id AND q1.acre01_acr_class_c = a.acre01_acr_class_c AND q1.header__change_seq = a.max_seq WHERE	q1.acr552_id = 31372;

drop table if exists datahub_test.iacre04_acr_cls_Oozie purge;
alter table datahub_test.iacre04_acr_cls_temp rename to datahub_test.iacre04_acr_cls_Oozie;