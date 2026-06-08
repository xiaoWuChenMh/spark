# 字段加工逻辑
```sql
    SELECT CASE WHEN uri = '/report/deal' AND LENGTH(split(get_json_object(kv,'$.contentId'),'')[1]) > 0 THEN split(get_json_object(kv,'$.contentId'),'')[0]
                WHEN uri = '/report/deal' THEN '' ELSE split(get_json_object(kv,'$.contentId'),'')[0] END AS content_type
    FROM pcg_atta_public_tdbank::t_atta_v1_0e600066510
    WHERE tdbank_imp_date = 2026031309
      AND code = 0
```