# 字段加工逻辑
```sql
    SELECT CASE WHEN length(get_json_object(kv,'$.content')) > 1 THEN get_json_object(kv,'$.content')
                WHEN length(get_json_object(kv,'$.text')) > 1 THEN get_json_object(kv,'$.text') ELSE '0' END AS content
    FROM pcg_atta_public_tdbank::t_atta_v1_0e600066510
    WHERE tdbank_imp_date = 2026031309
      AND code = 0
```