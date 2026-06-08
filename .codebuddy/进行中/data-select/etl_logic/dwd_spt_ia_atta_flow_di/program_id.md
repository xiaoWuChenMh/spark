# 字段加工逻辑
```sql
    SELECT CASE WHEN uri = '/dmComment/create' AND get_json_object(kv,'$.commentFrom') = '1' THEN get_json_object(get_json_object(kv,'$.liveInfo'),'$.programId') ELSE get_json_object(kv,'$.program_id') END AS program_id
    FROM pcg_atta_public_tdbank::t_atta_v1_0e600066510
    WHERE tdbank_imp_date = 2026031309
      AND code = 0
```