# 字段加工逻辑
```sql
    SELECT original_kv_list.vid AS vid
    FROM (SELECT kv AS original_kv FROM pcg_atta_public_tdbank::t_atta_v1_0e600066510 WHERE tdbank_imp_date = 2026031309 AND code = 0) t1
    lateral view json_tuple(original_kv, 'vid') original_kv_list AS vid
```