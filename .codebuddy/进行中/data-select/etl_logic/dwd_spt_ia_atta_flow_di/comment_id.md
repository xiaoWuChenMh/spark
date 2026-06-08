

# 字段加工逻辑
```sql
    SELECT CASE WHEN length(get_json_object(kv,'$.commentId')) is not null THEN get_json_object(kv,'$.commentId')
                WHEN length(get_json_object(kv,'$.comment.commentId')) is not null THEN get_json_object(kv,'$.comment.commentId')
                WHEN uri IN ('/reportHandle','/moderator/sinkHandle','/moderator/reportedHandle') THEN get_json_object(kv,'$.id')
                WHEN uri = '/moderator/focusHandle' AND get_json_object(kv,'$.subFrom') = 'focus' AND get_json_object(kv,'$.op') = 'pass' THEN get_json_object(kv,'$.id') ELSE '' END AS comment_id
    FROM pcg_atta_public_tdbank::t_atta_v1_0e600066510
    WHERE tdbank_imp_date = 2026031309
      AND code = 0
```