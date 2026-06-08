# comment_id字段加工逻辑

```sql
SELECT CASE WHEN LENGTH(comment_id) > 3 THEN comment_id
             WHEN uri IN ('/moderator/sinkHandle','/moderator/reportedHandle') THEN get_json_object(kv,'$.id')
             WHEN uri = '/reportHandle' AND get_json_object(kv,'$.type') IN ('reply','news_reply') THEN get_json_object(kv,'$.id')  
             ELSE '' END AS comment_id
FROM dwd_spt_ia_atta_flow_di
WHERE imp_date = 20260312
AND code = 0

```

## 加工逻辑说明

1. **优先使用原始comment_id**：当原始comment_id长度大于3时，直接使用原始值

2. **特定URI路径的JSON解析**：
   - 对于URI为`/moderator/sinkHandle`或`/moderator/reportedHandle`的请求，从kv字段的JSON中提取`id`字段
   - 对于URI为`/reportHandle`且type为`reply`或`news_reply`的请求，从kv字段的JSON中提取`id`字段

3. **默认值处理**：不符合上述条件时，返回空字符串

## 数据来源
- 主表：`dwd_spt_ia_atta_flow_di`
- 关键字段：`comment_id`, `uri`, `kv`
- 过滤条件：`imp_date = 20260312` AND `code = 0`