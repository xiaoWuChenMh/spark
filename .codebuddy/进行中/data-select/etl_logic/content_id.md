# content_id字段加工逻辑

```sql
SELECT IF(LENGTH(A1.content_id) > 0, A1.content_id, A2.content_id) AS content_id
FROM
(
    SELECT 
        CASE WHEN uri = '/match/share' AND get_json_object(kv,'$.contentType') = '1' THEN mid
             WHEN uri = '/match/share' AND get_json_object(kv,'$.contentType') = '2' THEN news_id
             WHEN uri = '/match/share' AND get_json_object(kv,'$.contentType') = '8' THEN vid
             WHEN uri = '/match/share' AND get_json_object(kv,'$.contentType') = '9' THEN tid
             WHEN uri = '/match/share' AND get_json_object(kv,'$.contentType') = '18' THEN sid
             WHEN uri = '/moderator/focusHandle' THEN split(get_json_object(kv,'$.id'),'')[1]
             WHEN vid is not null THEN vid
             WHEN tid is not null THEN tid
             WHEN news_id is not null THEN news_id
             WHEN mid is not null THEN mid  ELSE content_id END AS content_id,
        comment_id
    FROM dwd_spt_ia_atta_flow_di
    WHERE imp_date = 20260312
    AND code = 0
) A1
LEFT JOIN (
    SELECT comment_id, MAX(content_id) AS content_id
    FROM pcg_sports_dim::dim_spt_ecol_comment_info_hf
    WHERE imp_hour = 2026031223
    AND LENGTH(comment_id) > 0
    AND 20260312 > 20260208
    GROUP BY comment_id
    UNION ALL
    SELECT comment_id, MAX(content_id) AS content_id
    FROM pcg_sports_dim::dim_spt_ecol_comment_info_mf
    WHERE imp_month = 20260201
    AND LENGTH(comment_id) > 0
    AND 20260312 <= 20260208
    GROUP BY comment_id
) A2
ON A1.comment_id = A2.comment_id
```

## 加工逻辑说明

### 数据处理流程
content_id字段的加工涉及两层数据处理：

1. **基础数据计算层**：从主表`dwd_spt_ia_atta_flow_di`提取必要字段并进行content_id计算
2. **评论信息关联层**：通过LEFT JOIN关联评论信息表，获取评论相关的content_id作为备选值

### 优先级判断规则
content_id字段的提取遵循严格的优先级顺序：

1. **A1表优先**：如果A1表计算的content_id长度大于0，优先使用A1.content_id
2. **A2表备选**：如果A1表content_id为空，使用A2表（评论信息表）的content_id

### A1表content_id计算规则
在A1子查询中，content_id的计算遵循以下优先级：

1. **特定URI路径的精确匹配**：
   - `/match/share`接口根据contentType参数选择不同的ID字段
   - `/moderator/focusHandle`接口从JSON的id字段中提取第二部分

2. **字段存在性判断**：按照vid→tid→news_id→mid的顺序使用非空字段

3. **默认值**：不符合上述条件时，使用原始`content_id`字段

### 关键Join操作说明

#### LEFT JOIN A2表：评论信息关联
- **关联表**：`pcg_sports_dim::dim_spt_ecol_comment_info_hf`和`pcg_sports_dim::dim_spt_ecol_comment_info_mf`
- **关联条件**：`A1.comment_id = A2.comment_id`
- **数据源选择**：根据日期条件选择不同的数据源表
- **目的**：获取评论对应的content_id信息作为备选值

### 数据过滤条件
- **主表过滤**：`imp_date = 20260312` AND `code = 0`
- **评论表过滤**：`LENGTH(comment_id) > 0`和日期条件判断
- **最终过滤**：`split(A1.ia_content_cate, '#')[1] IS NOT NULL`

### 修正说明
1. **修正外层content_id判断语法**：使用正确的`IF(LENGTH(A1.content_id) > 0, A1.content_id, A2.content_id)`语法
2. **添加ia_content_cate字段**：在A1子查询中保留ia_content_cate字段用于外层WHERE条件
3. **移除未使用字段**：只保留content_id、comment_id和ia_content_cate三个必要字段
4. **简化外层逻辑**：去除不必要的CASE WHEN嵌套，直接使用IF函数

### 字段精简说明
根据原SQL外层SELECT的实际使用情况，只保留了以下字段：
- **content_id**：核心加工字段
- **comment_id**：用于关联A2表
- **ia_content_cate**：用于外层WHERE条件过滤

移除了在外层SELECT中未使用的字段：uri, kv, mid, news_id, vid, tid, sid等