
# 数据探查sql
```sql
select uri
     ,length(get_json_object(kv,'$.tid')) AS index_id_length
     ,max(get_json_object(kv,'$.tid'))    AS index_id_max
     ,count(1)                                  As row_count
from pcg_atta_public_tdbank::t_atta_v1_0e600066510
where tdbank_imp_date >= 2026031300
  and tdbank_imp_date <=2026031323
  and code = 0
  and length(get_json_object(kv,'$.tid'))>0
group by uri,length(get_json_object(kv,'$.tid'))
order by uri desc;
```

#  数据情况
-- 说明：某个uri不存在，说明index_id_length的值是null或空字符串

``` text
uri	index_id_length	index_id_max	row_count
/user/follow	19	1859525658304053483	105
/user/follow	14	20260312A08YCK	11
/topic/vote	19	1858612377494225086	59
/topic/support	19	1859530138670072052	46
/topic/create	19	1859532856364105968	7195
/reply/support	19	1859530138670072052	573
/reply/create	19	1859521731339747403	254
/match/share	14	20260313A057KK	2935
/match/share	19	1859532779472027888	174033
```