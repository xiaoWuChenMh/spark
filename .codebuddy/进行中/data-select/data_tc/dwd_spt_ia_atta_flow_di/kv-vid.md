
# 数据探查sql
```sql
select uri
     ,length(get_json_object(kv,'$.vid')) AS index_id_length
     ,max(get_json_object(kv,'$.vid'))    AS index_id_max
     ,count(1)                                  As row_count
from pcg_atta_public_tdbank::t_atta_v1_0e600066510
where tdbank_imp_date >= 2026031300
  and tdbank_imp_date <=2026031323
  and code = 0
  and length(get_json_object(kv,'$.vid'))>0
group by uri,length(get_json_object(kv,'$.vid'))
order by uri desc;
```

#  数据情况
-- 说明：某个uri不存在，说明index_id_length的值是null或空字符串

``` text
uri	index_id_length	index_id_max	row_count
/videoDm/publish	11	z4101xfd8pg	648
/video/thumbUp	11	v1259s16eec	11
/user/follow	11	z410187mf7i	72
/match/share	11	z4101yn9404	1936
/match/share	10	1393564300	1
/comment/reply	11	s1258xthge9	1
/access/publish	11	z1259xreg87	742
```