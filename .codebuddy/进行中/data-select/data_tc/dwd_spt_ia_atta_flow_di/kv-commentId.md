
# 数据探查sql
```sql
select uri
     ,length(get_json_object(kv,'$.commentId')) AS index_id_length
     ,max(get_json_object(kv,'$.commentId'))    AS index_id_max
     ,count(1)                                  As row_count
from pcg_atta_public_tdbank::t_atta_v1_0e600066510
where tdbank_imp_date >= 2026031300
  and tdbank_imp_date <=2026031323
  and code = 0
  and length(get_json_object(kv,'$.commentId'))>0
group by uri,length(get_json_object(kv,'$.commentId'))
order by uri desc;

```

#  数据情况
-- 说明：某个uri不存在，说明index_id_length的值是null或空字符串
``` text
uri	index_id_length	index_id_max	row_count
/videoDm/publish	1	0	18
/videoDm/publish	18	338083660374740590	630
/v2/comment/visible	19	1859529019155808349	28
/v2/comment/support	19	1859532693430075485	37619
/v2/comment/setElite	19	1859492018487558293	3
/v2/comment/delete	19	1859532828582084758	390
/report/report	19	1859530303675039894	1081
/report/deal	19	1859529241379471509	665
/reply/support	19	1859531075410198678	573
/moderator/focusHandle	19	1859532466305368213	7710
/dmComment/create	18	338083707753600460	96101
/comment/up	19	1859502851032613012	3
/comment/reply	19	1859530448390062229	55
/comment/flow	19	1859522163268124765	155
/comment/create	19	1859507862387032212	1
```