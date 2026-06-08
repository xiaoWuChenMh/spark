
# 数据探查sql
```sql
select uri
     ,length(get_json_object(kv,'$.contentId')) AS index_id_length
     ,max(get_json_object(kv,'$.contentId'))    AS index_id_max
     ,count(1)                                  As row_count
from pcg_atta_public_tdbank::t_atta_v1_0e600066510
where tdbank_imp_date >= 2026031300
  and tdbank_imp_date <=2026031323
  and code = 0
  and length(get_json_object(kv,'$.contentId'))>0
group by uri,length(get_json_object(kv,'$.contentId'))
order by uri desc;
```

#  数据情况
-- 说明：某个uri不存在，说明index_id_length的值是null或空字符串

``` text
uri	index_id_length	index_id_max	row_count
/v2/content/support	13	1_z4101yn9404	24683
/v2/content/support	21	3_1859532345093128434	43115
/v2/content/support	16	2_20260313A057KK	1583
/v2/comment/support	20	4_100000:10022500958	2
/v2/comment/support	17	4_100002:20258006	12
/v2/comment/support	26	4_100000:56336684_13_88608	1
/v2/comment/support	13	1_z4101tv9765	9135
/v2/comment/support	21	3_1859530138670072052	28148
/v2/comment/support	16	2_20260313A052I4	321
/v2/comment/publish	7	17_22_1	1
/v2/comment/publish	21	3_1859531929157632243	25561
/v2/comment/publish	17	4_100002:20258105	37
/v2/comment/publish	18	4_100008:100108576	121
/v2/comment/publish	13	1_z4101n5pnm6	7115
/v2/comment/publish	16	4_160004:3904265	743
/v2/comment/publish	11	4_8:2562177	3
/v2/comment/publish	20	4_100001:12022500455	41730
/v2/comment/delete	21	3_1859530715141505217	304
/v2/comment/delete	16	2_20260313A01YJ0	6
/v2/comment/delete	13	1_z1258gfj4qk	80
/topic/create	21	3_1859532856364105968	7195
/reportHandle	19	1859530303675039894	709
/report/report	13	1_z12532a29vy	49
/report/report	21	3_1859525956589322476	483
/report/report	18	3_2_20260312A029V2	1
/report/deal	21	3_1859514748103557359	2
/report/auditBatchSubmit	13	1_s1259lzxsru	1
/report/auditBatchSubmit	21	3_1859515306922213612	9
/prediction/result	21	3_1859522037566931180	812
/match/share	13	1_z1258gfj4qk	7
/content/support	21	3_1859509246460166382	13
/content/support	13	1_t1258lirrvy	5
/content/support	16	2_20260313A041WZ	2
/comment/up	13	1_v1259ouyxkt	3
/comment/reply	13	1_z1256oy60nb	55
/comment/flow	21	3_1859521731339747403	126
/comment/flow	16	2_20220123A0077N	1
/comment/flow	13	1_y1255qsp65s	28
/comment/create	13	1_h1259xacbca	1
```