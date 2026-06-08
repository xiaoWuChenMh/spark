
# 数据探查sql
```sql
select uri
     ,length(get_json_object(kv,'$.mid')) AS index_id_length
     ,max(get_json_object(kv,'$.mid'))    AS index_id_max
     ,count(1)                                  As row_count
from pcg_atta_public_tdbank::t_atta_v1_0e600066510
where tdbank_imp_date >= 2026031300
  and tdbank_imp_date <=2026031323
  and code = 0
  and length(get_json_object(kv,'$.mid'))>0
group by uri,length(get_json_object(kv,'$.mid'))
order by uri desc;
```

#  数据情况
-- 说明：某个uri不存在，说明index_id_length的值是null或空字符串

``` text
uri	index_id_length	index_id_max	row_count
/videoDm/publish	13	160006:125113	1
/videoDm/publish	14	100360:2100233	11
/videoDm/publish	15	100002:20258138	102
/videoDm/publish	18	100000:10022501111	223
/trpc.sport_interact.live_guess_v2.LiveGuess/Join	18	100000:10022501111	54245
/topic/create	9	8:2562177	1
/topic/create	10	23:2572213	3
/topic/create	11	202:2597110	12
/topic/create	15	100002:20258315	36
/topic/create	14	160004:3904301	61
/topic/create	18	100001:12022500462	4768
/topic/create	12	731:31067351	76
/topic/create	16	100008:100106093	27
/redPacket/receive	9	100000:15	4
/redPacket/receive	18	100001:12022500455	1014
/prediction/result	14	160004:3904266	29
/prediction/result	12	100979:88898	36
/prediction/result	11	202:2597110	14
/prediction/result	18	100001:12022500455	733
/nprops/send	14	100360:2100234	19
/nprops/send	18	100001:12022500455	1154
/match/teamSupport	11	98:31057460	21
/match/teamSupport	10	6:31072701	95
/match/teamSupport	9	8:2562194	23
/match/teamSupport	14	160004:3904301	123
/match/teamSupport	13	1171:31077698	44
/match/teamSupport	17	100729:1767689302	6
/match/teamSupport	18	100001:12022500486	2068
/match/teamSupport	16	100008:100106181	114
/match/teamSupport	12	731:31067351	525
/match/share	15	100002:20258315	36
/match/share	17	100729:1767689286	4
/match/share	18	100020:11022500086	466
/match/share	10	6:31072700	4
/match/share	13	160005:122994	12
/match/share	16	100008:100105995	13
/match/share	12	731:31067351	125
/match/share	14	160004:3904266	687
/match/clockIn	18	100000:10022501111	23448
/match/clockIn	13	100800:831936	419
/match/attend	11	98:31057460	7
/match/attend	16	100008:100106191	170
/match/attend	18	100020:11012600001	6027
/match/attend	15	100002:20258318	2468
/match/attend	12	940:31060157	301
/match/attend	9	8:2562194	19
/match/attend	17	100729:1767689302	2
/match/attend	10	6:31072700	73
/match/attend	14	160004:3904301	142586
/match/attend	13	1171:31077697	11
/m/reviews/score	14	160004:3904265	182
/m/reviews/score	9	8:2562177	2
/m/reviews/score	18	100001:12022500455	46899
/m/reviews/score	15	100000:66873606	33
/m/reviews/score	16	100008:100108576	121
/dmComment/create	11	202:2597104	4
/dmComment/create	12	personallive	625
/dmComment/create	10	6:31072700	38
/dmComment/create	18	100001:12022500455	81812
/dmComment/create	13	1125:31072772	8
/dmComment/create	15	100002:20258292	30
/dmComment/create	14	160004:3904266	13586
```