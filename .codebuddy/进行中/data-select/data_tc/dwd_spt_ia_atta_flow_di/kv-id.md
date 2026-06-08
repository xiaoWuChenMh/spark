
# 数据探查sql
```sql
select uri
     ,length(get_json_object(kv,'$.id'))    AS index_id_length
     ,max(get_json_object(kv,'$.id'))       AS index_id_max
     ,count(1)                              As row_count
from pcg_atta_public_tdbank::t_atta_v1_0e600066510
where tdbank_imp_date >= 2026031300
  and tdbank_imp_date <=2026031323
  and code = 0
  and length(get_json_object(kv,'$.id'))>0
group by uri,length(get_json_object(kv,'$.id'))
order by uri desc;
```

#  数据情况
-- 说明：某个uri不存在，说明index_id_length的值是null或空字符串

``` text
uri	index_id_length	index_id_max	row_count
/user/unFollow	13	1_z1259chw8ke	23
/user/unFollow	21	3_1859423714595045607	1
/user/unFollow	11	v12588w8k75	2
/user/unFollow	6	285973	19
/user/follow	19	1859526714815676609	164
/user/follow	21	3_1859521837268992198	144
/user/follow	11	z410187mf7i	38
/user/follow	14	20260312A08MG5	14
/user/follow	16	2_20260312A08U1F	6
/user/follow	13	1_z4101t0z8y2	345
/user/follow	6	285973	369
/topic/vote	10	1061109567	59
/subject/detail	84	6buR5YWr77yM5Y+y5LiK56ys5YWt5qyh6buR5YWr6K+e55Sf77yBSkLlvoHmnI3lr4blsJTmsoPln7rvvIE=	27
/subject/detail	16	bmZs5ZCN5Lq65aCC	894
/subject/detail	96	5Zu9546L6K6w6ICF77ya6L+Z5Liq6LWb5a2j5Zu9546L55qE6L+b5pS75q+U5Lu75L2V5LiA5bm05YuH5aOr6YO95by677yB	1
/subject/detail	112	54Gr566t5oOo6YGtMTbliIbpgIbovazvvJrnlLPkuqzooqvmlofnj63lrozniIbvvIzkuYzluqbljaHkvZPns7vlho3pnLLoh7Tlkb3nn63mnb8=	13
/subject/detail	48	TkJB5bm05bqm5pyA5L2z6Zi15a6577ya6Km55bqT5LiJ6Zi1	646
/subject/detail	92	5qyn5Yag5oq9562+OuWIqeeJqea1pumprOernuexs+WFsOmAoOatu+S6oeS5i+e7hCHlt7Tpu47pga3pgYfmm7zln44=	3
/subject/detail	4	bG9s	64
/subject/detail	40	TkJB6ZmE5Yqg6LWb77ya5YuH5aOrdnPngbDnhoo=	500
/subject/detail	8	eGdhbWVz	465
/subject/detail	88	6Zi/5b635be057qm54uC56CNODPliIbvvIzmiZPnoLTnp5Hmr5TlvpfliIbnuqrlvZXvvIzkvY3lsYXnrKzkuow=	34
/subject/detail	68	TkJB6KOB5Yik5oql5ZGK77ya5Zub5qyh6ZSZ5ryP5Yik6aqR5aOr5LiJ5qyh5ZCD5LqP	2382
/subject/detail	108	5q2j5byP5a6j5biD5LiO5pyq5ama5aa75a6J5aicwrfnjpvkuL3kuprliIbmiYvvvIzlubbop6Pph4rkuobku5bnm67liY3nmoTmg4XlhrU=	50
/subject/detail	80	OeiusOS4ieWIhu+8jOS4nOWlkeWlhzUwK+WHhuS4ieWPjO+8jOa5luS6uui9u+WPluWFrOeJm++8gQ==	9
/subject/detail	56	TkJB5pyI5pyA5L2z77ya5Lqa5Y6G5bGx5aSn5biD5Lym5qOu5b2T6YCJ	5073
/subject/detail	12	dW5kZWZpbmVk	613
/subject/detail	36	VXpp6LWb5a2j6aaW6IOc5aSa5LiN5a655piT	384
/subject/detail	28	VXpp6L+Y6IO95omT5q+U6LWb5ZCX	1194
/subject/detail	104	5bCk5paH5Zu+5pav5q2j5byP5ZGK5Yir5qyn6LaF77yM5qyn6LaF6IGU5Yib5aeL5L+x5LmQ6YOo5LuF5Ymp55qH6ams5ZKM5be06JCo	2
/subject/detail	60	TkJB5pyI5pyA5L2z5paw56eA77ya5YWL5Yqq5L2p5bCU5ZOI54+A5b2T6YCJ	3302
/subject/detail	32	V1RB5Y2w56ys5a6J57u05bCU5pav56uZ	759
/subject/detail	64	6ams6b6Z6LWb5ZCO5o6l5Y+X6YeH6K6/6LCI6Zu35ZCJ5LuK5aSp55qE6KGo546w	1614
/subject/detail	52	TkZM5byA6LWb77yB5qmE5qaE55CD55qE5b+r5LmQ5Zue5p2l5ZWm	12142
/subject/detail	44	U0dBMjfliIYg6Zu36ZyG5pOS5YuH5aOrNei/nuiDnA==	381
/subject/detail	24	d3R06YeN5bqG5Yag5Yab6LWb	951
/subject/detail	76	6Zi/6YeM57qz5pav6K+05pyA5aW955qE5pe25Luj5pivMDDlubTku6PvvIzkuLrku4DkuYjvvJ8=	24
/subject/detail	100	5pel5aqS77ya4oCc6Zi/5bCU54m55aGU6LWb5ZCO5bCG5Lit5Zu95LiO5pel5pys5bm25YiX77yM5r+A5oCS55CD6L+377yB4oCd	2
/subject/detail	72	MjAyNOmAieengOS5kOmAj+aKveetvu+8mueKtuWFg+etvuWwhuiKseiQveiwgeWutu+8nw==	80
/subject/detail	20	bmJh6aaW56eA57K+5Y2O	1255
/sticker/collect	5	63149	1
/sticker/collect	6	957559	910
/reportHandle	19	1859530303675039894	709
/hometeam/select	1	9	38
/hometeam/select	2	29	89
/access/publish	11	z1259xreg87	742
```