
SELECT  t.imp_date
     ,t.ftime
     ,t.qimei36
     ,t.omgid
     ,t.guid
     ,t.sports_uid
     ,t.app_version
     ,t.platform
     ,t.dev_type
     ,t.network_type
     ,t.dev_brand
     ,t.dev_model
     ,split(t.ia_content_cate,'#')[0]                               AS ia_type
     ,split(t.ia_content_cate,'#')[1]                               AS ia_cate
     ,split(t.ia_content_cate,'#')[2]                               AS ia_sub_cate
     ,t.uri
     ,t.content_id
     ,IF(length(t.content_type) > 0,t.content_type,t1.content_type) AS content_type
     ,t1.cate_id
     ,t1.cate_name
     ,t1.sub_cate_id
     ,t1.sub_cate_name
     ,t.match_id
     ,t.vid
     ,t.tid
     ,t.news_id
     ,t.comment_id
     ,t.content
     ,t.parent_comment_id
-- 2023041318时修改上游脚本，圈子id默认值改为空字符串
     ,CASE WHEN uri = '/commentAdmin/delete' AND t.module_id = '0' THEN t.module_id
           WHEN (t.module_id = '0' or t.module_id = '') AND LENGTH(t1.module_id) > 0 THEN t1.module_id
           WHEN t.module_id = '0' THEN ''  ELSE t.module_id END     AS module_id
     ,t.subject_name
     ,t.is_support
     ,t.in_detail_page
     ,t.kv
     ,get_json_object(t.kv,'$.isRecommend')                         AS is_recommend
     ,datahub_url_decode(t.context,'gbk')                           AS context
     ,room_id
     ,content_title
     ,content_from
     ,ecol_type
     ,is_pay
     ,video_length
     ,scene
     ,comment_column
     ,create_time
     ,IF(LENGTH(num) > 0,num,1)                                     AS in_interact_cnt
     ,NVL(report_reason,'')                                         AS report_reason
     ,NVL(report_expire,'')                                         AS report_expire
     ,NVL(content_uid,'')                                           AS content_uid
     ,NVL(comment_uid,'')                                           AS comment_uid
     ,NVL(scene_from,'')                                            AS scene_from
     ,NVL(scene_from_tab,'')                                        AS scene_from_tab
     ,NVL(ia_from,'')                                               AS ia_from
     ,t.second_comment_id                                           AS second_comment_id
FROM
    (
        SELECT  imp_date
             ,ftime
             ,qimei36
             ,omgid
             ,guid
             ,sports_uid
             ,app_version
             ,platform
             ,dev_type
             ,network_type
             ,dev_brand
             ,dev_model
             ,uri
             ,IF(LENGTH(A1.content_id) > 0,A1.content_id,A2.content_id)  AS content_id
             ,content_type
             ,match_id
             ,vid
             ,tid
             ,news_id
             ,A1.comment_id                                              AS comment_id
             ,content
             ,parent_comment_id
             ,module_id
             ,subject_name
             ,is_support
             ,in_detail_page
             ,kv
             ,context
             ,ia_content_cate
             ,room_id
             ,scene
             ,comment_column
             ,create_time
             ,num
             ,if(length(report_reason) > 0,report_reason,report_reasons) AS report_reason
             ,report_expire
             ,comment_uid
             ,scene_from
             ,scene_from_tab
             ,ia_from
             ,second_comment_id
        FROM
            (
                SELECT  imp_date
                     ,ftime
                     ,qimei36
                     ,omgid
                     ,guid
                     ,sports_uid
                     ,app_version
                     ,platform
                     ,CASE WHEN lower(platform) = 'android' THEN '2'
                           WHEN lower(platform) = 'ipad' THEN '3'
                           WHEN lower(platform) = 'iphone' THEN '4'  ELSE 0 END                                                                               AS dev_type
                     ,network network_type
                     ,manufacturer dev_brand
                     ,device_model dev_model
                     ,uri
                     ,CASE WHEN uri = '/match/share' AND get_json_object(kv,'$.contentType') = '1' THEN mid
                           WHEN uri = '/match/share' AND get_json_object(kv,'$.contentType') = '2' THEN news_id
                           WHEN uri = '/match/share' AND get_json_object(kv,'$.contentType') = '8' THEN vid
                           WHEN uri = '/match/share' AND get_json_object(kv,'$.contentType') = '9' THEN tid
                           WHEN uri = '/match/share' AND get_json_object(kv,'$.contentType') = '18' THEN sid
                           WHEN uri = '/moderator/focusHandle' THEN split(get_json_object(kv,'$.id'),'')[1]
                           WHEN vid is not null THEN vid
                           WHEN tid is not null THEN tid
                           WHEN news_id is not null THEN news_id
                           WHEN mid is not null THEN mid  ELSE content_id END                                                                                 AS content_id
                     ,CASE WHEN uri = '/match/share' AND get_json_object(kv,'$.contentType') = '1' THEN 4
                           WHEN uri = '/match/share' AND get_json_object(kv,'$.contentType') = '2' THEN 2
                           WHEN uri = '/match/share' AND get_json_object(kv,'$.contentType') = '8' THEN 1
                           WHEN uri = '/match/share' AND get_json_object(kv,'$.contentType') = '9' THEN 3
                           WHEN uri = '/match/share' AND get_json_object(kv,'$.contentType') = '18' THEN 18
                           WHEN uri = '/moderator/focusHandle' THEN split(get_json_object(kv,'$.id'),'')[0]
                           WHEN uri = '/reportHandle' THEN NULL
                           WHEN vid is not null THEN 1
                           WHEN tid is not null THEN 3
                           WHEN news_id is not null THEN 2
                           WHEN mid is not null THEN 4  ELSE content_type END                                                                                 AS content_type
                     ,mid                                                                                                                                     AS match_id
                     ,vid
                     ,tid
                     ,news_id
                     ,CASE WHEN LENGTH(comment_id) > 3 THEN comment_id
                           WHEN uri IN ('/moderator/sinkHandle','/moderator/reportedHandle') THEN get_json_object(kv,'$.id')
                           WHEN uri = '/reportHandle' AND get_json_object(kv,'$.type') IN ('reply','news_reply') THEN get_json_object(kv,'$.id')  ELSE '' END AS comment_id
                     ,content
                     ,parent_comment_id
                     ,module_id
                     ,subject_name
                     ,is_support
                     ,get_json_object(kv,'$.inDetailPage') in_detail_page
                     ,kv
                     ,context
                     ,CASE WHEN uri = '/playerComment/support' THEN 'content#like#player_rate'
                           WHEN uri = '/news/thumbUp' THEN 'content#like#content_news'
                           WHEN uri = '/content/support' THEN 'content#like#content_news'
                           WHEN uri = '/topic/support' THEN 'content#like#content_community'
                           WHEN uri = '/video/thumbUp' THEN 'content#like#content_video'
                           WHEN uri = '/v2/content/support' AND content_type = '1' AND is_support = 1 THEN 'content#like#content_video'
                           WHEN uri = '/v2/content/support' AND content_type = '2' AND is_support = 1 THEN 'content#like#content_news'
                           WHEN uri = '/v2/content/support' AND content_type = '3' AND is_support = 1 THEN 'content#like#content_community'
                           WHEN uri = '/v2/content/support' AND content_type = '1' AND is_support = 0 THEN 'content#unlike#content_video'
                           WHEN uri = '/v2/content/support' AND content_type = '2' AND is_support = 0 THEN 'content#unlike#content_news'
                           WHEN uri = '/v2/content/support' AND content_type = '3' AND is_support = 0 THEN 'content#unlike#content_community'
                           WHEN uri = '/reply/support' THEN 'content#like#comment_community'
                           WHEN uri = '/comment/up' AND content_type = '1' THEN 'content#like#comment_video'
                           WHEN uri = '/comment/up' AND content_type = '2' THEN 'content#like#comment_news'
                           WHEN uri = '/comment/up' AND content_type = '3' THEN 'content#like#comment_community'
                           WHEN uri = '/comment/up' AND content_type = '4' THEN 'content#like#comment_match'
                           WHEN uri = '/comment/up' THEN 'content#like'
                           WHEN uri = '/user/timeline_support' AND content_type = '1' THEN 'content#like#comment_video'
                           WHEN uri = '/user/timeline_support' AND content_type = '2' THEN 'content#like#comment_news'
                           WHEN uri = '/user/timeline_support' AND content_type = '3' THEN 'content#like#comment_community'
                           WHEN uri = '/user/timeline_support' THEN 'content#like'
                           WHEN uri = '/v2/comment/support' AND content_type = '1' AND is_support = 1 THEN 'content#like#comment_video'
                           WHEN uri = '/v2/comment/support' AND content_type = '2' AND is_support = 1 THEN 'content#like#comment_news'
                           WHEN uri = '/v2/comment/support' AND content_type = '3' AND is_support = 1 THEN 'content#like#comment_community'
                           WHEN uri = '/v2/comment/support' AND content_type = '4' AND is_support = 1 AND get_json_object(context,'$.playerId') is not null THEN 'content#like#player_rate'
                           WHEN uri = '/v2/comment/support' AND content_type = '4' AND is_support = 1 THEN 'content#like#comment_match'
                           WHEN uri = '/v2/comment/support' AND content_type = '1' AND is_support = 0 THEN 'content#unlike#comment_video'
                           WHEN uri = '/v2/comment/support' AND content_type = '2' AND is_support = 0 THEN 'content#unlike#comment_news'
                           WHEN uri = '/v2/comment/support' AND content_type = '3' AND is_support = 0 THEN 'content#unlike#comment_community'
                           WHEN uri = '/v2/comment/support' AND content_type = '4' AND is_support = 0 AND get_json_object(context,'$.playerId') is not null THEN 'content#unlike#player_rate'
                           WHEN uri = '/v2/comment/support' AND content_type = '4' AND is_support = 0 THEN 'content#unlike#comment_match'
                           WHEN uri = '/playerComment/create' THEN 'content#comment#player_rate'
                           WHEN uri = '/comment/create' AND content_type = '1' THEN 'content#comment#content_video'
                           WHEN uri = '/comment/create' AND content_type = '2' THEN 'content#comment#content_news'
                           WHEN uri = '/comment/create' AND content_type = '4' THEN 'content#comment#content_match'
                           WHEN uri = '/comment/create' THEN 'content#comment'
                           WHEN uri = '/comment/reply' AND content_type = '2' THEN 'content#comment#comment_news'
                           WHEN uri = '/comment/reply' AND content_type = '1' THEN 'content#comment#comment_video'
                           WHEN uri = '/comment/reply' THEN 'content#comment'
                           WHEN uri = '/reply/create' AND parent_comment_id = '0' THEN 'content#comment#content_community'
                           WHEN uri = '/reply/create' AND parent_comment_id <> '0' THEN 'content#comment#comment_community'
                           WHEN uri = '/v2/comment/publish' AND content_type = '1' AND comment_id is not null AND parent_comment_id is null THEN 'content#comment#content_video' --内容互动#评论#视频评论
                           WHEN uri = '/v2/comment/publish' AND content_type = '1' AND comment_id is not null AND parent_comment_id is not null THEN 'content#comment#comment_video' --内容互动#评论#视频评论回复
                           WHEN uri = '/v2/comment/publish' AND content_type = '2' AND comment_id is not null AND parent_comment_id is null THEN 'content#comment#content_news' --内容互动#评论#资讯评论
                           WHEN uri = '/v2/comment/publish' AND content_type = '2' AND comment_id is not null AND parent_comment_id is not null THEN 'content#comment#comment_news' --内容互动#评论#资讯评论回复
                           WHEN uri = '/v2/comment/publish' AND content_type = '3' AND comment_id is not null AND parent_comment_id is null THEN 'content#comment#content_community' --内容互动#评论#资讯评论
                           WHEN uri = '/v2/comment/publish' AND content_type = '3' AND comment_id is not null AND parent_comment_id is not null THEN 'content#comment#comment_community' --内容互动#评论#资讯评论回复
                           WHEN uri = '/v2/comment/publish' AND content_type = '4' AND comment_id is not null AND context is not null THEN 'content#comment#player_rate' --内容互动#评论#点评球员
                           WHEN uri = '/v2/comment/publish' AND content_type = '4' AND comment_id is not null AND parent_comment_id is null THEN 'content#comment#content_community' --内容互动#评论#资讯评论
                           WHEN uri = '/v2/comment/publish' AND content_type = '4' AND comment_id is not null AND parent_comment_id is not null THEN 'content#comment#comment_community' --内容互动#评论#资讯评论回复
                           WHEN uri = '/videoDm/publish' THEN 'content#comment#video_barrage' --内容互动#评论#视频弹幕
                           WHEN uri = '/match/share' AND get_json_object(kv,'$.contentType') = '1' THEN 'content#share#content_match' --内容互动#分享#比赛分享
                           WHEN uri = '/match/share' AND get_json_object(kv,'$.contentType') = '2' THEN 'content#share#content_news' --内容互动#分享#资讯分享
                           WHEN uri = '/match/share' AND get_json_object(kv,'$.contentType') = '8' THEN 'content#share#content_video' --内容互动#分享#视频分享
                           WHEN uri = '/match/share' AND get_json_object(kv,'$.contentType') = '9' THEN 'content#share#content_community' --内容互动#分享#帖子分享
                           WHEN uri = '/match/share' AND get_json_object(kv,'$.contentType') = '10' THEN 'content#share#content_circle' --内容互动#分享#圈子分享
                           WHEN uri = '/match/share' AND get_json_object(kv,'$.contentType') = '15' THEN 'content#share#comment' --内容互动#分享#评论分享
                           WHEN uri = '/match/share' AND get_json_object(kv,'$.contentType') = '16' THEN 'content#share#content_tag' --内容互动#分享#话题分享
                           WHEN uri = '/match/share' AND get_json_object(kv,'$.contentType') = '18' THEN 'content#share#special_subject' --内容互动#分享#专题分享
                           WHEN uri = '/match/share' THEN 'content#share#' --内容互动#分享#
                           WHEN uri = '/internal/share' AND content_type = '1' THEN 'content#in_share#content_video' -- 内容互动#分享#视频分享
                           WHEN uri = '/internal/share' AND content_type = '2' THEN 'content#in_share#content_news' --内容互动#分享#资讯分享
                           WHEN uri = '/internal/share' AND content_type = '3' THEN 'content#in_share#content_community' --内容互动#分享#帖子分享
                           WHEN uri = '/user/follow' THEN 'content#follow#user' --关注#关注用户
                           WHEN uri = '/user/unFollow' THEN 'content#unfollow#user' --关注#取关用户
                           WHEN uri = '/topic/vote' THEN 'content#vote#vote'
                           WHEN uri = '/v2/comment/delete' THEN'content#del_comment#comment'
                           WHEN uri = '/topic/create' THEN 'content#post#content_community'
                           WHEN uri = '/access/publish' THEN 'content#post#content_video'
                           WHEN uri = '/comment/flow' AND get_json_object(kv,'$.commentType') = 'natural' AND get_json_object(kv,'$.op') = 'add' THEN 'content#hot_comment#natural_hot_comment'
                           WHEN uri = '/comment/flow' AND get_json_object(kv,'$.commentType') = 'manual' AND get_json_object(kv,'$.op') = 'add' THEN 'content#hot_comment#manual_hot_comment'
                           WHEN uri = '/v2/comment/setElite' AND get_json_object(kv,'$.isSet') = 'true' THEN 'content#hot_comment#moderator_hot_comment'
                           WHEN uri = '/comment/flow' AND get_json_object(kv,'$.commentType') = 'natural' AND get_json_object(kv,'$.op') = 'cancel' THEN 'content#unset_hot_comment#natural_hot_comment'
                           WHEN uri = '/comment/flow' AND get_json_object(kv,'$.commentType') = 'manual' AND get_json_object(kv,'$.op') = 'cancel' THEN 'content#unset_hot_comment#manual_hot_comment'
                           WHEN uri = '/v2/comment/setElite' AND get_json_object(kv,'$.isSet') = 'false' THEN 'content#unset_hot_comment#moderator_hot_comment'
                           WHEN uri = '/v2/comment/delete' THEN 'content#del_comment#comment_content'
                           WHEN uri = '/moderator/focusHandle' AND get_json_object(kv,'$.op') = 'claim' AND split(get_json_object(kv,'$.id'),'')[0] = '2' THEN 'content#moderator_claim#content_news'
                           WHEN uri = '/moderator/focusHandle' AND get_json_object(kv,'$.op') = 'claim' AND split(get_json_object(kv,'$.id'),'')[0] = '3' THEN 'content#moderator_claim#content_community'
                           WHEN uri = '/moderator/focusHandle' AND get_json_object(kv,'$.op') = 'claim' AND split(get_json_object(kv,'$.id'),'_')[0] = '1' THEN 'content#moderator_claim#content_video'
                           WHEN uri = '/notifyModerator' THEN 'content#notify#notify_moderator'
                           WHEN uri = '/topic/updateModule' THEN 'content#post_manage#distract_community'
                           WHEN uri = '/topic/setType' AND get_json_object(kv,'$.type') = 'top' THEN 'content#post_manage#top_community'
                           WHEN uri = '/topic/setType' AND get_json_object(kv,'$.type') = 'elite' THEN 'content#post_manage#elite_community'
                           WHEN uri = '/report/deal' AND get_json_object(kv,'$.type') = 'topic' AND nvl(url_decode(get_json_object(kv,'$.expire'),'utf-8'),'') NOT IN ('不禁言','') THEN 'content#report_handle#del_silence_community'
                           WHEN uri = '/report/deal' AND get_json_object(kv,'$.type') = 'topic' AND nvl(url_decode(get_json_object(kv,'$.expire'),'utf-8'),'') IN ('不禁言','') THEN 'content#report_handle#del_community'
                           WHEN uri = '/report/deal' AND get_json_object(kv,'$.type') = 'reply' AND nvl(url_decode(get_json_object(kv,'$.expire'),'utf-8'),'') NOT IN ('不禁言','') AND get_json_object(kv,'$.op') = 'delete' THEN 'content#report_handle#del_silence_comment'
                           WHEN uri = '/report/deal' AND get_json_object(kv,'$.type') = 'reply' AND nvl(url_decode(get_json_object(kv,'$.expire'),'utf-8'),'') IN ('不禁言','') AND get_json_object(kv,'$.op') = 'delete' THEN 'content#report_handle#del_comment'
                           WHEN uri = '/reportHandle' AND get_json_object(kv,'$.type') IN ('reply','news_reply') THEN 'content#report_handle#community_reply'
                           WHEN uri = '/moderator/reportedHandle' AND get_json_object(kv,'$.op') = 'ignore' THEN 'content#report_handle#ignore'
                           WHEN uri = '/moderator/reportedHandle' AND get_json_object(kv,'$.op') = 'visible' THEN 'content#report_handle#creator_visible'
                           WHEN uri = '/report/auditBatchSubmit' AND get_json_object(kv,'$.type') = '1' AND get_json_object(kv,'$.rtx') = 'beyondReport' THEN 'content#report_handle#systemt_nodistri_conten'
                           WHEN uri = '/report/auditBatchSubmit' AND get_json_object(kv,'$.type') = '1' AND get_json_object(kv,'$.rtx') != 'beyondReport' THEN 'content#report_handle#manual_nodistri_content'
                           WHEN uri = '/report/auditBatchSubmit' AND get_json_object(kv,'$.type') = '2' THEN 'content#report_handle#admin_del_content'
                           WHEN uri = '/report/auditBatchSubmit' AND get_json_object(kv,'$.type') = '201' THEN 'content#report_handle#admin_del_silence_content'
                           WHEN uri = '/report/auditBatchSubmit' AND get_json_object(kv,'$.type') = '3' THEN 'content#report_handle#ignore_content'
                           WHEN uri = '/commentAdmin/delete' AND LENGTH(get_json_object(kv,'$.moduleId')) = 0 THEN 'content#report_handle#admin_del_comment '
                           WHEN uri = '/commentAdmin/delete' AND LENGTH(get_json_object(kv,'$.moduleId')) != 0 THEN 'content#report_handle#admin_del_silence_comment '
                           WHEN uri = '/moderator/sinkHandle' AND get_json_object(kv,'$.op') = 'visible' THEN 'content#violation_handle#moderator_visible_comment'
                           WHEN uri = '/moderator/sinkHandle' AND get_json_object(kv,'$.op') = 'rise' THEN 'content#violation_handle#moderator_revisible_comment'
                           WHEN uri = '/moderator/sinkHandle' AND get_json_object(kv,'$.op') = 'sink' THEN 'content#violation_handle#moderator_sink_comment'
                           WHEN uri = '/report/report' AND get_json_object(kv,'$.type') = 'user' THEN 'content#report#user'
                           WHEN uri = '/report/report' AND get_json_object(kv,'$.type') = 'video' THEN 'content#report#content_video'
                           WHEN uri = '/report/report' AND get_json_object(kv,'$.type') = 'news' THEN 'content#report#content_news'
                           WHEN uri = '/report/report' AND get_json_object(kv,'$.type') = 'topic' THEN 'content#report#content_community'
                           WHEN uri = '/report/report' AND get_json_object(kv,'$.type') IN ('reply','news_reply') THEN 'content#report#comment'
                           WHEN uri = '/v2/comment/fakeSupport' AND get_json_object(kv,'$.isSupport') = 'true' THEN 'content#human_like#human_add'
                           WHEN uri = '/moderator/focusHandle' AND get_json_object(kv,'$.subFrom') = 'focus' AND get_json_object(kv,'$.op') = 'pass' THEN 'content#content_review#fcmr_pass'
                           WHEN uri = '/moderator/focusHandle' AND nvl(get_json_object(kv,'$.subFrom'),'') != 'focus' AND get_json_object(kv,'$.op') = 'pass' THEN 'content#content_review#other_cmr_pass'
		             WHEN uri = '/report/deal' AND get_json_object(kv,'$.op') = 'visible' AND get_json_object(kv,'$.subFrom') = 'focus' THEN 'content#content_review#fcmr_private'
		             WHEN uri = '/report/deal' AND get_json_object(kv,'$.op') = 'visible' AND nvl(get_json_object(kv,'$.subFrom'),'') != 'focus' THEN 'content#content_review#other_private'  ELSE '' END AS ia_content_cate
		       ,room_id
		       ,scene
		       ,get_json_object(get_json_object(kv,'$.context'),'$.column')                                                                             AS comment_column
		       ,get_json_object(kv,'$.createTime')                                                                                                      AS create_time
		       ,cast(get_json_object(kv,'$.num') AS bigint)                                                                                             AS num
		       ,url_decode(get_json_object(kv,'$.reason'),'utf-8')                                                                                      AS report_reason
		       ,url_decode(get_json_object(kv,'$.reasons'),'utf-8')                                                                                     AS report_reasons
		       ,url_decode(get_json_object(kv,'$.expire'),'utf-8')                                                                                      AS report_expire
		       ,get_json_object(kv,'$.sceneFrom')                                                                                                       AS scene_from
		       ,url_decode(get_json_object(kv,'$.tab'),'utf-8')                                                                                         AS scene_from_tab
		       ,ia_from
                FROM dwd_spt_ia_atta_flow_di
            ) A1
                LEFT JOIN
            (
                SELECT  comment_id
                     ,MAX(content_id) AS content_id
                     ,MAX(sports_uid) AS comment_uid
                     ,MAX(second_id)  AS second_comment_id
                FROM pcg_sports_dim::dim_spt_ecol_comment_info_hf
                WHERE imp_hour = 2026031223
                  AND LENGTH(comment_id) > 0
                  AND 20260312 > 20260208
                GROUP BY  comment_id
                UNION ALL
                SELECT  comment_id
                        ,MAX(content_id) AS content_id
                        ,MAX(sports_uid) AS comment_uid
                        ,MAX(second_id)  AS second_comment_id
                FROM pcg_sports_dim::dim_spt_ecol_comment_info_mf
                WHERE imp_month = 20260201
                  AND LENGTH(comment_id) > 0
                  AND 20260312 <= 20260208
                GROUP BY  comment_id
            ) A2
            ON A1.comment_id = A2.comment_id
    ) t
        LEFT JOIN
    (
        SELECT  content_id
             ,cate_id
             ,cate_cn                                       AS cate_name
             ,sub_cate_id
             ,sub_cate_cn                                   AS sub_cate_name
             ,title                                         AS content_title
             ,plat_form                                     AS content_from
             ,ecol_type
             ,video_is_pay                                  AS is_pay
             ,video_length
             ,CASE WHEN content_type = 1 THEN 2
                   WHEN content_type = 2 THEN 3  ELSE 1 END AS content_type
             ,module_id
             ,sports_uid                                    AS content_uid
        FROM pcg_sports_dim::dim_spt_ecol_content_merge_info_hf
        WHERE imp_hour = 2026031223
          AND LENGTH(content_id) > 0 -- AND ( (content_type IN (2, 3)) OR (content_type = 1 AND cate_id > 0) )

        UNION ALL
        SELECT  mid   AS content_id
                ,cate_id
                ,cate_name
                ,sub_cate_id
                ,sub_cate_name
                ,title AS content_title
                ,null  AS content_from
                ,'3'   AS ecol_type
                ,is_pay
                ,0.0   AS video_length
                ,4     AS content_type
                ,''    AS module_id
                ,''    AS content_uid
        FROM pcg_sports_dim::dim_spt_live_match_info_hf --比赛
        WHERE imp_hour = 2026031223
          AND LENGTH(mid) > 0
    ) t1
    ON t.content_id = t1.content_id
WHERE split(t.ia_content_cate, '#')[1] IS NOT NULL