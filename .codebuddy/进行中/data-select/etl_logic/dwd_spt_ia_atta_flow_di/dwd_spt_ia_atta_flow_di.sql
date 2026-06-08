
SELECT  ftime
     ,qimei36
     ,sports_uid
     ,app_version
     ,platform
     ,uri
     ,kv
     ,network
     ,content_id
     ,content_type
     ,program_id
     ,live_gid
     ,room_id
     ,comment_id
     ,content
     ,module_id
     ,is_support
     ,subject_name
     ,original_kv_list.manufacturer      AS manufacturer
     ,original_kv_list.device_model      AS device_model
     ,original_kv_list.os_version        AS os_version
     ,original_kv_list.hardware          AS hardware
     ,original_kv_list.android_id        AS android_id
     ,original_kv_list.omgid             AS omgid
     ,original_kv_list.guid              AS guid
     ,original_kv_list.idfa              AS idfa
     ,original_kv_list.idfv              AS idfv
     ,original_kv_list.title             AS title
     ,original_kv_list.mid               AS mid
     ,original_kv_list.team_id           AS team_id
     ,original_kv_list.player_id         AS player_id
     ,original_kv_list.team_side         AS team_side
     ,original_kv_list.vid               AS vid
     ,original_kv_list.tid               AS tid
     ,original_kv_list.news_id           AS news_id
     ,original_kv_list.parent_comment_id AS parent_comment_id
     ,original_kv_list.target_id         AS target_id
     ,original_kv_list.is_full_screen    AS is_full_screen
     ,original_kv_list.is_detail_page    AS is_detail_page
     ,original_kv_list.props_order_id    AS props_order_id
     ,original_kv_list.is_hit_end        AS is_hit_end
     ,original_kv_list.is_free           AS is_free
     ,original_kv_list.consume_diamond   AS consume_diamond
     ,original_kv_list.props_id          AS props_id
     ,original_kv_list.total_hits        AS total_hits
     ,original_kv_list.multi_gid         AS multi_gid
     ,original_kv_list.single_gid        AS single_gid
     ,original_kv_list.choice_id         AS choice_id
     ,original_kv_list.is_recommend      AS is_recommend
     ,original_kv_list.score             AS score
     ,original_kv_list.share_type        AS share_type
     ,original_kv_list.scene             AS scene
     ,original_kv_list.vote_id           AS vote_id
     ,original_kv_list.follow_uid        AS follow_uid
     ,original_kv_list.ia_from           AS ia_from
     ,original_kv_list.is_set            AS is_set
     ,original_kv_list.is_force          AS is_force
     ,original_kv_list.is_follow         AS is_follow
     ,original_kv_list.context           AS context
     ,original_kv_list.report_reason     AS report_reason
     ,original_kv_list.report_expire     AS report_expire
     ,original_kv_list.sid               AS sid
     ,kv_assistInfo_list.assist_type     AS assist_type
FROM
    (
        SELECT  ftime
             ,qimei36
             ,sports_uid
             ,app_version
             ,platform
             ,uri
             ,kv
             ,CASE WHEN lower(get_json_object(kv,'$.network')) = 'wifi' THEN 1
                   WHEN lower(get_json_object(kv,'$.network')) = '2g' THEN 2
                   WHEN lower(get_json_object(kv,'$.network')) = 'cmnet' THEN 2
                   WHEN lower(get_json_object(kv,'$.network')) = 'ctnet' THEN 2
                   WHEN lower(get_json_object(kv,'$.network')) = 'cmwap' THEN 2
                   WHEN lower(get_json_object(kv,'$.network')) = '3g' THEN 3
                   WHEN lower(get_json_object(kv,'$.network')) = '3gnet' THEN 3
                   WHEN lower(get_json_object(kv,'$.network')) = '3gwap' THEN 3
                   WHEN lower(get_json_object(kv,'$.network')) = '4g' THEN 4
                   WHEN lower(get_json_object(kv,'$.network')) = '5g' THEN 5  ELSE 0 END                                                      AS network
             ,CASE WHEN uri = '/report/deal' AND LENGTH(split(get_json_object(kv,'$.contentId'),'')[1]) > 0 THEN split(get_json_object(kv,'$.contentId'),'')[1]
                   WHEN uri = '/report/deal' THEN get_json_object(kv,'$.contentId')  ELSE split(get_json_object(kv,'$.contentId'),'')[1] END  AS content_id
             ,CASE WHEN uri = '/report/deal' AND LENGTH(split(get_json_object(kv,'$.contentId'),'')[1]) > 0 THEN split(get_json_object(kv,'$.contentId'),'')[0]
                   WHEN uri = '/report/deal' THEN ''  ELSE split(get_json_object(kv,'$.contentId'),'')[0] END                                 AS content_type
             ,CASE WHEN uri = '/dmComment/create' AND get_json_object(kv,'$.commentFrom') = '1' THEN get_json_object(get_json_object(kv,'$.liveInfo'),'$.programId')  ELSE get_json_object(kv,'$.program_id') END AS program_id
             ,CASE WHEN uri = '/dmComment/create' AND get_json_object(kv,'$.commentFrom') = '1' THEN get_json_object(get_json_object(kv,'$.liveInfo'),'$.liveGid')  ELSE get_json_object(kv,'$.liveGid') END AS live_gid
             ,CASE WHEN uri = '/dmComment/create' AND get_json_object(kv,'$.commentFrom') = '1' THEN get_json_object(get_json_object(kv,'$.liveInfo'),'$.anchorRoomId')  ELSE get_json_object(kv,'$.roomId') END AS room_id
             ,CASE WHEN length(get_json_object(kv,'$.commentId')) is not null THEN get_json_object(kv,'$.commentId')
                   WHEN length(get_json_object(kv,'$.comment.commentId')) is not null THEN get_json_object(kv,'$.comment.commentId')
                   WHEN uri IN ('/reportHandle','/moderator/sinkHandle','/moderator/reportedHandle') THEN get_json_object(kv,'$.id')
                   WHEN uri = '/moderator/focusHandle' AND get_json_object(kv,'$.subFrom') = 'focus' AND get_json_object(kv,'$.op') = 'pass' THEN get_json_object(kv,'$.id')  ELSE '' END AS comment_id
             ,CASE WHEN length(get_json_object(kv,'$.content')) > 1 THEN get_json_object(kv,'$.content')
                   WHEN length(get_json_object(kv,'$.text')) > 1 THEN get_json_object(kv,'$.text')  ELSE '0' END                              AS content
             ,CASE WHEN length(get_json_object(kv,'$.moduleId')) > 0 THEN get_json_object(kv,'$.moduleId')
                   WHEN length(get_json_object(kv,'$.reportInfo.moduleId')) > 0 THEN get_json_object(kv,'$.reportInfo.moduleId')  ELSE '' END AS module_id
             ,CASE WHEN get_json_object(kv,'$.support') IN ('true','1') THEN 1
                   WHEN get_json_object(kv,'$.isSupport') IN ('true','1') THEN 1  ELSE '0' END                                                AS is_support
             ,CASE WHEN LENGTH(get_json_object(get_json_object(kv,'$.reportInfo'),'$.subjectName')) > 0 THEN get_json_object(get_json_object(kv,'$.reportInfo'),'$.subjectName')
                   WHEN LENGTH(get_json_object(get_json_object(ia_data,'$.reportInfo'),'$.subjectName')) > 0 THEN get_json_object(get_json_object(ia_data,'$.reportInfo'),'$.subjectName')  ELSE '' END AS subject_name
             ,kv                                                                                                                              AS original_kv
             ,get_json_object(kv,'$.assistInfo')                                                                                              AS kv_assistInfo
        FROM
            (
                SELECT  ftime
                     ,qimei36
                     ,uid sports_uid
                     ,app_vid app_version
                     ,platform
                     ,uri
                     ,kv
                     ,data AS ia_data
                FROM pcg_atta_public_tdbank::t_atta_v1_0e600066510
                WHERE tdbank_imp_date = 2026031309
                  AND code = 0 --接口成功返回标志

            ) t
    ) t1
    lateral view json_tuple(original_kv,
       'manufacturer', 'deviceModel', 'osVersion', 'hardware', 'androidId', 'omgid', 'guid', 'idfa', 'idfv', 'title', 'mid', 'teamId', 'playerId', 'team_side', 'vid', 'tid', 'newsId', 'parentId', 'targetId', 'fullscreen',
       'inDetailPage', 'orderId', 'isHitEnd', 'noFree', 'consumeDiamond', 'propsId', 'totalHits', 'multiGid', 'singleGid', 'choiceId', 'isRecommend', 'score', 'shareType', 'scene', 'voteId', 'uid', 'from', 'isSet', 'isForce',
       'follow', 'context', 'reason', 'expire', 'sid'
    ) original_kv_list AS manufacturer, device_model, os_version, hardware, android_id, omgid, guid, idfa, idfv, title, mid, team_id, player_id, team_side, vid, tid, news_id, parent_comment_id, target_id, is_full_screen,
                          is_detail_page, props_order_id, is_hit_end, is_free, consume_diamond, props_id, total_hits, multi_gid, single_gid, choice_id, is_recommend, score, share_type, scene, vote_id, follow_uid, ia_from,
                          is_set, is_force, is_follow, context, report_reason, report_expire, sid
lateral view json_tuple(kv_assistInfo, 'assistType') kv_assistInfo_list AS assist_type