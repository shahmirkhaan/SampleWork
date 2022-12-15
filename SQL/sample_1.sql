SELECT
     * ,
     CURRENT_TIMESTAMP() AS bq_created_dt,
     CURRENT_TIMESTAMP() AS bq_updated_dt,
     "etl" AS bq_created_by,
     "etl" AS bq_updated_by
FROM
(
with job_changes_history as (
  select time_changed, table_id, `table`, `column`, old_value, new_value, job_id from `raw.job_changes_history` where
  `table` in ("ms2_interactive_packet", 'ms2_job_match') and `column` in ('packetStatusName', 'statusId', 'jobMatchStatusId')
  and new_value in ('62', '64', '65', '66', '61', '67', '68', '71', 'Sent', 'Accepted By Client', "2","3","4","5", 'Hard Interview Rejection', 'Soft Interview Rejection', "Trial Failed") and (old_value is NULL
  OR old_value in ('Draft', '1'))
),
jmh_interview as (
    select mjm.job_id,
    developer_id,
    time_changed as jmh_schedule_interview_time,
    row_number() over (partition by mjm.job_id, developer_id order by time_changed) as rn
    from job_changes_history jch
    inner join raw.ms2_job_match mjm
    on mjm.id = jch.table_id
    where `table` = 'ms2_job_match'
    and (`column` = 'jobMatchStatusId' OR column = 'jobMatchStatusName')
    and new_value in ('62', '64', '65', '66', '61', '67', '68', '71', 'Hard Interview Rejection', 'Soft Interview Rejection', "Trial Failed")

    and developer_id is not null
    and mjm.is_deleted = 0
),
rejection as (
    select jch.job_id,
    developer_id,
    time_changed as reject_date,
    case when new_value='64' then 'soft'
          when new_value = 'Hard Interview Rejection' then 'hard'
          when new_value = 'Soft Interview Rejection' then 'soft'
         else 'hard' end as reject_type,

    1 as is_interview_rejection,
    row_number() over (partition by jch.job_id, developer_id order by time_changed) as rn
    from job_changes_history jch
    left join raw.ms2_job_match mjm
    on mjm.id = jch.table_id
    where `table` = 'ms2_job_match'
    and (`column` = 'jobMatchStatusId' OR column = 'jobMatchStatusName')
    and new_value in ('64', '65', 'Hard Interview Rejection', 'Soft Interview Rejection')
),
packets as (
    SELECT *,
    row_number() over (partition by job_id, developer_id order by packet_sent_time) as rn
    from (
        (
        SELECT histories.job_id AS job_id,
        histories.table_id AS packet_id,
        histories.time_changed AS packet_sent_time ,
        '3.old_packet' as packet_type,
        mjpd.developer_id as developer_id,
        timestamp(null) as thumbs_up_date,
        timestamp(null) as feedback_date
        FROM job_changes_history histories
        left join raw.ms2_job_packet mjp
            ON histories.table_id = mjp.id
        left join raw.ms2_job_packet_developers mjpd
            on mjpd.packet_id = mjp.id
            WHERE `column` = 'packetStatusName'
            AND new_value in ('Sent',
                                'Accepted By Client')
            AND (old_value is NULL
                OR old_value = 'Draft')
            AND mjp.is_deleted = 0
        )
        UNION all -- For new Packets (until the history is enabled for them)
        (
            SELECT mip.job_id AS job_id,
            mip.id AS packet_id,
            COALESCE(histories.time_changed,timestamp(packet_creation_date)) AS packet_sent_time,
            '2.new_packet' as packet_type,
            mipd.developer_id as developer_id,
            case when customer_feedback = 1 then timestamp(customer_feedback_last_updated) end as thumbs_up_date,
            case when customer_feedback is not null then timestamp(customer_feedback_last_updated) end as feedback_date
            FROM (
                select id,
                job_id,
                created_date as packet_creation_date
                from raw.ms2_interactive_packet
                where status_id in (2,3,4,5)
            ) mip
            left join (
              SELECT *
              FROM job_changes_history
              WHERE `table` = "ms2_interactive_packet"
              AND `column` = 'statusId'
              AND new_value in ("2","3","4","5")
              AND (old_value is NULL OR old_value = "1")
            ) histories
                ON histories.table_id = mip.id
            left join raw.ms2_interactive_packet_developers mipd
                on mipd.interactive_packet_id = mip.id
        )
    )
),
mjm as (
    select job_id,
    developer_id,
    created_ts,
    source,
    confidence,
    row_number() over (partition by job_id, developer_id order by (case when source='self-serve' then 1 else 0 end) desc, created_ts) as rn
    from raw.ms2_job_match mjm
    where mjm.is_deleted=0
),
sf_pairs_pre as(
    select job_id,
    dev_id,
    min(lower(account)) as account_lower,
    min(chosen_date) as chosen_date,
    min(signed_date) as signed_date,
    min(trial_date) as trial_date,
    min(start_date) as start_date,
    min(complete_date) as complete_date,
    min(lost_date) as lost_date,
    max(is_complete) as is_complete,
    max(starts) as is_start,
    max(trials) as is_trial,
    max(resource_chosen) as is_rc,
    max(is_lost) as is_lost,
    max(is_fire) as is_fire,
    max(is_trial_fail) as is_trial_fail,
    max(engagement_days) as engagement_days,
    max(lost_reason) as lost_reason,
    max(aborted_reason) as aborted_reason,
    max(aborted_reason_elaboration) as aborted_reason_elaboration
    from curated.opportunity_value
    where job_id is not null and dev_id is not null
    group by 1,2
),
self_serve_interview_requests as (
    select
        ssir.id,
        job_id,
        developer_id,
        j.company as company,
        lower(j.company) as company_lower,
        ssir.created_date as ssir_date,
        j.is_deleted,
        row_number() over (partition by job_id, developer_id order by ssir.created_date) as rn
    from raw.self_serve_interview_requests ssir
    join raw.ms2_job j
        on ssir.job_id = j.id
    where j.company not like 'Turing'
        and j.company not like '%Turing Generic%'
        and ssir.is_deleted = 0
),
interviews as (
    select coalesce(ji.job_id, ssir.job_id) as job_id,
    coalesce(ji.developer_id, ssir.developer_id) as developer_id,
    least(coalesce(jmh_schedule_interview_time, ssir_date), coalesce(ssir_date, jmh_schedule_interview_time)) as si_date,
    case when ssir.job_id is not null then 1 else 0 end as is_selfserve_interview
    from (select * from jmh_interview where rn=1) ji
    full outer join (
        select job_id,
        developer_id,
        ssir_date,
        from self_serve_interview_requests
        where is_deleted=0
        and rn=1
        and job_id is not null and developer_id is not null
    ) ssir
    on ji.job_id = ssir.job_id
    and ji.developer_id = ssir.developer_id
),
approx_ssir as (
    select sfo.job_id,
    sfo.dev_id,
    ssir.id as self_serve_interview_request_id,
    ssir_date,
    from sf_pairs_pre sfo
    inner join self_serve_interview_requests ssir
    on sfo.account_lower = ssir.company_lower
    and sfo.dev_id = ssir.developer_id
    where chosen_date>ssir_date
    and timestamp_diff(chosen_date, ssir_date, day)<=180
    and sfo.job_id != ssir.job_id
),
sf_pairs as (
    select spp.*,
    assir.self_serve_interview_request_id,
    ssir_date,
    from sf_pairs_pre spp
    left join approx_ssir assir
    on spp.job_id = assir.job_id
    and spp.dev_id = assir.dev_id
),
pairs_pre as (
    select coalesce(mjm.job_id, sf_pairs.job_id, i.job_id) as job_id,
    coalesce(mjm.developer_id, sf_pairs.dev_id, i.developer_id) as developer_id,
    confidence,
    is_selfserve_interview,
    case
        when source='self-serve'
        or is_selfserve_interview=1
        or ssir_date is not null
        then '1.self-serve' else packet_type end as serve_type,
    packet_id,
    created_ts as mjm_create_ts,
    packet_sent_time,
    feedback_date,
    coalesce(
        least(coalesce(si_date, thumbs_up_date), coalesce(thumbs_up_date, si_date)),
        ssir_date,
        chosen_date
    ) as si_date,
    case when chosen_date is null then reject_date end as reject_date,
    chosen_date,
    signed_date,
    trial_date,
    start_date,
    complete_date,
    lost_date,
    coalesce(case when chosen_date is null then is_interview_rejection end, 0) as is_interview_rejection,
    coalesce(is_complete, 0) as is_complete,
    coalesce(is_start, 0) as is_start,
    coalesce(is_trial, 0) as is_trial,
    coalesce(is_rc, 0) as is_rc,
    coalesce(is_lost, 0) as is_lost,
    coalesce(is_fire, 0) as is_fire,
    coalesce(is_trial_fail, 0) as is_trial_fail,
    coalesce(engagement_days, 0) as engagement_days,
    case when chosen_date is null then reject_type end as reject_type,
    lost_reason,
    aborted_reason,
    aborted_reason_elaboration,
    self_serve_interview_request_id as ssir_id
    from (select * from mjm where rn=1) mjm
    full outer join sf_pairs
    on mjm.job_id = sf_pairs.job_id
    and mjm.developer_id = sf_pairs.dev_id
    full outer join interviews i
    on coalesce(mjm.job_id, sf_pairs.job_id) = i.job_id
    and coalesce(mjm.developer_id, sf_pairs.dev_id) = i.developer_id
    left join (select * from packets where rn=1) packets
    on coalesce(mjm.job_id, sf_pairs.job_id) = packets.job_id
    and coalesce(mjm.developer_id, sf_pairs.dev_id) = packets.developer_id
    left join (select * from rejection where rn=1) rejection
    on coalesce(mjm.job_id, sf_pairs.job_id) = rejection.job_id
    and coalesce(mjm.developer_id, sf_pairs.dev_id) = rejection.developer_id
)
select job_id,
developer_id,
confidence,
mjm_create_ts,
serve_type,
packet_id,
packet_sent_time,
is_selfserve_interview,
coalesce(feedback_date, si_date) as feedback_date,
coalesce(case when serve_type='1.self-serve' then coalesce(least(mjm_create_ts, si_date), least(si_date, mjm_create_ts)) else packet_sent_time end, si_date) as serve_date,
si_date,
reject_date,
chosen_date,
signed_date,
trial_date,
start_date,
complete_date,
lost_date,
case when coalesce(case when serve_type='1.self-serve' then coalesce(least(mjm_create_ts, si_date), mjm_create_ts) else packet_sent_time end, si_date) is not null then 1 else 0 end as is_served,
case when si_date is not null then 1 else 0 end as is_si,
is_interview_rejection,
is_rc,
is_trial,
is_start,
is_complete,
is_lost,
is_fire,
is_trial_fail,
case when serve_type='1.self-serve' and si_date is not null then 1 else 0 end as is_si_selfserve,
case when serve_type='1.self-serve' and chosen_date is not null then 1 else 0 end as is_rc_selfserve,
engagement_days,
reject_type,
lost_reason,
aborted_reason,
aborted_reason_elaboration,
ssir_id
from pairs_pre
) PP;
    