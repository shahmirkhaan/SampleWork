WITH opportunities AS(
    SELECT 
      opportunity.id AS opportunityid, 
      CASE WHEN opportunity.leadsource = 'Existing Customer' THEN 'existing' ELSE 'new' END AS leadsource, 
      accountid, 
      account.name AS account, 
      CASE WHEN account.Customer_Category__c = 'Platinum' THEN '1.Platinum' WHEN account.Customer_Category__c = 'Gold' THEN '2.Gold' WHEN account.Customer_Category__c = 'Silver' THEN '3.Silver' WHEN account.Customer_Category__c = 'Bronze' THEN '4.Bronze' ELSE '5.Unknown' END AS client_category, 
      CASE WHEN account.Customer_Category__c = 'Platinum' THEN TRUE ELSE FALSE END AS is_platinum, 
      opportunity.name AS opportunity_name, 
      opportunity.name__c AS developer_name, 
      opportunity.project_start_date__c AS project_start_date, 
      CONCAT(
        account.name, ' - ', opportunity.name, 
        ' - ', opportunity.name__c
      ) AS opportunity_address, 
      opportunity.closedate AS sales_close_date, 
      opportunity.date_lost__c AS lost_or_closed_date, 
      opportunity.lost_stage__c AS lost_stage, 
      opportunity.stagename AS curr_stage, 
      opportunity.createddate AS opportunity_created_date, 
      opportunity.customer_bill_rate__c AS opp_cust_bill_rate, 
      opportunity.cost_per_hour__c AS opp_cost_per_hour, 
      opportunity.matching_system_developer_link__c AS MS_dev_link, 
      opportunity.matching_system_job_link__c AS MS_job_link, 
      opportunity.workspace_match_id__c AS WS_match_id, 
      opportunity.workspace_user_id__c AS WS_user_id, 
      opportunity.opp_lost_reason__c AS lost_reason, 
      opportunity.opp_lost_reason_elaboration__c AS lost_reason_elaboration, 
      opportunity.aborted_reason__c AS aborted_reason, 
      opportunity.aborted_reason_elaboration__c AS aborted_reason_elaboration, 
      opportunity.stagename AS stage, 
      opportunity.isdeleted, 
      vertical_eng_leader__c, 
      CASE WHEN region__c = 'Rest of US' THEN '2.Rest of US' WHEN region__c = 'Bay Area' THEN '1.Bay Area' WHEN region__c = 'Other top 30 per capita GDP Country. (Western Europe, Singapore, Australia, NZ, Japan, Canada etc.)' THEN '3.Top 30 GDP' WHEN region__c = 'Other country with lower per capita GDP' THEN '4.Others' WHEN region__c IS NULL THEN NULL ELSE '5.Error' END AS region, 
      row_number() over (
        partition BY opportunity.id 
        ORDER BY 
          opportunity.createddate
      ) AS rn 
    FROM 
      raw.Opportunity opportunity 
      LEFT JOIN raw.Account account ON opportunity.accountid = account.id 
    WHERE 
      opportunity.isdeleted = FALSE -- and stagename != 'Discovery Call' 
      ), 
  history as (
    select 
      distinct opportunityid, 
      createddate as modified_date, 
      LAG(stagename, 1) OVER (
        PARTITION BY opportunityid 
        ORDER BY 
          createddate
      ) AS fromstage, 
      stagename as tostage, 
    from 
      raw.OpportunityHistory
  ), 
  history_lost as (
    select 
      opportunityid, 
      modified_date as last_lost_date, 
      row_number() over (
        partition by opportunityid 
        order by 
          modified_date desc
      ) as rn 
    from 
      history 
    where 
      tostage in ('Lost', 'Project Aborted')
  ), 
  history_complete as (
    select 
      opportunityid, 
      modified_date as complete_date, 
      row_number() over (
        partition by opportunityid 
        order by 
          modified_date desc
      ) as rn 
    from 
      history 
    where 
      tostage = 'Project Successfully Completed'
  ), 
  history_start as (
    select 
      opportunityid, 
      modified_date as start_date, 
      row_number() over (
        partition by opportunityid 
        order by 
          modified_date
      ) as rn 
    from 
      history 
    where 
      (
        tostage = 'Paying Customer' 
        AND fromstage != 'Paying Customer'
      )
  ), 
  history_trial as (
    select 
      opportunityid, 
      modified_date as trial_date, 
      row_number() over (
        partition by opportunityid 
        order by 
          modified_date
      ) as rn 
    from 
      history 
    where 
      (
        tostage = '2 Week Trial Start' 
        or tostage = 'Paying Customer'
      )
  ), 
  history_signed as (
    select 
      opportunityid, 
      modified_date as signed_date, 
      row_number() over (
        partition by opportunityid 
        order by 
          modified_date
      ) as rn 
    from 
      history 
    where 
      tostage in (
        'Engagement Agreement Signed', '2 Week Trial Start', 
        'Paying Customer'
      )
  ), 
  history_chosen as (
    select 
      opportunityid, 
      modified_date as chosen_date, 
      row_number() over (
        partition by opportunityid 
        order by 
          modified_date
      ) as rn 
    from 
      history 
    where 
      tostage in (
        'Resource Choosen', 'Resource Chosen', 
        'Engagement Agreement Signed', 
        '2 Week Trial Start', 'Paying Customer'
      )
  ), 
  opportunities1 as (
    select 
      opportunities.*, 
      safe_cast(
        REGEXP_EXTRACT(MS_job_link, r "([0-9]+)") AS INT64
      ) as job_id, 
      safe_cast(
        REGEXP_EXTRACT(MS_dev_link, r "([0-9]+)") AS INT64
      ) as dev_id, 
      case when curr_stage = 'Project Successfully Completed' 
      and lost_or_closed_date > start_date then lost_or_closed_date when curr_stage = 'Project Successfully Completed' 
      and (
        lost_or_closed_date < start_date 
        or lost_or_closed_date is null
      ) then complete_date else null end as complete_date, 
      case when curr_stage != 'Project Successfully Completed' 
      and lost_or_closed_date is not null 
      and (
        curr_stage in ('Lost', 'Project Aborted') 
        or lost_or_closed_date > COALESCE(
          chosen_date, 
          case when history_trial.opportunityid is not null then COALESCE(project_start_date, trial_date) else null end
        ) 
        or COALESCE(
          chosen_date, 
          case when history_trial.opportunityid is not null then COALESCE(project_start_date, trial_date) else null end
        ) is null
      ) then COALESCE(
        lost_or_closed_date, last_lost_date
      ) end as lost_date, 
      greatest(
        start_date, opportunity_created_date
      ) as history_start_date, 
      case when history_start.opportunityid is not null then COALESCE(project_start_date, start_date) else null end as start_date, 
      case when history_trial.opportunityid is not null then COALESCE(project_start_date, trial_date) else null end as trial_date, 
      COALESCE(
        signed_date, 
        case when history_trial.opportunityid is not null then COALESCE(project_start_date, trial_date) else null end
      ) as signed_date, 
      COALESCE(
        chosen_date, 
        case when history_trial.opportunityid is not null then COALESCE(project_start_date, trial_date) else null end
      ) as chosen_date 
    from 
      opportunities 
      left join (
        select 
          * 
        from 
          history_lost 
        where 
          history_lost.rn = 1
      ) history_lost on opportunities.opportunityid = history_lost.opportunityid 
      left join (
        select 
          * 
        from 
          history_complete 
        where 
          history_complete.rn = 1
      ) history_complete on opportunities.opportunityid = history_complete.opportunityid 
      left join (
        select 
          * 
        from 
          history_start 
        where 
          history_start.rn = 1
      ) history_start on opportunities.opportunityid = history_start.opportunityid 
      left join (
        select 
          * 
        from 
          history_trial 
        where 
          history_trial.rn = 1
      ) history_trial on opportunities.opportunityid = history_trial.opportunityid 
      left join (
        select 
          * 
        from 
          history_signed 
        where 
          history_signed.rn = 1
      ) history_signed on opportunities.opportunityid = history_signed.opportunityid 
      left join (
        select 
          * 
        from 
          history_chosen 
        where 
          history_chosen.rn = 1
      ) history_chosen on opportunities.opportunityid = history_chosen.opportunityid 
    where 
      opportunities.rn = 1
  ), 
  account_trials as (
    select 
      accountid, 
      min(trial_date) as account_1st_psd, 
      min(opportunity_created_date) as account_1st_opp_created_date 
    from 
      opportunities1 
      left join raw.Account account ON opportunities1.accountid = account.id 
    group by 
      1
  ), 
  account_starts as (
    select 
      accountid, 
      opportunityid, 
      start_date, 
      COALESCE(complete_date, lost_date) as end_date 
    from 
      opportunities1 
      left join raw.Account account ON opportunities1.accountid = account.id 
    where 
      start_date is not null
  ), 
  engagement as (
    select 
      opportunities1.opportunityid, 
      count(account_starts.start_date) as num_engage_at_opp_creation 
    from 
      opportunities1 
      left join account_starts on opportunities1.accountid = account_starts.accountid 
      and account_starts.start_date < opportunities1.opportunity_created_date 
    where 
      (
        end_date is null 
        or end_date >= opportunities1.opportunity_created_date
      ) 
    group by 
      1
  ), 
  opportunities2 as (
    select 
      opportunities1.opportunityid, 
      opportunities1.job_id, 
      opportunity_name, 
      developer_name, 
      opportunities1.dev_id, 
      opportunities1.accountid, 
      account, 
      is_platinum, 
      client_category, 
      case when date(opportunity_created_date) > date(account_1st_psd) then 'existing' else 'new' end as client_type, 
      COALESCE(leadsource, 'new') as leadsource, 
      COALESCE(region, location_ori) as region, 
      location as region_filled, 
      case when location in ('1.Bay Area', '2.Rest of US') 
      and client_category = '1.Platinum' then coalesce(num_engage_at_opp_creation, 0)>= 3 when (
        location = '1.Bay Area' 
        and client_category in ('2.Gold', '3.Silver')
      ) 
      or (
        location = '1.Rest of US' 
        and client_category = '2.Gold'
      ) then COALESCE(num_engage_at_opp_creation, 0)>= 2 when location = '5.unlabeled' 
      or client_category = '5.unknown' then null else COALESCE(num_engage_at_opp_creation, 0)>= 1 end as is_established, 
      MS_job_link, 
      MS_dev_link, 
      lost_or_closed_date, 
      lost_stage, 
      isdeleted, 
      curr_stage, 
      opportunity_created_date, 
      date_trunc(
        date(opportunity_created_date), 
        month
      ) as creation_month, 
      project_start_date, 
      history_start_date, 
      complete_date, 
      start_date, 
      trial_date, 
      signed_date, 
      chosen_date, 
      lost_date, 
      sales_close_date, 
      case when complete_date is not null then 1 else 0 end as is_complete, 
      case when start_date is not null then 1 else 0 end as starts, 
      case when trial_date is not null then 1 else 0 end as trials, 
      case when signed_date is not null then 1 else 0 end as is_signed, 
      case when timestamp_diff(
        chosen_date, opportunity_created_date, 
        day
      ) <= 14 then 1 else 0 end as D14_resource_chosen, 
      case when chosen_date is not null then 1 else 0 end as resource_chosen, 
      case when lost_date is not null then 1 else 0 end as is_lost, 
      case when lost_date is not null 
      and start_date is not null then 1 else 0 end as is_fire, 
      case when lost_date is not null 
      and start_date is null 
      and trial_date is not null then 1 else 0 end as is_trial_fail, 
      case when start_date is not null then timestamp_diff(
        COALESCE(
          complete_date, 
          lost_date, 
          current_timestamp()
        ), 
        trial_date, 
        day
      ) end as engagement_days, 
      lost_reason, 
      lost_reason_elaboration, 
      aborted_reason, 
      aborted_reason_elaboration, 
      vertical_eng_leader__c 
    from 
      opportunities1 
      left join account_trials on opportunities1.accountid = account_trials.accountid 
      left join engagement on opportunities1.opportunityid = engagement.opportunityid 
      left join curated.opportunity_geo on opportunities1.opportunityid = opportunity_geo.opportunityid
  ), 
  jobs as (
    select 
      job.id as job_id, 
      job.created_date as job_created_date, 
      job_value, 
      case when customer_category = 'Platinum' then '1.Platinum' when customer_category = 'Gold' then '2.Gold' when customer_category = 'Silver' then '3.Silver' when customer_category = 'Bronze' then '4.Bronze' else null end as customer_category, 
      max_acceptable_rate, 
      tu_vel.full_name as vel, 
      tu_ae.full_name as ae, 
      tu_to.full_name as talent_ops 
    from 
      raw.ms2_job job 
      left join raw.tpm_user tu_vel on job.vertical_eng_leader_id = tu_vel.id 
      left join raw.tpm_user tu_ae on job.opportunity_owner_id = tu_ae.id 
      left join raw.tpm_user tu_to on job.talent_ops_id = tu_to.id
  ), 
  ir as (
    select 
      job_id, 
      min(si_date) as ir_date 
    from 
      curated.job_dev_pairs 
    group by 
      job_id
  ) 
  select 
    opportunities2.* 
  except 
    (
      vertical_eng_leader__c, client_category
    ), 
    COALESCE(ir_date, chosen_date) as ir_date, 
    case when timestamp_diff(
      COALESCE(ir_date, chosen_date), 
      opportunity_created_date, 
      day
    ) <= 7 then 1 else 0 end as D07_ir, 
    COALESCE(
      customer_category, client_category
    ) as client_category, 
    COALESCE(
      job_value, 
      case when region_filled = '1.Bay Area' 
      and client_category = '1.Platinum' then (
        case when is_established then 11.7 else 56.6 end
      ) when region_filled = '2.Rest of US' 
      and client_category = '1.Platinum' then (
        case when is_established then 11.7 else 29.2 end
      ) when region_filled = '1.Bay Area' 
      and client_category = '2.Gold' then (
        case when is_established then 11.7 else 22.2 end
      ) when region_filled = '2.Rest of US' 
      and client_category = '2.Gold' then (
        case when is_established then 11.7 else 23.4 end
      ) when region_filled = '1.Bay Area' 
      and client_category = '3.Silver' then (
        case when is_established then 9.1 else 14.7 end
      ) when region_filled = '2.Rest of US' 
      and client_category = '3.Silver' then (
        case when is_established then 6.5 else 13 end
      ) when region_filled = '1.Bay Area' 
      and client_category = '4.Bronze' then (
        case when is_established then 5.85 else 11.5 end
      ) when region_filled = '2.Rest of US' 
      and client_category = '4.Bronze' then (
        case when is_established then 3.9 else 7.4 end
      ) when region_filled = '3.Top 30 GDP' 
      and client_category = '1.Platinum' then (
        case when is_established then 5.9 else 5.8 end
      ) when region_filled = '4.Others' 
      and client_category = '1.Platinum' then (
        case when is_established then 2.0 else 1.9 end
      ) when region_filled = '3.Top 30 GDP' 
      and client_category = '2.Gold' then (
        case when is_established then 5.9 else 4.7 end
      ) when region_filled = '4.Others' 
      and client_category = '2.Gold' then (
        case when is_established then 2.0 else 1.6 end
      ) when region_filled = '3.Top 30 GDP' 
      and client_category = '3.Silver' then (
        case when is_established then 3.3 else 2.6 end
      ) when region_filled = '4.Others' 
      and client_category = '3.Silver' then (
        case when is_established then 1.1 else 0.9 end
      ) when region_filled = '3.Top 30 GDP' 
      and client_category = '4.Bronze' then (
        case when is_established then 2.0 else 1.5 end
      ) when region_filled = '4.Others' 
      and client_category = '4.Bronze' then (
        case when is_established then 0.7 else 0.5 end
      ) else null end
    ) as value, 
    job_created_date, 
    COALESCE(jobs.vel, user.full_name) as vel, 
    jobs.ae as ae, 
    talent_ops, 
    max_acceptable_rate 
  from 
    opportunities2 
    LEFT JOIN raw.tpm_user user ON CAST(
      opportunities2.vertical_eng_leader__c AS INT64
    ) = user.id 
    LEFT JOIN jobs ON opportunities2.job_id = jobs.job_id 
    LEFT JOIN ir ON opportunities2.job_id = ir.job_id 
  where 
    lower(account) not like '%turing%' 
    and lower(account) not like '%test%' 
    and lower(opportunity_name) not like '%turing%'
