create table policies_snapshot_v3 as

with min_cnv_dt_vw as (select indvdl_id, min (cnv_dt) as cnv_dt from (
select pf.indvdl_id, 
case when tc.aap_pol_conversion_flag is null and tc.aap_pol_conversion_date is null then pd.cnv_dt
     when tc.aap_pol_conversion_date is not null and tc.aap_pol_conversion_flag = 1 then tc.AAP_POL_CONVERSION_DATE
     else null end as cnv_dt
from gli_mart.gli_policy_f pf join gli_mart.gli_policy_d pd 
on pf.policy_num = pd.policy_num and pf.policy_rank=pd.policy_rank
left join gli_mart.true_conv_201812 tc 
on pf.policy_num = tc.policy_no and pf.policy_rank = tc.rank
where pf.policy_rank = 0) X 
group by indvdl_id)

SELECT 
trunc(CURRENT_DATE) as curr_date,
REC.INDVDL_ID, 
PF.POLICY_NUM, 
PRD.POLICY_PROD_CAT, 
FACE_AMT, 
CASE 
  WHEN PF.INDVDL_ID IS NULL OR PF.INDVDL_ID = -1 THEN NULL
    WHEN trc.aap_pol_conversion_flag is null and trc.aap_pol_conversion_date is null 
    AND NVL(PD.CNV_DT, TO_DATE('01/01/9999', 'MM/DD/YYYY')) = NVL(min_cnv_dt_vw.CNV_DT, TO_DATE('01/01/9999', 'MM/DD/YYYY')) THEN 'PROSPECT'
     when trc.aap_pol_conversion_date is not null and trc.aap_pol_conversion_flag = 1
     AND NVL(trc.AAP_POL_CONVERSION_DATE, TO_DATE('01/01/9999', 'MM/DD/YYYY')) = NVL(min_cnv_dt_vw.CNV_DT, TO_DATE('01/01/9999', 'MM/DD/YYYY')) THEN 'PROSPECT' 
      ELSE 'EXISTING' END AS AUDIENCE_TYPE,
md_type_chnnl,
MD_TYPE_SUB_CHNNL,
ch.subchannel_name as APPL_MODE,
AD.APPL_RCVD_DT,
policy_issue_dt,
case when trc.aap_pol_conversion_flag is null and trc.aap_pol_conversion_date is null then pd.cnv_dt
     when trc.aap_pol_conversion_date is not null and trc.aap_pol_conversion_flag = 1 then trc.AAP_POL_CONVERSION_DATE
     else null end as cnv_dt,
POLICY_STAT_nm,
CASE WHEN CURR_PYMNT_TO_DT + 67 > CURRENT_DATE THEN NULL ELSE CURR_PYMNT_TO_DT + 67 END AS INACTIVE_DT,
CURR_PYMNT_TO_DT,
CASE WHEN PD.CNV_DT IS NULL THEN 0
  ELSE MONTHS_BETWEEN(CURR_PYMNT_TO_DT, PD.CNV_DT) * mthly_prem_amt END AS TOTAL_PAID,
CASE WHEN PF.INDVDL_ID IS NULL OR PF.INDVDL_ID = -1 THEN NULL ELSE REC.DOB END AS OWNER_DOB,
CASE WHEN PF.INDVDL_ID IS NULL OR PF.INDVDL_ID = -1 THEN NULL ELSE REC.GENDER_CD END AS OWNER_GENDER,
CASE WHEN PF.INS_INDVDL_ID IS NULL OR PF.INS_INDVDL_ID = -1 THEN NULL ELSE INS.DOB END AS INS_DOB,
CASE WHEN PF.INS_INDVDL_ID IS NULL OR PF.INS_INDVDL_ID = -1 THEN NULL ELSE INS.GENDER_CD END AS INS_GENDER,
  REC.Zip_Cd,
  REC.Zipp4_Cd
FROM GLI_MART.GLI_POLICY_F PF
LEFT JOIN GLI_MART.GLI_POLICY_D PD ON PF.POLICY_NUM = PD.POLICY_NUM AND PF.POLICY_RANK = PD.POLICY_RANK
LEFT JOIN GLI_MART.GLI_APPL_F AF ON PF.POLICY_NUM = AF.APPL_NUM
LEFT JOIN GLI_MART.GLI_APPL_D AD ON AF.APPL_NUM = AD.APPL_NUM
LEFT JOIN gli_mart.gli_channel_id_ref_d ch on ad.dm_rcvng_chnl_id = ch.inbound_src_id
left JOIN 
     (Select * from GLI_MART.RECIPIENT_D
             where INDVDL_IDENTITY_OR_TXN_CD = 'I') REC ON PF.INDVDL_ID = REC.INDVDL_ID
left JOIN GLI_MART.GLI_POLICY_PROD_D PRD ON PD.POLICY_PROD_SKEY = PRD.POLICY_PROD_SKEY
left JOIN gli_bbodek.media_type_ref md on AD.market_ky_media_type = md.MD_TYPE_CD
LEFT JOIN GLI_MART.true_conv_201812 TRC ON pf.policy_num = trc.policy_no and pf.policy_rank = trc.rank
LEFT JOIN 
     (Select * from GLI_MART.RECIPIENT_D
             where INDVDL_IDENTITY_OR_TXN_CD = 'I') INS ON PF.INS_INDVDL_ID = INS.INDVDL_ID
LEFT JOIN min_cnv_dt_vw ON PF.INDVDL_ID = min_cnv_dt_vw.INDVDL_ID
WHERE PF.POLICY_RANK = 0 AND PD.POLICY_RANK = 0
;
