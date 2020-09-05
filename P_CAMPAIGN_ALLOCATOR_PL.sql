ALTER PROC [dbo].[P_CAMPAIGN_ALLOCATOR_PL]
--(@para_UserID FLOAT)
AS
BEGIN
BEGIN TRY
DECLARE
    @vc_product_name          VARCHAR(100),
    @vc_actioned_days         NUMERIC(5),

    @var_ProcessStatusID      NUMERIC(20),
    @vc_current_date_time     DATETIME,
    @vc_current_date          DATE,
    @vc_history_review_date   DATETIME,
    
    @cncl_c121_status       VARCHAR(100),
    @cncl_parental_status   VARCHAR(100),    
    @cncl_dStatus_Date      DATETIME,
    @cncl_dReview_Date      DATETIME,
    @cncl_Loan_Master_ID    NUMERIC(20,0),
    @cncl_Loan_Product_ID   NUMERIC(20,0),
    @cncl_Parental_Status_Id NUMERIC(20,0),
    @cncl_product_id        NUMERIC(20,0),
    @vc_new_c121_status     VARCHAR(100),
    @vc_action_status       VARCHAR(100),
    @vc_onhold_expiry_date  DATETIME,    
    
    @hist_ptp_arr_id        NUMERIC(20,0),
    @loanmaster_id          NUMERIC(20,0),
    @ptp_date               DATE,
    @followup_date          DATE,
    @History_Detail_ID      NUMERIC(20,0),
    @Loan_Product_ID        NUMERIC(20,0),
    @cat3id                 NUMERIC(20,0),
    @Parental_Status_ID     NUMERIC(20,0),
    @ptp_amount             NUMERIC(20,2),
    @parental_status        VARCHAR(100),
    @sms_method             VARCHAR(10),
    @product_id             NUMERIC(20,0),
    @vc_payment_amount      NUMERIC(20,2),    
    @vc_ptp_status          VARCHAR(1),
    @vc_notes               VARCHAR(1000),
    @vc_iaction_acc_status_ID NUMERIC(20,0),
    @vc_c121_status         VARCHAR(100),    
    @action_history_curr_id NUMERIC(20,0),
    @vc_ptp_payment_pct     NUMERIC(20,2),
    @vc_ptp_reporting_status VARCHAR(20),

    @arr_date               DATE,
    @arr_amount             NUMERIC(20,2),
    @arr_instlmnt_date      DATE,
    @hist_Pay_Arr_ID        NUMERIC(20,0),
    @max_arr_date           DATE,
    @vc_arr_status          VARCHAR(1),
    
    @Snapshot_Date          DATE,
    @Campaign_Section_Id    NUMERIC(20,0),
    @acc_count              NUMERIC(20,0),
    @campaign_main_curr_id  NUMERIC(20,0),
    @campaign_details_curr_id NUMERIC(20,0),

	@sql                    VARCHAR(4000),	
	@CAMPAIGN_CODE			VARCHAR(100),
	@PRODUCT_CODE			VARCHAR(20),
	@CONDITION				VARCHAR(4000),
	
	@IsERROR INT,
	@Error_Message VARCHAR(500),
    @ERROR_LINE VARCHAR(20);

	SET @var_ProcessStatusID = NEXT VALUE FOR SEQ_PROCESS_STATUS_LOG;
    SET @vc_current_date_time = GETDATE();
	SET @vc_current_date =  CONVERT(DATE,GETDATE());
	SET @vc_product_name =  'PL';
	SET @vc_actioned_days =  3;
	SET @vc_history_review_date =  CONVERT(Datetime,'01-01-1901 00:00:00', 120);

    INSERT INTO tbl_process_status_log( logid,
                                        processdesc,
                                        phase,
                                        starttime
                                        )
    VALUES( @var_ProcessStatusID,
            'Start Campaign Creation Stored Procedure P_CAMPAIGN_ALLOCATOR_PL',
            'Campaign Creation',
            GETDATE());

			DECLARE cancel_cur CURSOR FAST_FORWARD FOR
                       SELECT  --acc_stat.iC121_Status_ID,
                                cs.vtitle,
                                ea.dStatus_Date,
                                ISNULL(ea.dReview_Date,@vc_history_review_date),
                                lm.iLoan_Master_ID,
                                lm.iLoan_Product_ID,
                                acc_stat.iParental_Status_Id,
                                lm.iproduct_id
                        FROM    tbl_loan_master lm,
                                tbl_account_status acc_stat,
                                tbl_exception_actions ea,
                                tbl_c121_status cs
                        WHERE   lm.iLoan_Master_ID        = acc_stat.iLoan_Master_ID
                        AND     ea.iLoan_Master_Id        = lm.iLoan_Master_ID
                        AND     acc_stat.iC121_Status_ID  = cs.ic121_status_id
                        AND     ea.iActive                = 1
                        AND     EXISTS ( SELECT 'X'
                                         FROM   tbl_product p
                                         WHERE  p.iproduct_id = lm.iproduct_id
                                         AND    p.vloan_code  = @vc_product_name)
                        AND     acc_stat.iC121_Status_ID  IN (  SELECT iC121_Status_ID
                                                                FROM   tbl_c121_status
                                                                WHERE  vTitle IN (  'ONHOLD_PENDING_APPROVAL',
                                                                                    'ONHOLD_APPROVED',
                                                                                    'ONHOLD_REVIEW_ON',
                                                                                    'DECEASED_PENDING_APPROVAL',
                                                                                    'FRAUD_PENDING_APPROVAL',
                                                                                    'SKIP_TRACE_PENDING_APPROVAL',
                                                                                    'CRITICAL_ILLNESS_PENDING_APPROVAL',
                                                                                    'MEDIATION_BOARD_PENDING_APPROVAL',
                                                                                    'SPECIAL_SITUATIONS_PENDING_APPROVAL',
                                                                                    'REHABILITATION_PENDING_APPROVAL',
                                                                                    'NEGOTIATION_PENDING_APPROVAL',
                                                                                    'REPOSSESSION_PENDING_APPROVAL',
                                                                                    'PARATE_EXECUTION_PENDING_APPROVAL',
                                                                                    'ENJOINING_ORDER_PENDING_APPROVAL',
                                                                                    'EXTERNAL_AGENT_PENDING_APPROVAL',
                                                                                    'MORTGAGE_BOND_PENDING_APPROVAL',
                                                                                    'MONEY_RECOVERY_PENDING_APPROVAL',
                                                                                    'DEBT_RECOVERY_PENDING_APPROVAL',
                                                                                    'EVICTION_PENDING_APPROVAL',
                                                                                    'MISCELLANEOUS_PENDING_APPROVAL',
                                                                                    'APPEAL_PENDING_APPROVAL_MORTGAGE',
                                                                                    'APPEAL_PENDING_APPROVAL_MONEY_RECOVERY',
                                                                                    'APPEAL_PENDING_APPROVAL_DEBT_RECOVERY',
                                                                                    'APPEAL_PENDING_APPROVAL_EVICTION',
                                                                                    'APPEAL_PENDING_APPROVAL_MISCELLANEOUS',
                                                                                    'LEGAL_PENDING_APPROVAL',
                                                                                    'PRE_LEGAL_PENDING_APPROVAL',
                                                                                    'RECOVERIES_PENDING_APPROVAL',
                                                                                    'RO_ACTIVE_PENDING_APPROVAL',
                                                                                    'SRT_PENDING_APPROVAL',
                                                                                    'COMPLAINT_PENDING_APPROVAL'
                                                                                    )
                                                                                 );
                                                                                 
			OPEN cancel_cur
			FETCH NEXT FROM cancel_cur
			INTO  @cncl_c121_status,@cncl_dStatus_Date,@cncl_dReview_Date,@cncl_Loan_Master_ID,@cncl_Loan_Product_ID,@cncl_Parental_Status_Id,@cncl_product_id   

			WHILE @@FETCH_STATUS = 0
			BEGIN /* Begin of While loop */ 
            
            SET @vc_new_c121_status = NULL;
            SET @vc_action_status   = NULL;
            SET @vc_notes           = NULL;
            SET @vc_iaction_acc_status_ID = NULL;
            
       -- If an account in exception is not actioned within 3 days then the C121 Status is changed as Cancelled
        IF ((@cncl_c121_status = 'ONHOLD_PENDING_APPROVAL') AND (@vc_current_date > @cncl_dStatus_Date + @vc_actioned_days))
            BEGIN
            SET @vc_new_c121_status = 'ONHOLD_CANCELLED';
            SET @vc_action_status   = 'ONHOLD_CANCELLED';
            END
        ELSE IF ((@cncl_c121_status = 'DECEASED_PENDING_APPROVAL') AND (@vc_current_date > @cncl_dStatus_Date + @vc_actioned_days))
            BEGIN        
            SET @vc_new_c121_status = 'DECEASED_CANCELLED';
            SET @vc_action_status   = 'DECEASED_CANCELLED';
            END
        ELSE IF ((@cncl_c121_status = 'COMPLAINT_PENDING_APPROVAL') AND (@vc_current_date > @cncl_dStatus_Date + @vc_actioned_days))
            BEGIN        
            SET @vc_new_c121_status = 'COMPLAINT_AUTO_WITHDRAWN';
            SET @vc_action_status   = 'COMPLAINT_AUTO_WITHDRAWN';
            END             
        ELSE IF ((@cncl_c121_status = 'FRAUD_PENDING_APPROVAL') AND (@vc_current_date > @cncl_dStatus_Date + @vc_actioned_days))
            BEGIN        
            SET @vc_new_c121_status = 'FRAUD_CANCELLED';
            SET @vc_action_status   = 'FRAUD_CANCELLED';
            END
        ELSE IF ((@cncl_c121_status = 'SKIP_TRACE_PENDING_APPROVAL') AND (@vc_current_date > @cncl_dStatus_Date + @vc_actioned_days))
            BEGIN        
            SET @vc_new_c121_status = 'SKIP_TRACE_CANCELLED';
            SET @vc_action_status   = 'SKIP_TRACE_CANCELLED';
            END
        ELSE IF ((@cncl_c121_status = 'RNR_PENDING_APPROVAL') AND (@vc_current_date > @cncl_dStatus_Date + @vc_actioned_days))
            BEGIN        
            SET @vc_new_c121_status = 'RNR_AUTO_WITHDRAWN';
            SET @vc_action_status   = 'RNR_WITHDRAWN'; 
            END
        ELSE IF ((@cncl_c121_status = 'REHABILITATION_PENDING_APPROVAL') AND (@vc_current_date > @cncl_dStatus_Date + @vc_actioned_days))
            BEGIN        
            SET @vc_new_c121_status = 'REHABILITATION_AUTO_WITHDRAWN';
            SET @vc_action_status   = 'REHABILITATION_WITHDRAWN';
            END
        ELSE IF ((@cncl_c121_status = 'NEGOTIATION_PENDING_APPROVAL') AND (@vc_current_date > @cncl_dStatus_Date + @vc_actioned_days))
            BEGIN        
            SET @vc_new_c121_status = 'NEGOTIATION_AUTO_WITHDRAWN';
            SET @vc_action_status   = 'NEGOTIATION_WITHDRAWN';
            END
        ELSE IF ((@cncl_c121_status = 'REPOSSESSION_PENDING_APPROVAL') AND (@vc_current_date > @cncl_dStatus_Date + @vc_actioned_days))
            BEGIN        
            SET @vc_new_c121_status = 'REPOSSESSION_AUTO_WITHDRAWN';
            SET @vc_action_status   = 'REPOSSESSION_WITHDRAWN';
            END
        ELSE IF ((@cncl_c121_status = 'PARATE_EXECUTION_PENDING_APPROVAL') AND (@vc_current_date > @cncl_dStatus_Date + @vc_actioned_days))
            BEGIN        
            SET @vc_new_c121_status = 'PARATE_EXECUTION_AUTO_WITHDRAWN';
            SET @vc_action_status   = 'PARATE_WITHDRAWN';            
            END
        ELSE IF ((@cncl_c121_status = 'ENJOINING_ORDER_PENDING_APPROVAL') AND (@vc_current_date > @cncl_dStatus_Date + @vc_actioned_days))
            BEGIN        
            SET @vc_new_c121_status = 'ENJOINING_ORDER_AUTO_WITHDRAWN';
            SET @vc_action_status   = 'ENJOINING_ORDER_WITHDRAWN';
            END
        ELSE IF ((@cncl_c121_status = 'MORTGAGE_BOND_PENDING_APPROVAL') AND (@vc_current_date > @cncl_dStatus_Date + @vc_actioned_days))
            BEGIN        
            SET @vc_new_c121_status = 'MORTGAGE_BOND_AUTO_WITHDRAWN';
            SET @vc_action_status   = 'MORTGAGE_BOND_WITHDRAWN';
            END
        ELSE IF ((@cncl_c121_status = 'MONEY_RECOVERY_PENDING_APPROVAL') AND (@vc_current_date > @cncl_dStatus_Date + @vc_actioned_days))
            BEGIN        
            SET @vc_new_c121_status = 'MONEY_RECOVERY_AUTO_WITHDRAWN';
            SET @vc_action_status   = 'MONEY_RECOVERY_WITHDRAWN';  
            END
        ELSE IF ((@cncl_c121_status = 'DEBT_RECOVERY_PENDING_APPROVAL') AND (@vc_current_date > @cncl_dStatus_Date + @vc_actioned_days))
            BEGIN        
            SET @vc_new_c121_status = 'DEBT_RECOVERY_AUTO_WITHDRAWN';
            SET @vc_action_status   = 'DEBT_RECOVERY_WITHDRAWN';
            END
        ELSE IF ((@cncl_c121_status = 'EVICTION_PENDING_APPROVAL') AND (@vc_current_date > @cncl_dStatus_Date + @vc_actioned_days))
            BEGIN        
            SET @vc_new_c121_status = 'EVICTION_AUTO_WITHDRAWN';
            SET @vc_action_status   = 'EVICTION_WITHDRAWN';
            END
        ELSE IF ((@cncl_c121_status = 'MISCELLANEOUS_PENDING_APPROVAL') AND (@vc_current_date > @cncl_dStatus_Date + @vc_actioned_days))
            BEGIN        
            SET @vc_new_c121_status = 'MISCELLANEOUS_AUTO_WITHDRAWN';
            SET @vc_action_status   = 'MISCELLANEOUS_WITHDRAWN';            
            END
        ELSE IF ((@cncl_c121_status = 'APPEAL_PENDING_APPROVAL_MORTGAGE') AND (@vc_current_date > @cncl_dStatus_Date + @vc_actioned_days))
            BEGIN        
            SET @vc_new_c121_status = 'APPEAL_AUTO_WITHDRAWN_MORTGAGE';
            SET @vc_action_status   = 'APPEAL_WITHDRAWN';
            END
        ELSE IF ((@cncl_c121_status = 'APPEAL_PENDING_APPROVAL_MONEY_RECOVERY') AND (@vc_current_date > @cncl_dStatus_Date + @vc_actioned_days))
            BEGIN        
            SET @vc_new_c121_status = 'APPEAL_AUTO_WITHDRAWN_MONEY_RECOVERY';
            SET @vc_action_status   = 'APPEAL_WITHDRAWN';
            END
        ELSE IF ((@cncl_c121_status = 'APPEAL_PENDING_APPROVAL_DEBT_RECOVERY') AND (@vc_current_date > @cncl_dStatus_Date + @vc_actioned_days))
            BEGIN        
            SET @vc_new_c121_status = 'APPEAL_AUTO_WITHDRAWN_DEBT_RECOVERY';
            SET @vc_action_status   = 'APPEAL_WITHDRAWN';
            END
        ELSE IF ((@cncl_c121_status = 'APPEAL_PENDING_APPROVAL_EVICTION') AND (@vc_current_date > @cncl_dStatus_Date + @vc_actioned_days))
            BEGIN        
            SET @vc_new_c121_status = 'APPEAL_AUTO_WITHDRAWN_EVICTION';
            SET @vc_action_status   = 'APPEAL_WITHDRAWN';
            END
        ELSE IF ((@cncl_c121_status = 'APPEAL_PENDING_APPROVAL_MISCELLANEOUS') AND (@vc_current_date > @cncl_dStatus_Date + @vc_actioned_days))
            BEGIN        
            SET @vc_new_c121_status = 'APPEAL_AUTO_WITHDRAWN_MISCELLANEOUS';
            SET @vc_action_status   = 'APPEAL_WITHDRAWN';            
            END
        ELSE IF ((@cncl_c121_status = 'EXTERNAL_AGENT_PENDING_APPROVAL') AND (@vc_current_date > @cncl_dStatus_Date + @vc_actioned_days))
            BEGIN        
            SET @vc_new_c121_status = 'EXTERNAL_AGENT_AUTO_WITHDRAWN';
            SET @vc_action_status   = 'EXTERNAL_AGENT_WITHDRAWN'; 
            END
        ELSE IF ((@cncl_c121_status = 'CRITICAL_ILLNESS_PENDING_APPROVAL') AND (@vc_current_date > @cncl_dStatus_Date + @vc_actioned_days))
            BEGIN        
            SET @vc_new_c121_status = 'CRITICAL_ILLNESS_CANCELLED';
            SET @vc_action_status   = 'CRITICAL_ILLNESS_CANCELLED';
            END 
        ELSE IF ((@cncl_c121_status = 'MEDIATION_BOARD_PENDING_APPROVAL') AND (@vc_current_date > @cncl_dStatus_Date + @vc_actioned_days))
            BEGIN        
            SET @vc_new_c121_status = 'MEDIATION_BOARD_CANCELLED';
            SET @vc_action_status   = 'MEDIATION_BOARD_CANCELLED';
            END 
        ELSE IF ((@cncl_c121_status = 'PRE_LEGAL_PENDING_APPROVAL') AND (@vc_current_date > @cncl_dStatus_Date + @vc_actioned_days))
            BEGIN        
            SET @vc_new_c121_status = 'PRE_LEGAL_CANCELLED';
            SET @vc_action_status   = 'PRE_LEGAL_CANCELLED';
            END             
        ELSE IF ((@cncl_c121_status = 'SPECIAL_SITUATIONS_PENDING_APPROVAL') AND (@vc_current_date > @cncl_dStatus_Date + @vc_actioned_days))
            BEGIN        
            SET @vc_new_c121_status = 'SPECIAL_SITUATIONS_CANCELLED';
            SET @vc_action_status   = 'SPECIAL_SITUATIONS_CANCELLED';
            END
        ELSE IF ((@cncl_c121_status = 'RO_ACTIVE_PENDING_APPROVAL') AND (@vc_current_date > @cncl_dStatus_Date + @vc_actioned_days))
            BEGIN        
            SET @vc_new_c121_status = 'RO_ACTIVE_CANCELLED';
            SET @vc_action_status   = 'RO_ACTIVE_CANCELLED';
            END
        ELSE IF ((@cncl_c121_status = 'SRT_PENDING_APPROVAL') AND (@vc_current_date > @cncl_dStatus_Date + @vc_actioned_days))
            BEGIN        
            SET @vc_new_c121_status = 'SRT_AUTO_WITHDRAWN';
            SET @vc_action_status   = 'SRT_AUTO_WITHDRAWN';
            END            
       ELSE IF (@cncl_c121_status IN ('ONHOLD_PENDING_APPROVAL','ONHOLD_APPROVED','ONHOLD_REVIEW_ON'))  -- Onhold status has been expired  
       
        BEGIN
          BEGIN TRY
          
                SELECT  @vc_onhold_expiry_date = ISNULL(CONVERT(DATE,a.dreview_date),@vc_history_review_date)
                FROM    tbl_exception_actions a
                WHERE   a.iloan_master_id = @cncl_Loan_Master_ID
                AND     a.iactive         = 1  
                AND     EXISTS ( SELECT 'X'
                                 FROM   tbl_c121_status b
                                 WHERE  b.iC121_Status_Id = a.istatus_id
                                 AND    b.vTitle          = 'ONHOLD_APPROVED');
                                 
          END TRY                      
          BEGIN CATCH
                        SELECT @vc_onhold_expiry_date = ISNULL(x.ddate_to,@vc_history_review_date)
                        FROM   (
                                    SELECT b.ddate_to,
                                           b.iaction_hist_dtl_id,
                                           MAX(b.iaction_hist_dtl_id) OVER() AS max_iaction_hist_dtl_id
                                    FROM   tbl_action_history a,
                                           tbl_action_history_details b,
                                           tbl_action_category3 c
                                    WHERE  a.iaction_history_id = b.ihistory_id
                                    AND    b.icat3_id           = c.icategory3_id
                                    AND    a.iloan_master_id    = @cncl_Loan_Master_ID
                                    AND    a.IPRODUCT_ID   = @cncl_product_id
                                    AND    c.vcode              = 'OH'
                               ) x
                        WHERE  x.iaction_hist_dtl_id = x.max_iaction_hist_dtl_id;
          END CATCH
          
            IF (@vc_current_date >= @vc_onhold_expiry_date)
                BEGIN
                SET @vc_new_c121_status = 'ONHOLD_EXPIRED';
                SET @vc_action_status   = 'ONHOLD_EXPIRED';
                END
            --END IF
        END
        
        IF (@vc_new_c121_status IS NOT NULL)
            BEGIN
            
            UPDATE tbl_account_status
            SET    iC121_Status_ID     = ( SELECT iC121_Status_ID
                                           FROM   tbl_c121_status
                                           WHERE  vTitle = @vc_new_c121_status),
                   iParental_Status_id = ( SELECT iParantial_Status_Id
                                           FROM   tbl_c121_status
                                           WHERE  vTitle = @cncl_c121_status)
            WHERE  iLoan_Master_ID  = @cncl_Loan_Master_ID; 
            
           SET @vc_notes = 'Collect121 Status Changed from '+@cncl_c121_status+' To '+@vc_new_c121_status+'. Parental Status Changed To COLLECTIONS.';
           
            INSERT INTO tbl_action_history(   iaction_history_id,
                                              iloan_master_id,
                                              icampaign_detail_id,
                                              iuser_id,
                                              ddatetime,
                                              iloan_product_id,
                                              tntbs_notes,
                                              vntbs_title,
                                              iaccount_status,
                                              icontact_type_id,
                                              icycle_completed,
                                              icycle_number,
                                              iproduct_id)
            VALUES(  NEXT VALUE FOR seq_action_history,  -- iaction_history_id,
                     @cncl_Loan_Master_ID,        -- iloan_master_id,
                     NULL,                        -- icampaign_detail_id,
                     1,                           -- iuser_id,
                     getdate(),                   -- ddatetime,
                     @cncl_Loan_Product_ID,       -- iloan_product_id,
                     @vc_notes,                   -- tntbs_notes,
                     @vc_notes,                   -- vntbs_title,
                     0,                           -- iaccount_status,
                     NULL,                        -- icontact_type_id,
                     NULL,                        -- icycle_completed,
                     NULL,                        -- icycle_number
                     @cncl_product_id             -- iproduct_id
                  );
                  
                SELECT @vc_iaction_acc_status_ID = iaction_account_status_ID
                FROM   tbl_action_account_status
                WHERE  vTitle = @vc_action_status;

            SELECT @action_history_curr_id = CAST(Current_Value AS INT) FROM sys.Sequences WHERE name='seq_action_history';                
            INSERT INTO tbl_action_history_details ( iaction_hist_dtl_id,
                                                     ihistory_id,
                                                     iparental_status_id,
                                                     itype_id,
                                                     icat1_id,
                                                     icat2_id,
                                                     icat3_id,
                                                     tcomments,
                                                     vtypeid_sub,
                                                     iaccount_status,
                                                     icampaign_detail_id,
                                                     dletter_date,
                                                     vvalidation_status,
                                                     vvalidation_comments,
                                                     damount,
                                                     vpayment_frequency,
                                                     dpayment_occurrence,
                                                     ddate_from,
                                                     dtime_from,
                                                     ddate_to,
                                                     dtime_to,
                                                     vpayment_method,
                                                     dfollow_date,
                                                     created_date)
             VALUES(NEXT VALUE FOR seq_action_history_details,  -- iaction_hist_dtl_id,
                    @action_history_curr_id,             -- ihistory_id,
                    @cncl_Parental_Status_Id,            -- iparental_status_id,
                    1,                                   -- itype_id,   ????
                    1,                                   -- icat1_id,
                    1,                                   -- icat2_id,
                    1,                                   -- icat3_id,
                    NULL,                                -- tcomments,
                    'PF',                                -- vtypeid_sub,
                    @vc_iaction_acc_status_ID,           -- iaccount_status,
                    NULL,                                -- icampaign_detail_id,
                    NULL,                                -- dletter_date,
                    NULL,                                -- vvalidation_status,
                    NULL,                                -- vvalidation_comments,
                    NULL,                                -- damount,
                    NULL,                                -- vpayment_frequency,
                    NULL,                                -- dpayment_occurrence,
                    NULL,                                -- ddate_from,
                    NULL,                                -- dtime_from,
                    NULL,                                -- ddate_to,
                    NULL,                                -- dtime_to,
                    NULL,                                -- vpayment_method,
                    NULL,                               -- dfollow_date)
                    getdate());
                
           
            END
            
            FETCH NEXT FROM cancel_cur
			INTO   @cncl_c121_status,@cncl_dStatus_Date,@cncl_dReview_Date,@cncl_Loan_Master_ID,@cncl_Loan_Product_ID,@cncl_Parental_Status_Id,@cncl_product_id
        END

        CLOSE			cancel_cur
        DEALLOCATE		cancel_cur
        
       -- end of Cancel Cursor -------------------------------------------------*

            -- Check if the PTP has been kept or broken -----------------------*
			DECLARE ptp_cur CURSOR FAST_FORWARD FOR
                            SELECT a.iAction_hist_ptp_arr_id,
                            a.iloanmaster_id,                            
                            CONVERT(DATE,a.dDateTime) AS ptp_date,
                            CONVERT(DATE,a.dfollowup_date) AS followup_date,
                            a.iHistory_Detail_ID,
                            a.iLoan_Product_ID,
                            a.icat3id,
                            c.iParental_Status_ID,
                            a.dAmount AS ptp_amount,
                            d.vTitle AS parental_status,
                            ISNULL(a.vSms_method,'CALL') AS vSms_method,
                            e.iproduct_id
                     FROM   tbl_action_history_ptp_arr a,
                            tbl_account_status c,
                            tbl_parantial_status d,
                            tbl_loan_master e
                     WHERE  a.iloanmaster_id      = c.iLoan_Master_ID
                     AND    c.iParental_Status_ID = d.iParantial_Status_ID
                     AND    a.iloanmaster_id      = e.iLoan_Master_ID
                     AND    a.icat3id = 1
                     AND    a.iActive = 1
                     AND    a.dAmount > 0
                     AND    EXISTS ( SELECT 'X'
                                     FROM   tbl_product p
                                     WHERE  p.iproduct_id = e.iproduct_id
                                     AND    p.vloan_code  = @vc_product_name);

			OPEN ptp_cur
			FETCH NEXT FROM ptp_cur
			INTO  @hist_ptp_arr_id ,@loanmaster_id,@ptp_date,@followup_date,@History_Detail_ID,@Loan_Product_ID,
                  @cat3id,@Parental_Status_ID,@ptp_amount,@parental_status,@sms_method,@product_id

			WHILE @@FETCH_STATUS = 0
			BEGIN /* Begin of While loop */

            SET @vc_ptp_status            = NULL;
            SET @vc_notes                 = NULL;
            SET @vc_iaction_acc_status_ID = NULL;
            SET @vc_c121_status           = NULL;
            
            -- Total payments made to the account
                SELECT @vc_payment_amount = ISNULL(SUM(p.DTOTALAMT),0)
                FROM   tbl_payment_main p
                WHERE  p.iloan_master_id   = @loanmaster_id
                --AND    p.iloan_product_id  = @Loan_Product_ID
                --AND    p.PAYMENT_TYPE not in ('C','D','E')
                AND    CONVERT(DATE,p.dpaymt_date) BETWEEN @ptp_date AND @followup_date;

        IF (@vc_payment_amount >= @ptp_amount)
            BEGIN
            SET @vc_ptp_status = 'K';
            SET @vc_notes      = 'Kept PTP. '+'Period='+CAST(@ptp_date AS VARCHAR)+'/'+CAST(@followup_date AS VARCHAR)+', PTP Amount='+CAST(@ptp_amount AS VARCHAR)+', Payment Amount='+CAST(@vc_payment_amount AS VARCHAR);

            SELECT @vc_iaction_acc_status_ID = iaction_account_status_ID
            FROM   tbl_action_account_status
            WHERE  vTitle IN ('KEPT_PTP');            
            END
        ELSE IF (@vc_current_date >= @followup_date) AND (@vc_payment_amount < @ptp_amount)
            BEGIN
            SET @vc_ptp_status = 'B';                    
            SET @vc_notes      = 'Broken PTP. '+'Period='+CAST(@ptp_date AS VARCHAR)+'/'+CAST(@followup_date AS VARCHAR)+', PTP Amount='+CAST(@ptp_amount AS VARCHAR)+', Payment Amount='+CAST(@vc_payment_amount AS VARCHAR);

            SELECT @vc_iaction_acc_status_ID = iaction_account_status_ID
            FROM   tbl_action_account_status
            WHERE  vTitle IN ('BROKEN_PTP');      
		  	END			  				

       IF (@vc_ptp_status IS NOT NULL)
        BEGIN        

            IF ((@sms_method = 'CALL') AND (@vc_ptp_status = 'B') AND (@parental_status = 'COLLECTIONS'))
            
                SET @vc_c121_status = 'BROKEN_PTP';                            

            ELSE IF ((@sms_method = 'CALL') AND (@vc_ptp_status = 'K') AND (@parental_status = 'COLLECTIONS'))
            
                SET @vc_c121_status = 'KEPT_PTP';                                

            ELSE IF ((@vc_ptp_status = 'B') AND (@parental_status = 'CRITICAL_ILLNESS'))
            
                SET @vc_c121_status = 'CRITICAL_ILLNESS_BROKEN_PTP';

            ELSE IF ((@vc_ptp_status = 'K') AND (@parental_status = 'CRITICAL_ILLNESS'))
            
                SET @vc_c121_status = 'CRITICAL_ILLNESS_KEPT_PTP';

            ELSE IF ((@vc_ptp_status = 'B') AND (@parental_status = 'SPECIAL_SITUATIONS'))
            
                SET @vc_c121_status = 'SPECIAL_SITUATIONS_BROKEN_PTP';

            ELSE IF ((@vc_ptp_status = 'K') AND (@parental_status = 'SPECIAL_SITUATIONS'))
            
                SET @vc_c121_status = 'SPECIAL_SITUATIONS_KEPT_PTP';                

            ELSE IF ((@vc_ptp_status = 'B') AND (@parental_status = 'DECEASED'))
            
                SET @vc_c121_status = 'DECEASED_BROKEN_PTP';

            ELSE IF ((@vc_ptp_status = 'K') AND (@parental_status = 'DECEASED'))
            
                SET @vc_c121_status = 'DECEASED_KEPT_PTP';
                
            ELSE IF ((@vc_ptp_status = 'B') AND (@parental_status = 'COMPLAINT'))
            
                SET @vc_c121_status = 'COMPLAINT_BROKEN_PTP';

            ELSE IF ((@vc_ptp_status = 'K') AND (@parental_status = 'COMPLAINT'))
            
                SET @vc_c121_status = 'COMPLAINT_KEPT_PTP';                

            ELSE IF ((@vc_ptp_status = 'B') AND (@parental_status = 'FRAUD'))
            
                SET @vc_c121_status = 'FRAUD_BROKEN_PTP';

            ELSE IF ((@vc_ptp_status = 'K') AND (@parental_status = 'FRAUD'))
            
                SET @vc_c121_status = 'FRAUD_KEPT_PTP';

            ELSE IF ((@vc_ptp_status = 'B') AND (@parental_status = 'MEDIATION_BOARD'))
            
                SET @vc_c121_status = 'MEDIATION_BOARD_BROKEN_PTP';

            ELSE IF ((@vc_ptp_status = 'K') AND (@parental_status = 'MEDIATION_BOARD'))
            
                SET @vc_c121_status = 'MEDIATION_BOARD_KEPT_PTP';

            ELSE IF ((@vc_ptp_status = 'B') AND (@parental_status = 'SKIP_TRACE'))
            
                SET @vc_c121_status = 'SKIP_TRACE_BROKEN_PTP';

            ELSE IF ((@vc_ptp_status = 'K') AND (@parental_status = 'SKIP_TRACE'))
            
                SET @vc_c121_status = 'SKIP_TRACE_KEPT_PTP';
                
            ELSE IF ((@vc_ptp_status = 'B') AND (@parental_status = 'PRE_LEGAL'))
            
                SET @vc_c121_status = 'PRE_LEGAL_BROKEN_PTP';

            ELSE IF ((@vc_ptp_status = 'K') AND (@parental_status = 'PRE_LEGAL'))
            
                SET @vc_c121_status = 'PRE_LEGAL_KEPT_PTP';                

            --ELSE IF ((@vc_ptp_status = 'B') AND (@parental_status = 'REPOSSESSION'))
            
                --SET @vc_c121_status = 'REPOSSESSION_BROKEN_PTP';

            --ELSE IF ((@vc_ptp_status = 'K') AND (@parental_status = 'REPOSSESSION'))
            
                --SET @vc_c121_status = 'REPOSSESSION_KEPT_PTP';

            ELSE IF ((@vc_ptp_status = 'B') AND (@parental_status = 'RECOVERIES'))
            
                SET @vc_c121_status = 'RECOVERIES_BROKEN_PTP';

            ELSE IF ((@vc_ptp_status = 'K') AND (@parental_status = 'RECOVERIES'))
            
                SET @vc_c121_status = 'RECOVERIES_KEPT_PTP';
                
            ELSE IF ((@vc_ptp_status = 'B') AND (@parental_status = 'EARLY_RECOVERIES'))
            
                SET @vc_c121_status = 'RO_ACTIVE_BROKEN_PTP';

            ELSE IF ((@vc_ptp_status = 'K') AND (@parental_status = 'EARLY_RECOVERIES'))
            
                SET @vc_c121_status = 'RO_ACTIVE_KEPT_PTP';
                
            ELSE IF ((@vc_ptp_status = 'B') AND (@parental_status = 'SRT'))
            
                SET @vc_c121_status = 'SRT_BROKEN_PTP';

            ELSE IF ((@vc_ptp_status = 'K') AND (@parental_status = 'SRT'))
            
                SET @vc_c121_status = 'SRT_KEPT_PTP';                

            ELSE IF ((@vc_ptp_status = 'B') AND (@parental_status = 'EXTERNAL_AGENT'))
            
                SET @vc_c121_status = 'EXTERNAL_AGENT_BROKEN_PTP';

            ELSE IF ((@vc_ptp_status = 'K') AND (@parental_status = 'EXTERNAL_AGENT'))
            
                SET @vc_c121_status = 'EXTERNAL_AGENT_KEPT_PTP';        
                
            UPDATE tbl_account_status
            SET    iC121_Status_ID = ( SELECT iC121_Status_ID
                                       FROM   tbl_c121_status
                                       WHERE  vTitle = @vc_c121_status)
            WHERE  iLoan_Master_ID  = @loanmaster_id;
            
           /* INSERT INTO tbl_account_ptp_arr(iAccount_ptp_arr_id,
                                            iLoan_Master_ID,
                                            iCat3ID,
                                            iAccount_Status_ID,
                                            iHistory_Detail_ID,
                                            iLoan_Product_ID,
                                            iproduct_id
                                            )
            VALUES( NEXT VALUE FOR seq_account_ptp_arr,    -- iAccount_ptp_arr_id
                    @loanmaster_id,                 -- iLoan_Master_ID
                    @cat3id,                        -- iCat3ID
                    @vc_iaction_acc_status_ID,      -- iAccount_Status_ID
                    @History_Detail_ID,             -- iHistory_Detail_ID
                    @Loan_Product_ID,               -- iLoan_Product_ID
                    @product_id                     -- iproduct_id
                    );*/
          
            UPDATE tbl_action_history_ptp_arr
            SET    iActive = 0,
                   updated_date = @vc_current_date,
                   KEPT_BRKN_DATE = @vc_current_date,
                   vStatus = (CASE WHEN @vc_ptp_status = 'K' THEN 'Kept'
                                   WHEN @vc_ptp_status = 'B' THEN 'Broken'
                              END)
            WHERE  iAction_hist_ptp_arr_id = @hist_ptp_arr_id;
            
            INSERT INTO tbl_action_history( iaction_history_id,
                                            iloan_master_id,
                                            icampaign_detail_id,
                                            iuser_id,
                                            ddatetime,
                                            iloan_product_id,
                                            tntbs_notes,
                                            vntbs_title,
                                            iaccount_status,
                                            icontact_type_id,
                                            icycle_completed,
                                            icycle_number,
                                            iproduct_id)
            VALUES( NEXT VALUE FOR seq_action_history,  -- iaction_history_id,
                    @loanmaster_id,              -- iloan_master_id,
                    NULL,                        -- icampaign_detail_id,
                    1,                           -- iuser_id,
                    @vc_current_date_time,       -- ddatetime,
                    @Loan_Product_ID,            -- iloan_product_id,
                    @vc_notes,                   -- tntbs_notes,
                    @vc_notes,                    -- vntbs_title,
                    0,                           -- iaccount_status,
                    NULL,                        -- icontact_type_id,
                    NULL,                        -- icycle_completed,
                    NULL,                        -- icycle_number
                    @product_id                  -- iproduct_id
                  ); 

            SELECT @action_history_curr_id = CAST(Current_Value AS INT) FROM sys.Sequences WHERE name='seq_action_history';
            INSERT INTO tbl_action_history_details (iaction_hist_dtl_id,
                                                    ihistory_id,
                                                    iparental_status_id,
                                                    itype_id,
                                                    icat1_id,
                                                    icat2_id,
                                                    icat3_id,
                                                    tcomments,
                                                    vtypeid_sub,
                                                    iaccount_status,
                                                    icampaign_detail_id,
                                                    dletter_date,
                                                    vvalidation_status,
                                                    vvalidation_comments,
                                                    damount,
                                                    vpayment_frequency,
                                                    dpayment_occurrence,
                                                    ddate_from,
                                                    dtime_from,
                                                    ddate_to,
                                                    dtime_to,
                                                    vpayment_method,
                                                    dfollow_date,
                                                    created_date)
            VALUES(NEXT VALUE FOR seq_action_history_details,  -- iaction_hist_dtl_id,
                   @action_history_curr_id,             -- ihistory_id,
                   @Parental_Status_ID,                 -- iparental_status_id,
                   4,                                   -- itype_id,
                   1,                                   -- icat1_id,
                   1,                                   -- icat2_id,
                   1,                                   -- icat3_id,
                   NULL,                                -- tcomments,
                   'PF',                                -- vtypeid_sub,
                   @vc_iaction_acc_status_ID,            -- iaccount_status,
                   NULL,                                -- icampaign_detail_id,
                   NULL,                                -- dletter_date,
                   NULL,                                -- vvalidation_status,
                   NULL,                                -- vvalidation_comments,
                   NULL,                                -- damount,
                   NULL,                                -- vpayment_frequency,
                   NULL,                                -- dpayment_occurrence,
                   NULL,                                -- ddate_from,
                   NULL,                                -- dtime_from,
                   NULL,                                -- ddate_to,
                   NULL,                                -- dtime_to,
                   NULL,                                -- vpayment_method,
                   NULL,
                   getdate());                               -- dfollow_date)
                   
            -- Derive the payment percentage
            SET @vc_ptp_payment_pct = ROUND((@vc_payment_amount/@ptp_amount) * 100,2);
            
            IF (@vc_ptp_payment_pct <= 0)
                BEGIN
                SET @vc_ptp_reporting_status = 'PARTIAL_0';
                END

            ELSE IF ((@vc_ptp_payment_pct > 0) AND(@vc_ptp_payment_pct < 50))
                BEGIN
                SET @vc_ptp_reporting_status = 'PARTIAL_0_50';
                END

            ELSE IF ( (@vc_ptp_payment_pct >= 50) AND (@vc_ptp_payment_pct < 75))
                BEGIN
                SET @vc_ptp_reporting_status = 'PARTIAL_50_75';
                END

            ELSE IF ( (@vc_ptp_payment_pct >= 75) AND (@vc_ptp_payment_pct < 90))
                BEGIN
                SET @vc_ptp_reporting_status = 'PARTIAL_75_90';
                END

            ELSE IF ( (@vc_ptp_payment_pct >= 90) AND (@vc_ptp_payment_pct < 100))
                BEGIN
                SET @vc_ptp_reporting_status = 'PARTIAL_90_100';
                END

            ELSE IF (@vc_ptp_payment_pct >= 100)
                BEGIN
                SET @vc_ptp_reporting_status = 'FULL_100';
                END;
                
            -- Insert into the PTP reporting TABLE
            INSERT INTO tbl_ptp_reporting ( ptp_reporting_id,
                                            iAction_hist_ptp_arr_id,
                                            iloan_master_id,
                                            iLoan_Product_ID,
                                            iproduct_id,
                                            iCat3ID,
                                            iHistory_Detail_ID,
                                            ptp_date,
                                            dfollowup_date,
                                            ptp_amount,
                                            payment_amount,
                                            ptp_payment_pct,
                                            ptp_status,
                                            reporting_status,
                                            created_date
                                            )
            VALUES( NEXT VALUE FOR seq_ptp_reporting,       -- ptp_reporting_id
                    @hist_ptp_arr_id,               -- iAction_hist_ptp_arr_id
                    @loanmaster_id,                 -- iloan_master_id
                    @Loan_Product_ID,               -- iLoan_Product_ID
                    @product_id,                    -- iproduct_id
                    @cat3id,                        -- iCat3ID
                    @History_Detail_ID,             -- iHistory_Detail_ID
                    @ptp_date,                       -- ptp_date
                    @followup_date,                  -- followup_date
                    @ptp_amount,                     -- ptp_amount
                    @vc_payment_amount,              -- payment_amount
                    @vc_ptp_payment_pct,             -- ptp_payment_pct
                    (CASE WHEN @vc_ptp_status = 'K' THEN 'KEPT_PTP'
                          WHEN @vc_ptp_status = 'B' THEN 'BROKEN_PTP'
                     END),                           -- ptp_status
                    @vc_ptp_reporting_status,        -- reporting_status
                    @vc_current_date                    
                 );

                END        
					FETCH NEXT FROM ptp_cur
					INTO  @hist_ptp_arr_id ,@loanmaster_id,@ptp_date,@followup_date,@History_Detail_ID,@Loan_Product_ID,
                          @cat3id,@Parental_Status_ID,@ptp_amount,@parental_status,@sms_method,@product_id
            END

            CLOSE			ptp_cur
            DEALLOCATE		ptp_cur
			-- END PTP Cursor -------------------------------------------------*

            -- Check if the ARR has been kept or broken -----------------------*
			DECLARE arr_cur CURSOR FAST_FORWARD FOR
                     SELECT a.iAction_hist_ptp_arr_id,
                            a.iloanmaster_id,
                            CONVERT(DATE,a.dDateTime) AS arr_date,
                            a.iHistory_Detail_ID,
                            a.iLoan_Product_ID,
                            a.icat3id,
                            b.iParental_Status_ID,
                            (
                                SELECT SUM(d.dAmount)
                                FROM   tbl_action_hist_payment_arr d
                                WHERE  d.iaction_hist_ptp_arr_id = a.iAction_hist_ptp_arr_id
                                AND    d.dDate                   < DATEADD(DAY,-2,(CONVERT(DATE,c.dFollow_Date)))
                            ) AS arr_amount,
                            CONVERT(DATE,c.dDate) AS arr_instalment_date,
                            CONVERT(DATE,c.dFollow_Date) AS Follow_Date,
                            c.iAction_Hist_Pay_Arr_ID,
                            (
                                SELECT CONVERT(DATE,MAX(dDate))
                                FROM   tbl_action_hist_payment_arr d
                                WHERE  d.iAction_hist_ptp_arr_id = a.iAction_hist_ptp_arr_id
                            ) max_arr_date,
                            d.vTitle AS parental_status,
                            e.iproduct_id
                     FROM   tbl_action_history_ptp_arr a,
                            tbl_account_status b,
                            tbl_action_hist_payment_arr c,
                            tbl_parantial_status d,
                            tbl_loan_master e
                     WHERE  a.iloanmaster_id          = b.iLoan_Master_ID
                     AND    a.iAction_hist_ptp_arr_id = c.iaction_hist_ptp_arr_id
                     AND    b.iParental_Status_ID     = d.iParantial_Status_ID
                     AND    a.iloanmaster_id          = e.iLoan_Master_ID
                     AND    a.icat3id                 = 3
                     AND    a.iActive                 = 1
                     AND    c.iArrangment_Status      = 1
                     AND    c.dDate                   = ( SELECT MIN(f.dDate)
                                                          FROM   tbl_action_hist_payment_arr f
                                                          WHERE  f.iaction_hist_ptp_arr_id = a.iAction_hist_ptp_arr_id
                                                          AND    f.iArrangment_Status      = 1)
                     AND    EXISTS ( SELECT 'X'
                                     FROM   tbl_product p
                                     WHERE  p.iproduct_id = e.iproduct_id
                                     AND    p.vloan_code  = @vc_product_name);
                     --ORDER BY a.iLoan_Product_ID,c.dDate;

			OPEN arr_cur
			FETCH NEXT FROM arr_cur
			INTO   @hist_ptp_arr_id, @loanmaster_id, @arr_date, @History_Detail_ID, @Loan_Product_ID, @cat3id, @Parental_Status_ID,
                   @arr_amount, @arr_instlmnt_date, @followup_date, @hist_Pay_Arr_ID, @max_arr_date, @parental_status, @product_id
           
			WHILE @@FETCH_STATUS = 0
			BEGIN /* Begin of While loop */
            
                SET @vc_arr_status            = NULL;
                SET @vc_notes                 = NULL;
                SET @vc_iaction_acc_status_ID = NULL;
                SET @vc_c121_status           = NULL;

            -- Total payments made to the account
                SELECT @vc_payment_amount = ISNULL(SUM(p.DTOTALAMT),0)
                FROM   tbl_payment_main p
                WHERE  p.iloan_master_id   = @loanmaster_id
                --AND    p.iloan_product_id  = @Loan_Product_ID
                --AND    p.PAYMENT_TYPE not in ('C','D','E')
                AND    CONVERT(DATE,p.dpaymt_date) BETWEEN @arr_date AND @followup_date;
                
                
        IF (@vc_payment_amount >= @arr_amount)
            BEGIN
            SET @vc_arr_status = 'K';
            SET @vc_notes      = 'Kept Arrangement. '+'Period='+CAST(@arr_date AS VARCHAR)+'/'+CAST(@followup_date AS VARCHAR)+', ARR Amount='+CAST(@arr_amount AS VARCHAR)+', Payment Amount='+CAST(@vc_payment_amount AS VARCHAR);

            SELECT @vc_iaction_acc_status_ID = iaction_account_status_ID
            FROM   tbl_action_account_status
            WHERE  vTitle IN ('KEPT_ARRANGEMENT');            
            END

        ELSE IF (@vc_current_date >= @followup_date) AND (@vc_payment_amount < @arr_amount)
            BEGIN
            SET @vc_arr_status = 'B';                    
            SET @vc_notes      = 'Broken Arrangement. '+'Period='+CAST(@arr_date AS VARCHAR)+'/'+CAST(@followup_date AS VARCHAR)+', ARR Amount='+CAST(@arr_amount AS VARCHAR)+', Payment Amount='+CAST(@vc_payment_amount AS VARCHAR);

            SELECT @vc_iaction_acc_status_ID = iaction_account_status_ID
            FROM   tbl_action_account_status
            WHERE  vTitle IN ('BROKEN_ARRANGEMENT');      
            END               

        IF (@vc_arr_status IS NOT NULL)
        BEGIN

            IF ((@vc_arr_status = 'B') AND (@parental_status = 'COLLECTIONS'))
            
                SET @vc_c121_status = 'BROKEN_ARR';

            ELSE IF ((@vc_arr_status = 'K') AND (@parental_status = 'COLLECTIONS'))
            
                SET @vc_c121_status = 'KEPT_ARR';                                 

            ELSE IF ((@vc_arr_status = 'B') AND (@parental_status = 'CRITICAL_ILLNESS'))
            
                SET @vc_c121_status = 'CRITICAL_ILLNESS_BROKEN_ARR';

            ELSE IF ((@vc_arr_status = 'K') AND (@parental_status = 'CRITICAL_ILLNESS'))
            
                SET @vc_c121_status = 'CRITICAL_ILLNESS_KEPT_ARR';

            ELSE IF ((@vc_arr_status = 'B') AND (@parental_status = 'SPECIAL_SITUATIONS'))
            
                SET @vc_c121_status = 'SPECIAL_SITUATIONS_BROKEN_ARR';

            ELSE IF ((@vc_arr_status = 'K') AND (@parental_status = 'SPECIAL_SITUATIONS'))
            
                SET @vc_c121_status = 'SPECIAL_SITUATIONS_KEPT_ARR';                

            ELSE IF ((@vc_arr_status = 'B') AND (@parental_status = 'DECEASED'))
            
                SET @vc_c121_status = 'DECEASED_BROKEN_ARR';

            ELSE IF ((@vc_arr_status = 'K') AND (@parental_status = 'DECEASED'))
            
                SET @vc_c121_status = 'DECEASED_KEPT_ARR';
                
            ELSE IF ((@vc_arr_status = 'B') AND (@parental_status = 'COMPLAINT'))
            
                SET @vc_c121_status = 'COMPLAINT_BROKEN_ARR';

            ELSE IF ((@vc_arr_status = 'K') AND (@parental_status = 'COMPLAINT'))
            
                SET @vc_c121_status = 'COMPLAINT_KEPT_ARR';                

            ELSE IF ((@vc_arr_status = 'B') AND (@parental_status = 'FRAUD'))
            
                SET @vc_c121_status = 'FRAUD_BROKEN_ARR';

            ELSE IF ((@vc_arr_status = 'K') AND (@parental_status = 'FRAUD'))
            
                SET @vc_c121_status = 'FRAUD_KEPT_ARR';

            ELSE IF ((@vc_arr_status = 'B') AND (@parental_status = 'MEDIATION_BOARD'))
            
                SET @vc_c121_status = 'MEDIATION_BOARD_BROKEN_ARR';

            ELSE IF ((@vc_arr_status = 'K') AND (@parental_status = 'MEDIATION_BOARD'))
            
                SET @vc_c121_status = 'MEDIATION_BOARD_KEPT_ARR';

            ELSE IF ((@vc_arr_status = 'B') AND (@parental_status = 'SKIP_TRACE'))
            
                SET @vc_c121_status = 'SKIP_TRACE_BROKEN_ARR';

            ELSE IF ((@vc_arr_status = 'K') AND (@parental_status = 'SKIP_TRACE'))
            
                SET @vc_c121_status = 'SKIP_TRACE_KEPT_ARR';
                
            ELSE IF ((@vc_arr_status = 'B') AND (@parental_status = 'PRE_LEGAL'))
            
                SET @vc_c121_status = 'PRE_LEGAL_BROKEN_ARR';

            ELSE IF ((@vc_arr_status = 'K') AND (@parental_status = 'PRE_LEGAL'))
            
                SET @vc_c121_status = 'PRE_LEGAL_KEPT_ARR';                

            --ELSE IF ((@vc_arr_status = 'B') AND (@parental_status = 'REPOSSESSION'))
            
                --SET @vc_c121_status = 'REPOSSESSION_BROKEN_ARR';

            --ELSE IF ((@vc_arr_status = 'K') AND (@parental_status = 'REPOSSESSION'))
            
                --SET @vc_c121_status = 'REPOSSESSION_KEPT_ARR';             

            ELSE IF ((@vc_ptp_status = 'B') AND (@parental_status = 'RECOVERIES'))
            
                SET @vc_c121_status = 'RECOVERIES_BROKEN_ARR';

            ELSE IF ((@vc_ptp_status = 'K') AND (@parental_status = 'RECOVERIES'))
            
                SET @vc_c121_status = 'RECOVERIES_KEPT_ARR';
                
            ELSE IF ((@vc_ptp_status = 'B') AND (@parental_status = 'EARLY_RECOVERIES'))
            
                SET @vc_c121_status = 'RO_ACTIVE_BROKEN_ARR';

            ELSE IF ((@vc_ptp_status = 'K') AND (@parental_status = 'EARLY_RECOVERIES'))
            
                SET @vc_c121_status = 'RO_ACTIVE_KEPT_ARR';
                
            ELSE IF ((@vc_ptp_status = 'B') AND (@parental_status = 'SRT'))
            
                SET @vc_c121_status = 'SRT_BROKEN_ARR';

            ELSE IF ((@vc_ptp_status = 'K') AND (@parental_status = 'SRT'))
            
                SET @vc_c121_status = 'SRT_KEPT_ARR';
                
            ELSE IF ((@vc_ptp_status = 'B') AND (@parental_status = 'EXTERNAL_AGENT'))
            
                SET @vc_c121_status = 'EXTERNAL_AGENT_BROKEN_ARR';

            ELSE IF ((@vc_ptp_status = 'K') AND (@parental_status = 'EXTERNAL_AGENT'))
            
                SET @vc_c121_status = 'EXTERNAL_AGENT_KEPT_ARR';                
            
            /*INSERT INTO tbl_account_ptp_arr(iAccount_ptp_arr_id,
                                            iLoan_Master_ID,
                                            iCat3ID,
                                            iAccount_Status_ID,
                                            iHistory_Detail_ID,
                                            iLoan_Product_ID,
                                            iproduct_id
                                            )
            VALUES( NEXT VALUE FOR seq_account_ptp_arr,    -- iAccount_ptp_arr_id
                    @loanmaster_id,                 -- iLoan_Master_ID
                    @cat3id,                        -- iCat3ID
                    @vc_iaction_acc_status_ID,      -- iAccount_Status_ID
                    @History_Detail_ID,             -- iHistory_Detail_ID
                    @Loan_Product_ID,               -- iLoan_Product_ID
                    @product_id                     -- iproduct_id
                    );*/
                    
            IF (@vc_arr_status = 'B')
                BEGIN

                -- Update all current and future instalments as broken
                UPDATE tbl_action_hist_payment_arr
                SET    vPayment_Status    = 'Broken',
                       iArrangment_Status = 0
                WHERE  iaction_hist_ptp_arr_id = @hist_ptp_arr_id
                AND    iArrangment_Status = 1
                AND    vPayment_Status IS NULL;

                -- The arrangement is made inactive
                UPDATE tbl_action_history_ptp_arr
                SET    iActive = 0,
                       updated_date = @vc_current_date,
                       KEPT_BRKN_DATE = @vc_current_date,
                       vStatus = 'Broken'
                WHERE  iAction_hist_ptp_arr_id = @hist_ptp_arr_id;
                END

            ELSE IF (@vc_arr_status = 'K')
                BEGIN

                -- Update the current instalment as Kept
                UPDATE tbl_action_hist_payment_arr
                SET    vPayment_Status    = 'Kept',
                       iArrangment_Status = 0
                WHERE  iAction_Hist_Pay_Arr_ID = @hist_Pay_Arr_ID
                AND    vPayment_Status IS NULL;

                END;                     


            IF ( (@vc_arr_status = 'B') OR 
                 ((@vc_arr_status = 'K') AND (@max_arr_date = @arr_instlmnt_date)))  -- Last instalment is kept
                BEGIN

                UPDATE tbl_account_status
                SET    iC121_Status_ID = ( SELECT iC121_Status_ID
                                           FROM   tbl_c121_status
                                           WHERE  vTitle = @vc_c121_status)
                WHERE  iLoan_Master_ID = @loanmaster_id;        



                UPDATE tbl_action_history_ptp_arr
                SET    iActive = 0,
                       updated_date = @vc_current_date,
                       KEPT_BRKN_DATE = @vc_current_date,
                       vStatus = (CASE WHEN @vc_arr_status = 'K' THEN 'Kept'
                                       WHEN @vc_arr_status = 'B' THEN 'Broken'
                                  END)
                WHERE  iAction_hist_ptp_arr_id = @hist_ptp_arr_id;
                END;


            INSERT INTO tbl_action_history(iaction_history_id,
                                           iloan_master_id,
                                           icampaign_detail_id,
                                           iuser_id,
                                           ddatetime,
                                           iloan_product_id,
                                           tntbs_notes,
                                           vntbs_title,
                                           iaccount_status,
                                           icontact_type_id,
                                           icycle_completed,
                                           icycle_number,
                                           iproduct_id)
            VALUES(NEXT VALUE FOR seq_action_history,  -- iaction_history_id,
                   @loanmaster_id,              -- iloan_master_id,
                   NULL,                        -- icampaign_detail_id,
                   1,                           -- iuser_id,
                   @vc_current_date_time,       -- ddatetime,
                   @Loan_Product_ID,            -- iloan_product_id,
                   @vc_notes,                   -- tntbs_notes,
                   @vc_notes,                   -- vntbs_title,
                   0,                           -- iaccount_status,
                   NULL,                        -- icontact_type_id,
                   NULL,                        -- icycle_completed,
                   NULL,                        -- icycle_number
                   @product_id                  -- iproduct_id
                   );                          

            SELECT @action_history_curr_id = CAST(Current_Value AS INT) FROM sys.Sequences WHERE name='seq_action_history';
            
            INSERT INTO tbl_action_history_details(iaction_hist_dtl_id,
                                                   ihistory_id,
                                                   iparental_status_id,
                                                   itype_id,
                                                   icat1_id,
                                                   icat2_id,
                                                   icat3_id,
                                                   tcomments,
                                                   vtypeid_sub,
                                                   iaccount_status,
                                                   icampaign_detail_id,
                                                   dletter_date,
                                                   vvalidation_status,
                                                   vvalidation_comments,
                                                   damount,
                                                   vpayment_frequency,
                                                   dpayment_occurrence,
                                                   ddate_from,
                                                   dtime_from,
                                                   ddate_to,
                                                   dtime_to,
                                                   vpayment_method,
                                                   dfollow_date,
                                                   created_date)
            VALUES(NEXT VALUE FOR seq_action_history_details,  -- iaction_hist_dtl_id,
                  @action_history_curr_id,             -- ihistory_id,
                  @Parental_Status_ID,                 -- iparental_status_id,
                  4,                                   -- itype_id,
                  1,                                   -- icat1_id,
                  1,                                   -- icat2_id,
                  3,                                   -- icat3_id,
                  NULL,                                -- tcomments,
                  'PF',                                -- vtypeid_sub,
                  @vc_iaction_acc_status_ID,           -- iaccount_status,
                  NULL,                                -- icampaign_detail_id,
                  NULL,                                -- dletter_date,
                  NULL,                                -- vvalidation_status,
                  NULL,                                -- vvalidation_comments,
                  NULL,                                -- damount,
                  NULL,                                -- vpayment_frequency,
                  NULL,                                -- dpayment_occurrence,
                  NULL,                                -- ddate_from,
                  NULL,                                -- dtime_from,
                  NULL,                                -- ddate_to,
                  NULL,                                -- dtime_to,
                  NULL,                                -- vpayment_method,
                  NULL,                                -- dfollow_date
                  getdate());

                
           END     
            FETCH NEXT FROM arr_cur
			INTO   @hist_ptp_arr_id, @loanmaster_id, @arr_date, @History_Detail_ID, @Loan_Product_ID, @cat3id, @Parental_Status_ID,
                   @arr_amount, @arr_instlmnt_date, @followup_date, @hist_Pay_Arr_ID, @max_arr_date, @parental_status, @product_id
        END

        CLOSE			arr_cur
        DEALLOCATE		arr_cur
			-- END ARR Cursor -------------------------------------------------*  

    -- For FRAUD,DECEASED accounts, 
    -- when the arrears becomes zero, change the parental status to Collections and C121 Status to NRPC
			DECLARE pqa_cur CURSOR FAST_FORWARD FOR
                    SELECT   a.iloanmaster_id,
                             a.iloan_product_id,
                             e.vtitle AS c121_status,
                             b.iparental_status_id,
                             f.iproduct_id,
                             c.vtitle AS parental_status
                     FROM    tbl_delinquency a
                     INNER JOIN tbl_account_status b on b.iloan_master_id = a.iloanmaster_id 
                     INNER JOIN tbl_parantial_status c on c.iparantial_status_id = b.iparental_status_id
                     INNER JOIN tbl_grand_parantial_status d on d.iid = b.igrand_parantial_status_id
                     INNER JOIN tbl_loan_master f on f.iLoan_Master_ID = a.iloanmaster_id
                     LEFT OUTER JOIN tbl_c121_status e on e.ic121_status_id = b.ic121_status_id 

                     WHERE   a.iactioned                     = 1
                     AND     c.iPublish                      = 1
                     AND     d.vStatus                       = 'ACTIVE'    
                     AND     d.iPublish                      = 1
                     AND     c.vtitle                        IN ('CRITICAL_ILLNESS','FRAUD','DECEASED','INSURANCE_DECEASED','INSURANCE_CRITICAL_ILLNESS','WAIVE_OFF','COMPLAINT','SPECIAL_SITUATIONS','SKIP_TRACE','SRT')
                     AND     ISNULL(a.IARR_ADV_DAYS,0)         <= 0
                     --AND     ISNULL(a.IARR_ADV_AMT,0)          <= 0
                     AND     EXISTS ( SELECT 'X'
                                      FROM   tbl_product p
                                      WHERE  p.iproduct_id = f.iproduct_id
                                      AND    p.vloan_code  = @vc_product_name);
                                      
			OPEN pqa_cur
			FETCH NEXT FROM pqa_cur
			INTO @cncl_Loan_Master_ID,@cncl_Loan_Product_ID,@cncl_c121_status,@cncl_Parental_Status_Id,@cncl_product_id,@cncl_parental_status 

			WHILE @@FETCH_STATUS = 0
			BEGIN /* Begin of While loop */ 
            
            SET @vc_notes           = NULL;
            SET @vc_iaction_acc_status_ID = NULL;
            
            UPDATE tbl_account_status
            SET    iC121_Status_ID     = 1,
                   iParental_Status_id = ( SELECT iParantial_Status_Id
                                           FROM   tbl_parantial_status
                                           WHERE  vTitle = 'COLLECTIONS')
            WHERE  iLoan_Master_ID  = @cncl_Loan_Master_ID;

            SET @vc_notes = 'Parental Status Changed From '+@cncl_parental_status +' To COLLECTIONS and Collect121 Status Changed from '+@cncl_c121_status+' to NRPC. This is due to account being non delinquent';
 
             INSERT INTO tbl_action_history( iaction_history_id,
                                             iloan_master_id,
                                             icampaign_detail_id,
                                             iuser_id,
                                             ddatetime,
                                             iloan_product_id,
                                             tntbs_notes,
                                             vntbs_title,
                                             iaccount_status,
                                             icontact_type_id,
                                             icycle_completed,
                                             icycle_number,
                                             iproduct_id)
            VALUES( NEXT VALUE FOR seq_action_history,  -- iaction_history_id,
                    @cncl_Loan_Master_ID,      -- iloan_master_id,
                    NULL,                        -- icampaign_detail_id,
                    1,                           -- iuser_id,
                    getdate(),                     -- ddatetime,
                    @cncl_Loan_Product_ID,    -- iloan_product_id,
                    @vc_notes,                    -- tntbs_notes,
                    @vc_notes,                    -- vntbs_title,
                    0,                           -- iaccount_status,
                    NULL,                        -- icontact_type_id,
                    NULL,                        -- icycle_completed,
                    NULL,                        -- icycle_number
                    @cncl_product_id);        -- iproduct_id                        



                    SELECT @vc_iaction_acc_status_ID = iaction_account_status_ID
                    FROM   tbl_action_account_status
                    WHERE  vTitle = 'NRPC';

                    SELECT @action_history_curr_id = CAST(Current_Value AS INT) FROM sys.Sequences WHERE name='seq_action_history';
                    INSERT INTO tbl_action_history_details (iaction_hist_dtl_id,
                                                            ihistory_id,
                                                            iparental_status_id,
                                                            itype_id,
                                                            icat1_id,
                                                            icat2_id,
                                                            icat3_id,
                                                            tcomments,
                                                            vtypeid_sub,
                                                            iaccount_status,
                                                            icampaign_detail_id,
                                                            dletter_date,
                                                            vvalidation_status,
                                                            vvalidation_comments,
                                                            damount,
                                                            vpayment_frequency,
                                                            dpayment_occurrence,
                                                            ddate_from,
                                                            dtime_from,
                                                            ddate_to,
                                                            dtime_to,
                                                            vpayment_method,
                                                            dfollow_date,
                                                            created_date)
                    VALUES(NEXT VALUE FOR seq_action_history_details,  -- iaction_hist_dtl_id,
                           @action_history_curr_id,          -- ihistory_id,
                           @cncl_Parental_Status_Id,          -- iparental_status_id,
                           1,                                   -- itype_id,   ????
                           1,                                   -- icat1_id,
                           1,                                   -- icat2_id,
                           1,                                   -- icat3_id,
                           NULL,                                -- tcomments,
                           'PF',                                -- vtypeid_sub,
                           @vc_iaction_acc_status_ID,            -- iaccount_status,
                           NULL,                                -- icampaign_detail_id,
                           NULL,                                -- dletter_date,
                           NULL,                                -- vvalidation_status,
                           NULL,                                -- vvalidation_comments,
                           NULL,                                -- damount,
                           NULL,                                -- vpayment_frequency,
                           NULL,                                -- dpayment_occurrence,
                           NULL,                                -- ddate_from,
                           NULL,                                -- dtime_from,
                           NULL,                                -- ddate_to,
                           NULL,                                -- dtime_to,
                           NULL,                                -- vpayment_method,
                           NULL,
                           getdate()); 
                           
            FETCH NEXT FROM pqa_cur
			INTO  @cncl_Loan_Master_ID,@cncl_Loan_Product_ID,@cncl_c121_status,@cncl_Parental_Status_Id,@cncl_product_id,@cncl_parental_status
            END

            CLOSE			pqa_cur
            DEALLOCATE		pqa_cur 
        -- End PQA Cursor -------------------------------------------------*
            
    -- When the DPD becomes zero, set the account_unpproved flag to NULL
    UPDATE tbl_account_status
    SET    account_unapproved = NULL
    WHERE  account_unapproved = 'Y'
    AND    EXISTS ( SELECT 'X'
                    FROM   tbl_loan_master b,
                           tbl_delinquency c,
                           tbl_product d
                    WHERE  b.iLoan_Master_ID = c.iLoanMaster_ID
                    AND    b.iLoan_Master_ID = tbl_account_status.iLoan_Master_ID
                    AND    b.iproduct_id     = d.iproduct_id
                    AND    d.vloan_code      = @vc_product_name
                    AND    c.iActioned       = 1
                    AND    ISNULL(c.IARR_ADV_DAYS,0)   <= 0
                    AND    ISNULL(c.IARR_ADV_AMT,0)   <= 0); 
                    
    -- When the DPD becomes zero, set the DPD Zero Broken_ptp_arr acounts to NRPC
    UPDATE tbl_account_status
    SET    IC121_STATUS_ID = 1 --set to NRPC
    WHERE  IC121_STATUS_ID IN (14,48) --C121_Status IN ('BROKEN_ARR','BROKEN_PTP')
    AND    IPARENTAL_STATUS_ID = 1 -- Collection
    AND    EXISTS ( SELECT 'X'
                    FROM   tbl_loan_master b,
                           tbl_delinquency c,
                           tbl_product d
                    WHERE  b.iLoan_Master_ID = c.iLoanMaster_ID
                    AND    b.iLoan_Master_ID = tbl_account_status.iLoan_Master_ID
                    AND    b.iproduct_id     = d.iproduct_id
                    AND    d.vloan_code      = @vc_product_name
                    AND    c.iActioned       = 1
                    AND    ISNULL(c.IARR_ADV_DAYS,0) <= 0);                    
/*
   --When the account is in a legal code,set PARENTAL to LEGAL and C121 to LEGAL_APPROVED
			DECLARE legal_cur CURSOR FAST_FORWARD FOR
            SELECT   lm.ILOAN_MASTER_ID,
                     lm.iloan_product_id,
                     ISNULL(e.vtitle,'NULL') AS c121_status,
                     a.iparental_status_id,
                     lm.iproduct_id,
                     c.vtitle AS parental_status
            FROM tbl_account_status a
            INNER JOIN tbl_loan_master lm on lm.iLoan_Master_ID = a.iloan_master_id
            LEFT OUTER JOIN tbl_c121_status e on e.ic121_status_id = a.ic121_status_id
            LEFT OUTER JOIN tbl_parantial_status c on c.iparantial_status_id = a.iparental_status_id
            WHERE  ISNULL(c.vtitle,'N') NOT IN ('LEGAL')
			AND    ISNULL(e.vtitle,'N') NOT IN ('LEGAL_APPROVED','LEGAL_PENDING_APPROVAL','LEGAL_UNAPPROVED')
            AND    lm.FOLLOWUP_OFF in ('LEG001','LEG002','LEG003','LEG004','LEG005','LEG006','LEG007','LEG008','LEG009','LEG010','LEG015','LEG000')
            AND     EXISTS ( SELECT 'X'
                             FROM   tbl_product p
                             WHERE  p.iproduct_id = lm.iproduct_id
                             AND    p.vloan_code  = @vc_product_name);
                                      
			OPEN legal_cur
			FETCH NEXT FROM legal_cur
			INTO @cncl_Loan_Master_ID,@cncl_Loan_Product_ID,@cncl_c121_status,@cncl_Parental_Status_Id,@cncl_product_id,@cncl_parental_status 

			WHILE @@FETCH_STATUS = 0
			BEGIN 
            
            SET @vc_notes           = NULL;
            SET @vc_iaction_acc_status_ID = NULL;
            
            UPDATE tbl_account_status
            SET    IPARENTAL_STATUS_ID = (SELECT iParantial_Status_Id FROM tbl_parantial_status WHERE vTitle = 'LEGAL'),
                   iC121_Status_ID = (SELECT IC121_STATUS_ID FROM TBL_C121_STATUS WHERE VTITLE = 'LEGAL_APPROVED'),
                   TO_LEGAL = GETDATE()
            WHERE  iLoan_Master_ID  = @cncl_Loan_Master_ID;

            SET @vc_notes = 'Parental Status Changed From '+@cncl_parental_status +' To LEGAL and Collect121 Status Changed from '+@cncl_c121_status+' to LEGAL_APPROVED. This is due to Legal code tag';
 
             INSERT INTO tbl_action_history( iaction_history_id,
                                             iloan_master_id,
                                             icampaign_detail_id,
                                             iuser_id,
                                             ddatetime,
                                             iloan_product_id,
                                             tntbs_notes,
                                             vntbs_title,
                                             iaccount_status,
                                             icontact_type_id,
                                             icycle_completed,
                                             icycle_number,
                                             iproduct_id)
            VALUES( NEXT VALUE FOR seq_action_history,  -- iaction_history_id,
                    @cncl_Loan_Master_ID,      -- iloan_master_id,
                    NULL,                      -- icampaign_detail_id,
                    1,                         -- iuser_id,
                    getdate(),                 -- ddatetime,
                    @cncl_Loan_Product_ID,     -- iloan_product_id,
                    @vc_notes,                 -- tntbs_notes,
                    @vc_notes,                 -- vntbs_title,
                    0,                         -- iaccount_status,
                    NULL,                      -- icontact_type_id,
                    NULL,                      -- icycle_completed,
                    NULL,                      -- icycle_number
                    @cncl_product_id);         -- iproduct_id                        


                    --SELECT @vc_iaction_acc_status_ID = iaction_account_status_ID
                    --FROM   tbl_action_account_status
                    --WHERE  vTitle = 'NRPC';

                    SELECT @action_history_curr_id = CAST(Current_Value AS INT) FROM sys.Sequences WHERE name='seq_action_history';
                    INSERT INTO tbl_action_history_details (iaction_hist_dtl_id,
                                                            ihistory_id,
                                                            iparental_status_id,
                                                            itype_id,
                                                            icat1_id,
                                                            icat2_id,
                                                            icat3_id,
                                                            tcomments,
                                                            vtypeid_sub,
                                                            iaccount_status,
                                                            icampaign_detail_id,
                                                            dletter_date,
                                                            vvalidation_status,
                                                            vvalidation_comments,
                                                            damount,
                                                            vpayment_frequency,
                                                            dpayment_occurrence,
                                                            ddate_from,
                                                            dtime_from,
                                                            ddate_to,
                                                            dtime_to,
                                                            vpayment_method,
                                                            dfollow_date,
                                                            created_date)
                    VALUES(NEXT VALUE FOR seq_action_history_details,  -- iaction_hist_dtl_id,
                           @action_history_curr_id,          -- ihistory_id,
                           @cncl_Parental_Status_Id,          -- iparental_status_id,
                           1,                                   -- itype_id,   ????
                           1,                                   -- icat1_id,
                           1,                                   -- icat2_id,
                           1,                                   -- icat3_id,
                           NULL,                                -- tcomments,
                           'PF',                                -- vtypeid_sub,
                           @vc_iaction_acc_status_ID,            -- iaccount_status,
                           NULL,                                -- icampaign_detail_id,
                           NULL,                                -- dletter_date,
                           NULL,                                -- vvalidation_status,
                           NULL,                                -- vvalidation_comments,
                           NULL,                                -- damount,
                           NULL,                                -- vpayment_frequency,
                           NULL,                                -- dpayment_occurrence,
                           NULL,                                -- ddate_from,
                           NULL,                                -- dtime_from,
                           NULL,                                -- ddate_to,
                           NULL,                                -- dtime_to,
                           NULL,                                -- vpayment_method,
                           NULL,
                           getdate()); 
                           
            FETCH NEXT FROM legal_cur
			INTO  @cncl_Loan_Master_ID,@cncl_Loan_Product_ID,@cncl_c121_status,@cncl_Parental_Status_Id,@cncl_product_id,@cncl_parental_status
            END

            CLOSE			legal_cur
            DEALLOCATE		legal_cur 
        -- End legal assign Cursor -------------------------------------------------*
*/

   --When the account LEGAL_UNAPPROVED,set PARENTAL to COLLECTION and C121 to NRPC
			DECLARE legal_rem_cur CURSOR FAST_FORWARD FOR
            SELECT   lm.ILOAN_MASTER_ID,
                     lm.iloan_product_id,
                     ISNULL(e.vtitle,'NULL') AS c121_status,
                     a.iparental_status_id,
                     lm.iproduct_id,
                     c.vtitle AS parental_status
            FROM tbl_account_status a
            INNER JOIN tbl_loan_master lm on lm.iLoan_Master_ID = a.iloan_master_id
            LEFT OUTER JOIN tbl_c121_status e on e.ic121_status_id = a.ic121_status_id
            LEFT OUTER JOIN tbl_parantial_status c on c.iparantial_status_id = a.iparental_status_id
            WHERE  ISNULL(e.vtitle,'N') IN ('LEGAL_UNAPPROVED')
            --AND    ISNULL(lm.FOLLOWUP_OFF,'N') NOT IN ('LEG001','LEG002','LEG003','LEG004','LEG005','LEG006','LEG007','LEG008','LEG009','LEG010','LEG015','LEG000')
            AND     EXISTS ( SELECT 'X'
                             FROM   tbl_product p
                             WHERE  p.iproduct_id = lm.iproduct_id
                             AND    p.vloan_code  = @vc_product_name);
                                      
			OPEN legal_rem_cur
			FETCH NEXT FROM legal_rem_cur
			INTO @cncl_Loan_Master_ID,@cncl_Loan_Product_ID,@cncl_c121_status,@cncl_Parental_Status_Id,@cncl_product_id,@cncl_parental_status 

			WHILE @@FETCH_STATUS = 0
			BEGIN 
            
            SET @vc_notes           = NULL;
            SET @vc_iaction_acc_status_ID = NULL;
            
            UPDATE tbl_account_status
            SET    IPARENTAL_STATUS_ID = (SELECT iParantial_Status_Id FROM tbl_parantial_status WHERE vTitle = 'COLLECTIONS'),
                   iC121_Status_ID = (SELECT IC121_STATUS_ID FROM TBL_C121_STATUS WHERE VTITLE = 'NRPC'),
                   BACK_FROM_LEGAL = GETDATE()
            WHERE  iLoan_Master_ID  = @cncl_Loan_Master_ID;

            SET @vc_notes = 'Parental Status Changed From '+@cncl_parental_status +' To COLLECTIONS and Collect121 Status Changed from '+@cncl_c121_status+' to NRPC. This is due to Legal Unapproved';
 
             INSERT INTO tbl_action_history( iaction_history_id,
                                             iloan_master_id,
                                             icampaign_detail_id,
                                             iuser_id,
                                             ddatetime,
                                             iloan_product_id,
                                             tntbs_notes,
                                             vntbs_title,
                                             iaccount_status,
                                             icontact_type_id,
                                             icycle_completed,
                                             icycle_number,
                                             iproduct_id)
            VALUES( NEXT VALUE FOR seq_action_history,  -- iaction_history_id,
                    @cncl_Loan_Master_ID,      -- iloan_master_id,
                    NULL,                      -- icampaign_detail_id,
                    1,                         -- iuser_id,
                    getdate(),                 -- ddatetime,
                    @cncl_Loan_Product_ID,     -- iloan_product_id,
                    @vc_notes,                 -- tntbs_notes,
                    @vc_notes,                 -- vntbs_title,
                    0,                         -- iaccount_status,
                    NULL,                      -- icontact_type_id,
                    NULL,                      -- icycle_completed,
                    NULL,                      -- icycle_number
                    @cncl_product_id);         -- iproduct_id                        


                    --SELECT @vc_iaction_acc_status_ID = iaction_account_status_ID
                    --FROM   tbl_action_account_status
                    --WHERE  vTitle = 'NRPC';

                    SELECT @action_history_curr_id = CAST(Current_Value AS INT) FROM sys.Sequences WHERE name='seq_action_history';
                    INSERT INTO tbl_action_history_details (iaction_hist_dtl_id,
                                                            ihistory_id,
                                                            iparental_status_id,
                                                            itype_id,
                                                            icat1_id,
                                                            icat2_id,
                                                            icat3_id,
                                                            tcomments,
                                                            vtypeid_sub,
                                                            iaccount_status,
                                                            icampaign_detail_id,
                                                            dletter_date,
                                                            vvalidation_status,
                                                            vvalidation_comments,
                                                            damount,
                                                            vpayment_frequency,
                                                            dpayment_occurrence,
                                                            ddate_from,
                                                            dtime_from,
                                                            ddate_to,
                                                            dtime_to,
                                                            vpayment_method,
                                                            dfollow_date,
                                                            created_date)
                    VALUES(NEXT VALUE FOR seq_action_history_details,  -- iaction_hist_dtl_id,
                           @action_history_curr_id,          -- ihistory_id,
                           @cncl_Parental_Status_Id,          -- iparental_status_id,
                           1,                                   -- itype_id,   ????
                           1,                                   -- icat1_id,
                           1,                                   -- icat2_id,
                           1,                                   -- icat3_id,
                           NULL,                                -- tcomments,
                           'PF',                                -- vtypeid_sub,
                           @vc_iaction_acc_status_ID,            -- iaccount_status,
                           NULL,                                -- icampaign_detail_id,
                           NULL,                                -- dletter_date,
                           NULL,                                -- vvalidation_status,
                           NULL,                                -- vvalidation_comments,
                           NULL,                                -- damount,
                           NULL,                                -- vpayment_frequency,
                           NULL,                                -- dpayment_occurrence,
                           NULL,                                -- ddate_from,
                           NULL,                                -- dtime_from,
                           NULL,                                -- ddate_to,
                           NULL,                                -- dtime_to,
                           NULL,                                -- vpayment_method,
                           NULL,
                           getdate()); 
                           
            FETCH NEXT FROM legal_rem_cur
			INTO  @cncl_Loan_Master_ID,@cncl_Loan_Product_ID,@cncl_c121_status,@cncl_Parental_Status_Id,@cncl_product_id,@cncl_parental_status
            END

            CLOSE			legal_rem_cur
            DEALLOCATE		legal_rem_cur 
        -- End legal remove Cursor -------------------------------------------------*
      
   --When the account is in a legal_tranfer_form approved,set PARENTAL to RECOVERIES and C121 to LEGAL_PENDING_APPROVAL
			DECLARE legal_screen_cur CURSOR FAST_FORWARD FOR
            SELECT   lm.ILOAN_MASTER_ID,
                     lm.iloan_product_id,
                     ISNULL(e.vtitle,'NULL') AS c121_status,
                     a.iparental_status_id,
                     lm.iproduct_id,
                     c.vtitle AS parental_status
            FROM tbl_account_status a
            INNER JOIN tbl_loan_master lm on lm.iLoan_Master_ID = a.iloan_master_id
            LEFT OUTER JOIN tbl_c121_status e on e.ic121_status_id = a.ic121_status_id
            LEFT OUTER JOIN tbl_parantial_status c on c.iparantial_status_id = a.iparental_status_id
            WHERE  ISNULL(c.vtitle,'N') NOT IN ('LEGAL')
			AND    ISNULL(e.vtitle,'N') NOT IN ('LEGAL_APPROVED','LEGAL_PENDING_APPROVAL','LEGAL_UNAPPROVED','LEGAL_CHECKLIST_COMPLETE','LEGAL_CHECKLIST_PENDING','LEGAL_ACCEPTED','LEGAL_DOCUMENT_PENDING','LEGAL_DOCUMENT_COMPLETE')
            AND     EXISTS ( SELECT 'X'
                             FROM   tbl_product p
                             WHERE  p.iproduct_id = lm.iproduct_id
                             AND    p.vloan_code  = @vc_product_name)
            AND     EXISTS (SELECT 'X' 
                            FROM TBL_CASE C
                            INNER JOIN TBL_WK_WORKFLOW_MAIN A on A.IID = C.IWFMAINID
                            INNER JOIN TBL_WK_TYPE_FILE AF on AF.IID=A.IWK_TYPE_FILE_ID
                            WHERE c.ILOAN_MASTER_ID = lm.ILOAN_MASTER_ID
                            AND C.VSTATUS='APPROVED' 
                            AND AF.VTYPE='LEGAL' 
                            AND AF.VFILENAME='legal_tranfer_form')
                            
            AND ISNULL(a.BACK_FROM_LEGAL,CONVERT(Datetime,'01-01-1901 00:00:00', 120)) < (SELECT MAX(c.DUPDATEDATE) 
                                    FROM TBL_CASE C
                                    INNER JOIN TBL_WK_WORKFLOW_MAIN A on A.IID = C.IWFMAINID
                                    INNER JOIN TBL_WK_TYPE_FILE AF on AF.IID=A.IWK_TYPE_FILE_ID
                                    WHERE c.ILOAN_MASTER_ID = lm.ILOAN_MASTER_ID
                                    AND C.VSTATUS='APPROVED' 
                                    AND AF.VTYPE='LEGAL' 
                                    AND AF.VFILENAME='legal_tranfer_form');
                                      
			OPEN legal_screen_cur
			FETCH NEXT FROM legal_screen_cur
			INTO @cncl_Loan_Master_ID,@cncl_Loan_Product_ID,@cncl_c121_status,@cncl_Parental_Status_Id,@cncl_product_id,@cncl_parental_status 

			WHILE @@FETCH_STATUS = 0
			BEGIN /* Begin of While loop */ 
            
            SET @vc_notes           = NULL;
            SET @vc_iaction_acc_status_ID = NULL;
            
            UPDATE tbl_account_status
            SET    IPARENTAL_STATUS_ID = 8,--RECOVERIES
                   iC121_Status_ID = (SELECT IC121_STATUS_ID FROM TBL_C121_STATUS WHERE VTITLE = 'LEGAL_PENDING_APPROVAL')
            WHERE  iLoan_Master_ID  = @cncl_Loan_Master_ID;

            SET @vc_notes = 'Parental Status Changed From '+@cncl_parental_status +' To RECOVERIES and Collect121 Status Changed from '+@cncl_c121_status+' to LEGAL_PENDING_APPROVAL. This is due to legal_tranfer_form approved';
 
             INSERT INTO tbl_action_history( iaction_history_id,
                                             iloan_master_id,
                                             icampaign_detail_id,
                                             iuser_id,
                                             ddatetime,
                                             iloan_product_id,
                                             tntbs_notes,
                                             vntbs_title,
                                             iaccount_status,
                                             icontact_type_id,
                                             icycle_completed,
                                             icycle_number,
                                             iproduct_id)
            VALUES( NEXT VALUE FOR seq_action_history,  -- iaction_history_id,
                    @cncl_Loan_Master_ID,      -- iloan_master_id,
                    NULL,                      -- icampaign_detail_id,
                    1,                         -- iuser_id,
                    getdate(),                 -- ddatetime,
                    @cncl_Loan_Product_ID,     -- iloan_product_id,
                    @vc_notes,                 -- tntbs_notes,
                    @vc_notes,                 -- vntbs_title,
                    0,                         -- iaccount_status,
                    NULL,                      -- icontact_type_id,
                    NULL,                      -- icycle_completed,
                    NULL,                      -- icycle_number
                    @cncl_product_id);         -- iproduct_id                        


                    --SELECT @vc_iaction_acc_status_ID = iaction_account_status_ID
                    --FROM   tbl_action_account_status
                    --WHERE  vTitle = 'NRPC';

                    SELECT @action_history_curr_id = CAST(Current_Value AS INT) FROM sys.Sequences WHERE name='seq_action_history';
                    INSERT INTO tbl_action_history_details (iaction_hist_dtl_id,
                                                            ihistory_id,
                                                            iparental_status_id,
                                                            itype_id,
                                                            icat1_id,
                                                            icat2_id,
                                                            icat3_id,
                                                            tcomments,
                                                            vtypeid_sub,
                                                            iaccount_status,
                                                            icampaign_detail_id,
                                                            dletter_date,
                                                            vvalidation_status,
                                                            vvalidation_comments,
                                                            damount,
                                                            vpayment_frequency,
                                                            dpayment_occurrence,
                                                            ddate_from,
                                                            dtime_from,
                                                            ddate_to,
                                                            dtime_to,
                                                            vpayment_method,
                                                            dfollow_date,
                                                            created_date)
                    VALUES(NEXT VALUE FOR seq_action_history_details,  -- iaction_hist_dtl_id,
                           @action_history_curr_id,          -- ihistory_id,
                           @cncl_Parental_Status_Id,          -- iparental_status_id,
                           1,                                   -- itype_id,   ????
                           1,                                   -- icat1_id,
                           1,                                   -- icat2_id,
                           1,                                   -- icat3_id,
                           NULL,                                -- tcomments,
                           'PF',                                -- vtypeid_sub,
                           @vc_iaction_acc_status_ID,            -- iaccount_status,
                           NULL,                                -- icampaign_detail_id,
                           NULL,                                -- dletter_date,
                           NULL,                                -- vvalidation_status,
                           NULL,                                -- vvalidation_comments,
                           NULL,                                -- damount,
                           NULL,                                -- vpayment_frequency,
                           NULL,                                -- dpayment_occurrence,
                           NULL,                                -- ddate_from,
                           NULL,                                -- dtime_from,
                           NULL,                                -- ddate_to,
                           NULL,                                -- dtime_to,
                           NULL,                                -- vpayment_method,
                           NULL,
                           getdate()); 
                           
            FETCH NEXT FROM legal_screen_cur
			INTO  @cncl_Loan_Master_ID,@cncl_Loan_Product_ID,@cncl_c121_status,@cncl_Parental_Status_Id,@cncl_product_id,@cncl_parental_status
            END

            CLOSE			legal_screen_cur
            DEALLOCATE		legal_screen_cur 
        -- End legal screen assign Cursor -------------------------------------------------*

--when the external agent tag comes, change the parental status to EXTERNAL_AGENT and C121 Status to EXTERNAL_AGENT_APPROVED
			DECLARE ext_agent_in CURSOR FAST_FORWARD FOR
                     SELECT  a.iloanmaster_id,
                             a.iloan_product_id,
                             e.vtitle AS c121_status,
                             b.iparental_status_id,
                             f.iproduct_id,
                             c.vtitle AS parental_status
                     FROM    tbl_delinquency a
                             inner join tbl_account_status b on b.iloan_master_id = a.iloanmaster_id
                             inner join tbl_loan_master f on f.iLoan_Master_ID = a.iloanmaster_id
                             left outer join tbl_parantial_status c on c.iparantial_status_id = b.iparental_status_id
                             left outer join tbl_c121_status e on e.ic121_status_id = b.ic121_status_id
                             
                     WHERE   a.iactioned                     = 1
                     AND     c.iPublish                      = 1
                     AND     c.vtitle                       NOT IN ('EXTERNAL_AGENT')
                     AND     (ISNULL(e.vtitle,'N') NOT IN ('EXTERNAL_AGENT_UNAPPROVED','EXTERNAL_AGENT_ACCOUNT_RETURN','ASSIGN_TO_EXTERNAL_AGENT',
                                                           'NEW_TO_EXTERNAL_AGENT_SCREENING','EXTERNAL_AGENT_PENDING_APPROVAL'))
                     AND     f.VCORE_OWNER is not null                    
                     AND     EXISTS ( SELECT 'X'
                                      FROM   tbl_product p
                                      WHERE  p.iproduct_id = f.iproduct_id
                                      AND    p.vloan_code  = @vc_product_name);
                                      
			OPEN ext_agent_in
			FETCH NEXT FROM ext_agent_in
			INTO @cncl_Loan_Master_ID,@cncl_Loan_Product_ID,@cncl_c121_status,@cncl_Parental_Status_Id,@cncl_product_id,@cncl_parental_status 

			WHILE @@FETCH_STATUS = 0
			BEGIN /* Begin of While loop */ 
            
            SET @vc_notes           = NULL;
            SET @vc_iaction_acc_status_ID = NULL;
            
            UPDATE tbl_account_status
            SET    IPARENTAL_STATUS_ID = 6, --set to EXTERNAL_AGENT
                   iC121_Status_ID = 110 --set to EXTERNAL_AGENT_APPROVED
            WHERE  iLoan_Master_ID  = @cncl_Loan_Master_ID;

            SET @vc_notes = 'Parental Status Changed From '+@cncl_parental_status +' To EXTERNAL_AGENT and Collect121 Status Changed from '+@cncl_c121_status+' to EXTERNAL_AGENT_APPROVED.';
 
             INSERT INTO tbl_action_history( iaction_history_id,
                                             iloan_master_id,
                                             icampaign_detail_id,
                                             iuser_id,
                                             ddatetime,
                                             iloan_product_id,
                                             tntbs_notes,
                                             vntbs_title,
                                             iaccount_status,
                                             icontact_type_id,
                                             icycle_completed,
                                             icycle_number,
                                             iproduct_id)
            VALUES( NEXT VALUE FOR seq_action_history,  -- iaction_history_id,
                    @cncl_Loan_Master_ID,      -- iloan_master_id,
                    NULL,                      -- icampaign_detail_id,
                    1,                         -- iuser_id,
                    getdate(),                 -- ddatetime,
                    @cncl_Loan_Product_ID,     -- iloan_product_id,
                    @vc_notes,                 -- tntbs_notes,
                    @vc_notes,                 -- vntbs_title,
                    0,                         -- iaccount_status,
                    NULL,                      -- icontact_type_id,
                    NULL,                      -- icycle_completed,
                    NULL,                      -- icycle_number
                    @cncl_product_id);         -- iproduct_id                        


                    --SELECT @vc_iaction_acc_status_ID = iaction_account_status_ID
                    --FROM   tbl_action_account_status
                    --WHERE  vTitle = 'NRPC';

                    SELECT @action_history_curr_id = CAST(Current_Value AS INT) FROM sys.Sequences WHERE name='seq_action_history';
                    INSERT INTO tbl_action_history_details (iaction_hist_dtl_id,
                                                            ihistory_id,
                                                            iparental_status_id,
                                                            itype_id,
                                                            icat1_id,
                                                            icat2_id,
                                                            icat3_id,
                                                            tcomments,
                                                            vtypeid_sub,
                                                            iaccount_status,
                                                            icampaign_detail_id,
                                                            dletter_date,
                                                            vvalidation_status,
                                                            vvalidation_comments,
                                                            damount,
                                                            vpayment_frequency,
                                                            dpayment_occurrence,
                                                            ddate_from,
                                                            dtime_from,
                                                            ddate_to,
                                                            dtime_to,
                                                            vpayment_method,
                                                            dfollow_date,
                                                            created_date)
                    VALUES(NEXT VALUE FOR seq_action_history_details,  -- iaction_hist_dtl_id,
                           @action_history_curr_id,          -- ihistory_id,
                           @cncl_Parental_Status_Id,          -- iparental_status_id,
                           1,                                   -- itype_id,   ????
                           1,                                   -- icat1_id,
                           1,                                   -- icat2_id,
                           1,                                   -- icat3_id,
                           NULL,                                -- tcomments,
                           'PF',                                -- vtypeid_sub,
                           @vc_iaction_acc_status_ID,            -- iaccount_status,
                           NULL,                                -- icampaign_detail_id,
                           NULL,                                -- dletter_date,
                           NULL,                                -- vvalidation_status,
                           NULL,                                -- vvalidation_comments,
                           NULL,                                -- damount,
                           NULL,                                -- vpayment_frequency,
                           NULL,                                -- dpayment_occurrence,
                           NULL,                                -- ddate_from,
                           NULL,                                -- dtime_from,
                           NULL,                                -- ddate_to,
                           NULL,                                -- dtime_to,
                           NULL,                                -- vpayment_method,
                           NULL,
                           getdate()); 
                           
            FETCH NEXT FROM ext_agent_in
			INTO  @cncl_Loan_Master_ID,@cncl_Loan_Product_ID,@cncl_c121_status,@cncl_Parental_Status_Id,@cncl_product_id,@cncl_parental_status
            END

            CLOSE			ext_agent_in
            DEALLOCATE		ext_agent_in 
        -- End ext_agent_in -------------------------------------------------*
        
    -- When the DPD > 60 and Parent in collection and no PTP, set parent to recovery and assign to recovery campaign
			DECLARE recovery_cur CURSOR FAST_FORWARD FOR
            SELECT   lm.ILOAN_MASTER_ID,
                     lm.iloan_product_id,
                     ISNULL(e.vtitle,'NULL') AS c121_status,
                     a.iparental_status_id,
                     lm.iproduct_id,
                     c.vtitle AS parental_status
            FROM tbl_account_status a
            INNER JOIN tbl_loan_master lm on lm.iLoan_Master_ID = a.iloan_master_id
            LEFT OUTER JOIN tbl_c121_status e on e.ic121_status_id = a.ic121_status_id
            LEFT OUTER JOIN tbl_parantial_status c on c.iparantial_status_id = a.iparental_status_id
            WHERE  a.IPARENTAL_STATUS_ID = 1 --Collection
            AND ISNULL(a.IC121_STATUS_ID,0) NOT IN (15,46,17,18,20,249,278,108,111,306,307,308) --C121_Status NOT IN ('ACTIVE_PTP','ACTIVE_ARR') 
            AND    EXISTS ( SELECT 'X'
                            FROM   tbl_loan_master b,
                                   tbl_delinquency c,
                                   tbl_product d
                            WHERE  b.iLoan_Master_ID = c.iLoanMaster_ID
                            AND    b.iLoan_Master_ID = a.iLoan_Master_ID
                            AND    b.iproduct_id     = d.iproduct_id
                            AND    d.vloan_code      = @vc_product_name
                            AND    c.iActioned       = 1
                            AND    ISNULL(c.IARR_ADV_DAYS,0) > 60);
                                      
			OPEN recovery_cur
			FETCH NEXT FROM recovery_cur
			INTO @cncl_Loan_Master_ID,@cncl_Loan_Product_ID,@cncl_c121_status,@cncl_Parental_Status_Id,@cncl_product_id,@cncl_parental_status 

			WHILE @@FETCH_STATUS = 0
			BEGIN /* Begin of While loop */ 
            
            SET @vc_notes           = NULL;
            SET @vc_iaction_acc_status_ID = NULL;
            
            UPDATE tbl_account_status
            SET    IPARENTAL_STATUS_ID = 8, --set to RECOVERIES
                   iC121_Status_ID = 279 --set to ASSIGN_TO_RECOVERIES
            WHERE  iLoan_Master_ID  = @cncl_Loan_Master_ID;

            SET @vc_notes = 'Parental Status Changed From '+@cncl_parental_status +' To RECOVERIES and Collect121 Status Changed from '+@cncl_c121_status+' to ASSIGN_TO_RECOVERIES. This is due to DPD becomes greater than 59';
 
             INSERT INTO tbl_action_history( iaction_history_id,
                                             iloan_master_id,
                                             icampaign_detail_id,
                                             iuser_id,
                                             ddatetime,
                                             iloan_product_id,
                                             tntbs_notes,
                                             vntbs_title,
                                             iaccount_status,
                                             icontact_type_id,
                                             icycle_completed,
                                             icycle_number,
                                             iproduct_id)
            VALUES( NEXT VALUE FOR seq_action_history,  -- iaction_history_id,
                    @cncl_Loan_Master_ID,      -- iloan_master_id,
                    NULL,                      -- icampaign_detail_id,
                    1,                         -- iuser_id,
                    getdate(),                 -- ddatetime,
                    @cncl_Loan_Product_ID,     -- iloan_product_id,
                    @vc_notes,                 -- tntbs_notes,
                    @vc_notes,                 -- vntbs_title,
                    0,                         -- iaccount_status,
                    NULL,                      -- icontact_type_id,
                    NULL,                      -- icycle_completed,
                    NULL,                      -- icycle_number
                    @cncl_product_id);         -- iproduct_id                        


                    --SELECT @vc_iaction_acc_status_ID = iaction_account_status_ID
                    --FROM   tbl_action_account_status
                    --WHERE  vTitle = 'NRPC';

                    SELECT @action_history_curr_id = CAST(Current_Value AS INT) FROM sys.Sequences WHERE name='seq_action_history';
                    INSERT INTO tbl_action_history_details (iaction_hist_dtl_id,
                                                            ihistory_id,
                                                            iparental_status_id,
                                                            itype_id,
                                                            icat1_id,
                                                            icat2_id,
                                                            icat3_id,
                                                            tcomments,
                                                            vtypeid_sub,
                                                            iaccount_status,
                                                            icampaign_detail_id,
                                                            dletter_date,
                                                            vvalidation_status,
                                                            vvalidation_comments,
                                                            damount,
                                                            vpayment_frequency,
                                                            dpayment_occurrence,
                                                            ddate_from,
                                                            dtime_from,
                                                            ddate_to,
                                                            dtime_to,
                                                            vpayment_method,
                                                            dfollow_date,
                                                            created_date)
                    VALUES(NEXT VALUE FOR seq_action_history_details,  -- iaction_hist_dtl_id,
                           @action_history_curr_id,          -- ihistory_id,
                           @cncl_Parental_Status_Id,          -- iparental_status_id,
                           1,                                   -- itype_id,   ????
                           1,                                   -- icat1_id,
                           1,                                   -- icat2_id,
                           1,                                   -- icat3_id,
                           NULL,                                -- tcomments,
                           'PF',                                -- vtypeid_sub,
                           @vc_iaction_acc_status_ID,            -- iaccount_status,
                           NULL,                                -- icampaign_detail_id,
                           NULL,                                -- dletter_date,
                           NULL,                                -- vvalidation_status,
                           NULL,                                -- vvalidation_comments,
                           NULL,                                -- damount,
                           NULL,                                -- vpayment_frequency,
                           NULL,                                -- dpayment_occurrence,
                           NULL,                                -- ddate_from,
                           NULL,                                -- dtime_from,
                           NULL,                                -- ddate_to,
                           NULL,                                -- dtime_to,
                           NULL,                                -- vpayment_method,
                           NULL,
                           getdate()); 
                           
            FETCH NEXT FROM recovery_cur
			INTO  @cncl_Loan_Master_ID,@cncl_Loan_Product_ID,@cncl_c121_status,@cncl_Parental_Status_Id,@cncl_product_id,@cncl_parental_status
            END

            CLOSE			recovery_cur
            DEALLOCATE		recovery_cur 
        -- End recovery Cursor -------------------------------------------------*
        
-- When the DPD <= 60 and Parent in RECOVERIES, set parent to collection and assign to call campaigns
			DECLARE collect_cur CURSOR FAST_FORWARD FOR
            SELECT   lm.ILOAN_MASTER_ID,
                     lm.iloan_product_id,
                     ISNULL(e.vtitle,'NULL') AS c121_status,
                     a.iparental_status_id,
                     lm.iproduct_id,
                     c.vtitle AS parental_status
            FROM tbl_account_status a
            INNER JOIN tbl_loan_master lm on lm.iLoan_Master_ID = a.iloan_master_id
            LEFT OUTER JOIN tbl_c121_status e on e.ic121_status_id = a.ic121_status_id
            LEFT OUTER JOIN tbl_parantial_status c on c.iparantial_status_id = a.iparental_status_id
            WHERE  a.IPARENTAL_STATUS_ID = 8 --RECOVERIES
            AND    EXISTS ( SELECT 'X'
                            FROM   tbl_loan_master b,
                                   tbl_delinquency c,
                                   tbl_product d
                            WHERE  b.iLoan_Master_ID = c.iLoanMaster_ID
                            AND    b.iLoan_Master_ID = a.iLoan_Master_ID
                            AND    b.iproduct_id     = d.iproduct_id
                            AND    d.vloan_code      = @vc_product_name
                            AND    c.iActioned       = 1
                            AND    ISNULL(c.IARR_ADV_DAYS,0) <= 60);
                                      
			OPEN collect_cur
			FETCH NEXT FROM collect_cur
			INTO @cncl_Loan_Master_ID,@cncl_Loan_Product_ID,@cncl_c121_status,@cncl_Parental_Status_Id,@cncl_product_id,@cncl_parental_status 

			WHILE @@FETCH_STATUS = 0
			BEGIN /* Begin of While loop */ 
            
            SET @vc_notes           = NULL;
            SET @vc_iaction_acc_status_ID = NULL;
            
            UPDATE tbl_account_status
            SET    IPARENTAL_STATUS_ID = 1, --set to COLLECTIONS
                   iC121_Status_ID = 49 --set to RELEASE_TO_COLLECTION
            WHERE  iLoan_Master_ID  = @cncl_Loan_Master_ID;

            SET @vc_notes = 'Parental Status Changed From '+@cncl_parental_status +' To COLLECTIONS and Collect121 Status Changed from '+@cncl_c121_status+' to RELEASE_TO_COLLECTION. This is due to DPD becomes less than 59';
 
             INSERT INTO tbl_action_history( iaction_history_id,
                                             iloan_master_id,
                                             icampaign_detail_id,
                                             iuser_id,
                                             ddatetime,
                                             iloan_product_id,
                                             tntbs_notes,
                                             vntbs_title,
                                             iaccount_status,
                                             icontact_type_id,
                                             icycle_completed,
                                             icycle_number,
                                             iproduct_id)
            VALUES( NEXT VALUE FOR seq_action_history,  -- iaction_history_id,
                    @cncl_Loan_Master_ID,      -- iloan_master_id,
                    NULL,                      -- icampaign_detail_id,
                    1,                         -- iuser_id,
                    getdate(),                 -- ddatetime,
                    @cncl_Loan_Product_ID,     -- iloan_product_id,
                    @vc_notes,                 -- tntbs_notes,
                    @vc_notes,                 -- vntbs_title,
                    0,                         -- iaccount_status,
                    NULL,                      -- icontact_type_id,
                    NULL,                      -- icycle_completed,
                    NULL,                      -- icycle_number
                    @cncl_product_id);         -- iproduct_id                        


                    --SELECT @vc_iaction_acc_status_ID = iaction_account_status_ID
                    --FROM   tbl_action_account_status
                    --WHERE  vTitle = 'NRPC';

                    SELECT @action_history_curr_id = CAST(Current_Value AS INT) FROM sys.Sequences WHERE name='seq_action_history';
                    INSERT INTO tbl_action_history_details (iaction_hist_dtl_id,
                                                            ihistory_id,
                                                            iparental_status_id,
                                                            itype_id,
                                                            icat1_id,
                                                            icat2_id,
                                                            icat3_id,
                                                            tcomments,
                                                            vtypeid_sub,
                                                            iaccount_status,
                                                            icampaign_detail_id,
                                                            dletter_date,
                                                            vvalidation_status,
                                                            vvalidation_comments,
                                                            damount,
                                                            vpayment_frequency,
                                                            dpayment_occurrence,
                                                            ddate_from,
                                                            dtime_from,
                                                            ddate_to,
                                                            dtime_to,
                                                            vpayment_method,
                                                            dfollow_date,
                                                            created_date)
                    VALUES(NEXT VALUE FOR seq_action_history_details,  -- iaction_hist_dtl_id,
                           @action_history_curr_id,          -- ihistory_id,
                           @cncl_Parental_Status_Id,          -- iparental_status_id,
                           1,                                   -- itype_id,   ????
                           1,                                   -- icat1_id,
                           1,                                   -- icat2_id,
                           1,                                   -- icat3_id,
                           NULL,                                -- tcomments,
                           'PF',                                -- vtypeid_sub,
                           @vc_iaction_acc_status_ID,            -- iaccount_status,
                           NULL,                                -- icampaign_detail_id,
                           NULL,                                -- dletter_date,
                           NULL,                                -- vvalidation_status,
                           NULL,                                -- vvalidation_comments,
                           NULL,                                -- damount,
                           NULL,                                -- vpayment_frequency,
                           NULL,                                -- dpayment_occurrence,
                           NULL,                                -- ddate_from,
                           NULL,                                -- dtime_from,
                           NULL,                                -- ddate_to,
                           NULL,                                -- dtime_to,
                           NULL,                                -- vpayment_method,
                           NULL,
                           getdate()); 
                           
            FETCH NEXT FROM collect_cur
			INTO  @cncl_Loan_Master_ID,@cncl_Loan_Product_ID,@cncl_c121_status,@cncl_Parental_Status_Id,@cncl_product_id,@cncl_parental_status
            END

            CLOSE			collect_cur
            DEALLOCATE		collect_cur 
        -- End collections Cursor -------------------------------------------------*
        
	TRUNCATE TABLE gtt_campaign_accounts;
	 -- Retrieve Accounts that should be allocated to a campaign
    INSERT INTO gtt_campaign_accounts (  iLoan_Master_ID,
                                         iProduct_ID,
                                         iLoan_Product_ID,
                                         iGrand_Parental_Status_ID,
                                         iParental_Status_ID,
                                         iC121_Status_ID,
                                         iGrand_Parental_Status,
                                         iParental_Status,
                                         iC121_Status,
                                         vActive_Ptp_Arr,
                                         dReview_Date,
                                         vProduct_Name,
                                         idpd,
                                         iArr_Adv_Amt,
                                         dptp_date,
                                         darr_date,
                                         vMob_Phn_No_1,
                                         iOs_Balance,
                                         iDays_since_Last_RPC,
                                         iDays_In_Collection,
                                         dSnapshot_Date,
                                         account_unapproved,
                                         VCONTRACTNO,
										 LAST_PAY_DATE,
										 LOAN_EXP_DATE,
                                         IDELIQUENT_ID,
                                         IBRANCH_ID,
                                         BRANCH_CODE,
                                         FACILITY_PERIOD,
                                         RO_ACTIVE,
                                         DISPOSAL_STATUS,
                                         VCORE_OWNER,
                                         RO_REQUEST
                                         --CURRENT_DUES,
                                         --WRITTEN_OFF,
                                         --MORATORIUM,
                                         )
    SELECT lm.iLoan_Master_ID,                      -- iLoan_Master_ID  
           lm.iProduct_ID,                          -- iProduct_ID
           lm.iLoan_Product_ID,                     -- iLoan_Product_ID
           acc_stat.iGrand_Parantial_Status_ID,     -- iGrand_Parental_Status_ID
           acc_stat.iParental_Status_ID,            -- iParental_Status_ID
           acc_stat.iC121_Status_ID,                -- iC121_Status_ID
           gps.vStatus,                             -- iGrand_Parental_Status
           ps.vTitle,                               -- iParental_Status
           c121_stat.vTitle,                        -- iC121_Status
           (
               SELECT CASE WHEN COUNT(*) = 0 THEN 'N' ELSE 'Y' END
               FROM   tbl_action_history_ptp_arr ahpa
               WHERE  ahpa.iLoanMaster_ID   = lm.iLoan_Master_ID
               AND    ahpa.iActive          = 1
               --AND    ahpa.iLoan_Product_ID = lm.iLoan_Product_ID
           ) AS active_ptp_arr,
           ISNULL((
                   SELECT ea.dreview_date
                   FROM   tbl_exception_actions ea
                   WHERE  ea.iLoan_Master_Id = lm.iLoan_Master_ID
                   AND    iActive            = 1
               ),@vc_history_review_date) AS review_date,
           prd.vloan_code,
           ISNULL(dl.IARR_ADV_DAYS,0),
           ISNULL(dl.IARR_ADV_AMT,0),
           (
               SELECT ahpa.dptp_arr_date
               FROM   tbl_action_history_ptp_arr ahpa
               WHERE  ahpa.iLoanMaster_ID     = lm.iLoan_Master_ID
               --AND    ahpa.iLoan_Product_ID   = lm.iLoan_Product_ID
               AND    ahpa.iActive            = 1
               AND    ahpa.iCat3ID            = 1
           ) AS ptp_date,
           (
               SELECT MIN(aharr.dDate)
               FROM   tbl_action_history_ptp_arr ahpa,
                      tbl_action_hist_payment_arr aharr
               WHERE  ahpa.iLoanMaster_ID          = lm.iLoan_Master_ID
               --AND    ahpa.iLoan_Product_ID        = lm.iLoan_Product_ID
               AND    ahpa.iAction_hist_ptp_arr_id = aharr.iaction_hist_ptp_arr_id
               AND    ahpa.iActive                 = 1
               AND    ahpa.iCat3ID                 = 3
               AND    aharr.iArrangment_Status     = 1
           ) AS arr_date,
           (
               SELECT ISNULL(ci.VMOB_PHN_NO_1,VMOB_PHN_NO_2)
               FROM   tbl_customer_info ci
               WHERE  ci.IID = lm.IPRIMARY_CUSTORMER_ID
           ) AS vMob_Phn_No_1,           
           dl.ITOTAL_BALANCE,
           (
               SELECT ISNULL(DATEDIFF(DAY, MAX(ah.dDateTime), getdate()),0)
               FROM   tbl_action_history ah,
                      tbl_action_history_details ahd
               WHERE  ah.iAction_History_ID = ahd.iHistory_ID
               --AND    ah.iLoan_Product_ID   = lm.iLoan_Product_ID
               AND    ah.iLoan_Master_ID    = lm.iLoan_Master_ID
               AND    ahd.iCat3_ID          > 0
           ) iDays_since_Last_RPC,
           case when dl.IARR_ADV_DAYS = 0 then 0
           else (SELECT  ISNULL(DATEDIFF(DAY, MAX(dl2.dSnapshot_Date), getdate()),0)
                               FROM    tbl_loan_master lm2,
                                       tbl_delinquency dl2
                               WHERE   lm2.iLoan_Master_ID = dl2.iLoanMaster_ID
                               AND     lm2.iLoan_Master_ID = lm.iLoan_Master_ID
                               AND     dl2.IARR_ADV_DAYS            = 0
                ) end as Days_In_Collection,
           dl.dSnapshot_Date,
           acc_stat.account_unapproved,
           lm.VCONTRACT_NO,
		   (SELECT DISTINCT MAX(CONVERT(date, p.DPAYMT_DATE))
                FROM tbl_payment_main p
                WHERE  p.iloan_master_id   = lm.iLoan_Master_ID
                --AND    p.iloan_product_id  = lm.iLoan_Product_ID
                --AND    p.PAYMENT_TYPE not in ('C','D','E')
                ) LAST_PAY_DATE,
            lm.DCLOSE_DATE,
            dl.IID,
            lm.IBRANCH_ID,
            (SELECT VBRANCH_CODE FROM TBL_BRANCH WHERE IID = lm.IBRANCH_ID),
            lm.ILOAN_TERM,
            case when lm.FACILITY_SUB_STATUS = 'RO' then 'Y' else 'N' end as RO_ACTIVE,
           (SELECT TOP 1 d.VPROCESS
            FROM TBL_DP_WK_LOAN_WORKFLOW_STATUS a
            INNER JOIN TBL_DP_WK_WORKFLOW_DETAIL b ON b.IID = a.iworkflowdetailid
            INNER JOIN TBL_DP_WK_M_PROCESS_SUB c on c.IID = b.isubprocessid
            INNER JOIN TBL_DP_WK_M_PROCESS d on d.IID = c.iprocessid
            INNER JOIN TBL_DP_DISPOSAL_MANAGE e on e.IID = a.IDPMANAGEID
            INNER JOIN TBL_YARD_VEHICLE_MAIN f on f.IID = e.IYARDINID
            WHERE a.ILOANMASTERID=lm.iLoan_Master_ID
            and a.ipublish=1
            and b.ipublish=1
            and e.ipublish=1
            and f.ipublish=1
            and f.VYARDSTATUS='YARD_IN'),
            lm.VCORE_OWNER,
            (SELECT CASE WHEN COUNT(*) = 0 THEN 'N' ELSE 'Y' END
                FROM TBL_CASE C
                INNER JOIN TBL_WK_WORKFLOW_MAIN A on A.IID = C.IWFMAINID
                INNER JOIN TBL_WK_TYPE_FILE AF on AF.IID=A.IWK_TYPE_FILE_ID
                WHERE c.ILOAN_MASTER_ID = lm.ILOAN_MASTER_ID
                AND C.VSTATUS in ('APPROVED','ON_GOING') 
                AND AF.VTYPE='RECOVERY' 
                AND AF.VFILENAME='ro_form') AS RO_REQUEST
    FROM   tbl_loan_master lm
    inner join tbl_delinquency dl on lm.iLoan_Master_ID = dl.iLoanMaster_ID
    inner join tbl_account_status acc_stat on lm.iLoan_Master_ID = acc_stat.iLoan_Master_ID
    inner join tbl_grand_parantial_status gps on acc_stat.iGrand_Parantial_Status_ID = gps.IID
    inner join tbl_product prd on prd.iProduct_ID = lm.iProduct_ID
    inner join tbl_parantial_status ps on acc_stat.iParental_Status_ID = ps.iParantial_Status_ID
    left outer join tbl_c121_status c121_stat on acc_stat.iC121_Status_ID  = c121_stat.iC121_Status_ID and c121_stat.iPublish = 1
           
    WHERE  1=1
    AND    prd.vloan_code                       = @vc_product_name
    AND    lm.DELETE_FLAG is null
    AND    dl.iActioned                         = 1
    AND    gps.vStatus                          = 'ACTIVE'    
    AND    gps.iPublish                         = 1
    AND    ps.iPublish                          = 1
    AND    dl.ITOTAL_BALANCE                    > 0
    AND    prd.IPUBLISH                         = 1
    AND    ( ((dl.IARR_ADV_DAYS > 0) AND (dl.IARR_ADV_AMT > 0)) OR            
            ((ps.vTitle IS NOT NULL) AND (ps.vTitle <> 'COLLECTIONS'))
           );

		-- Declare cursor for extract all the campaign rules
		DECLARE camp_cur CURSOR FAST_FORWARD FOR
		SELECT  CAMP_CODE,
                PRODUCT_CODE, 
                CONDITION
        FROM    tbl_campaign_setup
        WHERE ISACTIVE =1
        AND   PRODUCT_CODE = @vc_product_name
        ORDER BY ICAMPAIGN_ORDER ASC;

		OPEN camp_cur
		FETCH NEXT FROM camp_cur
		INTO @CAMPAIGN_CODE,@PRODUCT_CODE,@CONDITION

		    WHILE @@FETCH_STATUS = 0
			BEGIN /* Begin of While loop */
				SET  @IsERROR =0;
				SET  @Error_Message=NULL;
			BEGIN TRY
				BEGIN TRANSACTION
				-- Update status
                                            
                SET @sql = 'UPDATE gtt_campaign_accounts
				SET    gtt_campaign_accounts.iCampaign_Section_Id=(SELECT a.iCampaign_Section_Id
                                            FROM   tbl_campaign_sections a,
                                                   tbl_campaign_section_main b
                                            WHERE   a.iSection_Main_ID = b.iSection_Main_ID
                                            AND     b.iPublish         = 1
                                            AND     a.iProduct_ID = gtt_campaign_accounts.iProduct_ID
                                            AND     b.campaign_code = ' +''''+ @CAMPAIGN_CODE +''''+')';                                            


				SET @sql = @sql + CHAR(10) + @CONDITION;
				print @sql;
				EXEC (@sql);                            
                                            

				COMMIT TRANSACTION;

				END TRY
			BEGIN CATCH
					  SELECT @IsERROR =1,
					  @Error_Message=ERROR_MESSAGE();

					  IF XACT_STATE() <> 0
							ROLLBACK TRANSACTION;
				END CATCH;

					FETCH NEXT FROM camp_cur
					INTO @CAMPAIGN_CODE,@PRODUCT_CODE,@CONDITION
			END /* End of While loop -----*/ 
		  CLOSE camp_cur
		  DEALLOCATE camp_cur
/*          
    -- Mark account to send PTP/ARR Reminder SMS         
    UPDATE gtt
    SET    gtt.letter_sms_type2     = CASE WHEN (gtt.vActive_Ptp_Arr = 'Y' AND gtt.dptp_date = (DATEADD(DAY,1,@vc_current_date))) THEN 'SMS_PTP_REMINDER'
                                           WHEN (gtt.vActive_Ptp_Arr = 'Y' AND gtt.darr_date = (DATEADD(DAY,1,@vc_current_date))) THEN 'SMS_ARR_REMINDER'                                           
                                      END 
    FROM gtt_campaign_accounts gtt                                  
    WHERE  gtt.vProduct_Name     = @vc_product_name
    AND    gtt.vMob_Phn_No_1     IS NOT NULL   
    AND    ( ((gtt.vActive_Ptp_Arr = 'Y')  AND (gtt.dptp_date = (DATEADD(DAY,1,@vc_current_date)))) OR
             ((gtt.vActive_Ptp_Arr = 'Y')  AND (gtt.darr_date = (DATEADD(DAY,1,@vc_current_date))))
           );*/
           
    /*-- Mark Account as Letter being sent
    UPDATE gtt
    SET    gtt.letter_sms_type  = CASE WHEN (gtt.iDPD IN (6,7,8))  THEN 'LETTER_1'
                                       WHEN (gtt.iDPD IN (25,26,27))  THEN 'LETTER_2'
                                       WHEN (gtt.iDPD IN (40,41,42))  THEN 'LETTER_3'
                                       WHEN (gtt.iDPD IN (60,61,62))  THEN 'LOD'
                                  END
    FROM gtt_campaign_accounts gtt                              
    WHERE  vProduct_Name          = @vc_product_name
    AND    gtt.idpd              IN (6,7,8,25,26,27,40,41,42,60,61,62)
    AND    gtt.iParental_Status   = 'COLLECTIONS'
    AND    NOT EXISTS    ( SELECT 'X'
                           FROM   tbl_letters l
                           WHERE  l.iLoan_Master_ID  = gtt.iLoan_Master_ID
                           AND    l.iproduct_id      = gtt.iproduct_id
                           AND    l.iloan_product_id = gtt.iloan_product_id
                           AND    ( ((gtt.iDPD IN (6,7,8))  AND (l.vStatus = 'LETTER_1') AND (l.vActive = 'Y')) OR 
                                    ((gtt.iDPD IN (25,26,27))  AND (l.vStatus = 'LETTER_2') AND (l.vActive = 'Y')) OR
                                    ((gtt.iDPD IN (40,41,42))  AND (l.vStatus = 'LETTER_3') AND (l.vActive = 'Y')) OR
                                    ((gtt.iDPD IN (60,61,62))  AND (l.vStatus = 'LOD') AND (l.vActive = 'Y'))
                                  )
                         );  */ 
                         
    DELETE FROM tbl_account_current_status
    WHERE  EXISTS (  SELECT 'X'
                     FROM   tbl_product p
                     WHERE  p.iproduct_id = tbl_account_current_status.iproduct_id
                     AND    p.vloan_code  = @vc_product_name); 
                     
    INSERT INTO tbl_account_current_status( IID,
                                            iLoan_Master_ID,
                                            iUse_for_Campaign,
                                            dDate_Update,
                                            dEnd_Date,
                                            iLetter_Type,
                                            dDays_to_Eliminate,
                                            iAdmin_Approve,
                                            iLoan_Product_ID,
                                            iCampaign_Section_ID,
                                            iArr_Adv_Days,
                                            dArr_Adv_Amt,
                                            iHistory_Detail_ID,
                                            iDays_Last_RPC,
                                            iDays_In_Collection,
                                            iOs_Balance,
                                            vState,
                                            vCampaign_Note,
                                            dMin_due_dt,
                                            iproduct_id
                                            )
    SELECT NEXT VALUE FOR seq_account_current_status,  -- iAccount_Current_Status_ID,
           gtt.iLoan_Master_ID,                     -- iLoan_Master_ID,
           1,                                       -- iUse_for_Campaign,
           @vc_current_date_time,                   -- dDate_Update,
           NULL,                                    -- dEnd_Date,
           NULL,                                    -- iLetter_Type,  Letter table id
           0,                                       -- dDays_to_Eliminate,
           0,                                       -- iAdmin_Approve,
           gtt.iLoan_Product_ID,                    -- iLoan_Product_ID,
           gtt.iCampaign_Section_Id,                -- iCampaign_Section_ID,
           gtt.idpd,                                -- iArr_Adv_Days,
           gtt.iArr_Adv_Amt,                        -- dArr_Adv_Amt,
           NULL,                                    -- iHistory_Detail_ID,
           gtt.iDays_since_Last_RPC,                -- iDays_Last_RPC
           gtt.iDays_In_Collection,                 -- iDays_In_Collection
           gtt.iOs_Balance,                         -- iOs_Balance,
           NULL,                                    -- vState,
           gtt.campaign_note,                       -- vCampaign_Note,
           NULL,                                    -- dMin_due_dt
           gtt.iproduct_id                          -- iproduct_id
    FROM   gtt_campaign_accounts gtt; 

    -- Insert records to the Campaign tables    
    DECLARE campaign_cur CURSOR FAST_FORWARD FOR
    SELECT DISTINCT a.iproduct_id,
                    a.dSnapshot_Date,
                    ( 
                       SELECT b.vLoan_Code
                       FROM   tbl_product b
                       WHERE  b.iproduct_id = a.iproduct_id
                       ) AS vLoan_Code
                      FROM   gtt_campaign_accounts a;
			
            OPEN campaign_cur
			FETCH NEXT FROM campaign_cur
			INTO  @product_id , @Snapshot_Date, @vc_product_name

			WHILE @@FETCH_STATUS = 0
			BEGIN /* Begin of While loop */ 

            -- Inactivate all active campaigns    
                UPDATE tbl_campaign_main
                SET    iPublish = 0
                WHERE  iPublish = 1
                AND    iProduct_ID = @product_id; 
                
            -- Create new campaign
            INSERT INTO tbl_campaign_main(  iCampaign_Main_ID,
                                            vTitle,
                                            iLoan_Product_ID,
                                            dCurrent_Date_Time,
                                            iUser_ID,
                                            tDes,
                                            vURL,
                                            iPublish,
                                            dCampaign_Date,
                                            vType,
                                            vApply_Filter_Automate,
                                            iproduct_id)
            VALUES( NEXT VALUE FOR seq_campaign_main,                                          -- iCampaign_Main_ID,
                    @vc_product_name+'-'+CAST(@vc_current_date_time AS VARCHAR),              -- vTitle,
                    --camp_cur.iLoan_Product_ID,                                               -- iLoan_Product_ID,
                    NULL,                                                                      -- iLoan_Product_ID,
                    @vc_current_date_time,                                                     -- dCurrent_Date_Time,
                    1,                                                                         -- iUser_ID,
                    NULL,                                                                      -- tDes,
                    NEWID(),                                                                   -- vURL,
                    1,                                                                         -- iPublish,
                    @Snapshot_Date,                                                            -- dCampaign_Date,
                    NULL,                                                                      -- Type,
                    'Y',                                                                       -- vApply_Filter_Automate
                    @product_id                                                                -- iproduct_id
                 );                                                                      

        -- Inactivate all active campaign details    
            UPDATE a
            SET    a.iPublish = 0,
                   a.vStatus  = 'Cancelled'
            FROM tbl_campaign_details a
            WHERE  (a.vStatus = 'Active' OR a.iPublish = 1)
            AND    EXISTS ( SELECT 'X'
                            FROM   tbl_campaign_main b
                            WHERE  b.iCampaign_Main_ID = a.iCampaign_ID
                            AND    b.iProduct_ID = @product_id
                          );     
                          
                          DECLARE camp_dtls_cur CURSOR FAST_FORWARD FOR
                          SELECT iProduct_ID,
                                 iCampaign_Section_Id,
                                 COUNT(*) AS tot_accounts
                               FROM   gtt_campaign_accounts
                               WHERE  iProduct_ID  = @product_id
                               GROUP BY iProduct_ID,iCampaign_Section_Id;
                               
                         OPEN camp_dtls_cur
			             FETCH NEXT FROM camp_dtls_cur
                         INTO  @product_id , @Campaign_Section_Id, @acc_count

                            WHILE @@FETCH_STATUS = 0
                            BEGIN /* Begin of While loop */ 


                                SELECT @campaign_main_curr_id = CAST(Current_Value AS INT) FROM sys.Sequences WHERE name='seq_campaign_main';                            
                                INSERT INTO tbl_campaign_details
                                        (iCampaign_Detail_ID,
                                         iCampaign_ID,
                                         iSection_ID,
                                         iPublish,
                                         iTot_Account,
                                         iActioned_Account,
                                         vURL,
                                         vStatus,
                                         iActioned_Type,
                                         iProduct_ID,
                                         ddate_time
                                         )
                    
                                VALUES( NEXT VALUE FOR seq_campaign_details, -- iCampaign_Detail_ID
                                        @campaign_main_curr_id,              -- iCampaign_ID
                                        @Campaign_Section_Id,                -- iSection_ID
                                        1,                                   -- iPublish
                                        @acc_count,                          -- iTot_Account
                                        0,                                   -- iActioned_Account
                                        NEWID(),                             -- vURL
                                        'Active',                            -- vStatus
                                        0,                                   -- iActioned_Type
                                        @product_id,                         -- iProduct_ID
                                        getdate()
                                      );  
                                      
                 
                                SELECT @campaign_details_curr_id = CAST(Current_Value AS INT) FROM sys.Sequences WHERE name='seq_campaign_details';     
                                INSERT INTO tbl_campaign_accounts( icampaign_account_id, 
                                                                   iloan_product_id, 
                                                                   icamaign_detail_id,
                                                                   ddate_time, 
                                                                   iloan_master_id, 
                                                                   vurl, 
                                                                   iaction_taken,
                                                                   iuserid_inuse, 
                                                                   iletter_type, 
                                                                   iaction_type_id, 
                                                                   iaction_account_status_id,
                                                                   iassigned_to,                                                
                                                                   idays_last_rpc, 
                                                                   idays_in_collection, 
                                                                   ifilter_flag, 
                                                                   iC121_Status_ID,
                                                                   iParental_Status_id,
                                                                   iProduct_ID,
                                                                   ideliquent_id,
                                                                   iarr_adv_days,                                               
                                                                   iarr_adv_amt,
                                                                   itotal_balance,
                                                                   IBRANCH_ID
                                                                   )                                
                                
                                   SELECT NEXT VALUE FOR seq_campaign_accounts,    -- icampaign_account_id, 
                                          x.iLoan_Product_ID,               -- iloan_product_id, 
                                          @campaign_details_curr_id,        -- icamaign_detail_id,
                                          @vc_current_date_time,            -- ddate_time, 
                                          x.iloan_master_id,                -- iloan_master_id
                                          NEWID(),                          -- vurl, 
                                          0,                                -- iaction_taken,
                                          0,                                -- iuserid_inuse,                                           
                                          0,                                -- iletter_type,                                           
                                          0,                                -- iaction_type_id, 
                                          0,                                -- iaction_account_status_id,
                                          0,                                -- iassigned_to,                                                                                     
                                          x.iDays_since_Last_RPC,           -- idays_last_rpc
                                          x.idays_in_collection,            -- idays_in_collection
                                          1,                                -- ifilter_flag,                                          
                                          x.iC121_Status_ID,                -- iC121_Status_ID
                                          x.iParental_Status_ID,            -- iParental_Status_id
                                          x.iProduct_ID,                    -- iProduct_ID                                          
                                          x.IDELIQUENT_ID,                  -- Delinquency_ID                                          
                                          x.iDPD,                           -- iarr_adv_days
                                          x.iarr_adv_amt,                   -- darr_adv_amt
                                          x.ios_balance,                    -- ios_balance 
                                          x.IBRANCH_ID
                                   FROM   (
                                              SELECT a.iLoan_Product_ID,
                                                     a.iloan_master_id,
                                                     a.iDays_since_Last_RPC,
                                                     a.idays_in_collection,
                                                     a.iDPD,
                                                     a.ios_balance,
                                                     a.iC121_Status_ID,
                                                     a.iParental_Status_ID,
                                                     a.iProduct_ID,
                                                     a.IDELIQUENT_ID,                                                     
                                                     a.iarr_adv_amt,
                                                     a.LAST_PAY_DATE,
                                                     a.LOAN_EXP_DATE,
                                                     a.IBRANCH_ID
                                              FROM   gtt_campaign_accounts a
                                              WHERE  iProduct_ID          = @product_id
                                              AND    iCampaign_Section_Id = @Campaign_Section_Id
                                          ) x;


                        
                         FETCH NEXT FROM camp_dtls_cur
                         INTO  @product_id , @Campaign_Section_Id, @acc_count
                         END

                         CLOSE			camp_dtls_cur
                         DEALLOCATE		camp_dtls_cur 

			FETCH NEXT FROM campaign_cur
            INTO  @product_id , @Snapshot_Date, @vc_product_name
            END

            CLOSE			campaign_cur
            DEALLOCATE		campaign_cur

			EXEC P_SMS_ALLOCATOR @vc_product_name;
			
    UPDATE  tbl_process_status_log
    SET     endtim   = GETDATE(),
            comments = 'End Campaign Creation Stored Procedure P_CAMPAIGN_ALLOCATOR_PL'
    WHERE   logid = @var_ProcessStatusID;			            
END TRY                      
BEGIN CATCH

     SELECT @IsERROR =1,
	 @Error_Message=ERROR_MESSAGE(),
     @ERROR_LINE = ERROR_LINE();
  -- ------------------------------------------------------------------------------------------------*

      /* Add row to Process Status Log- Indcating Error occured */
      UPDATE      TBL_PROCESS_STATUS_LOG
          SET     ROWSPROCESSE=0,
                  ENDTIM=GETDATE(),
                  COMMENTS='P_CAMPAIGN_ALLOCATOR_PL Completed With Errors, Please refer TBL_ERROR_LOG_DB for more details !'
          WHERE   LOGID= @var_ProcessStatusID;

      /* Log error Information in TBL_ERROR_LOG */

                    INSERT INTO TBL_ERROR_LOG_DB
                    (
                    IID,
                    ITEMNAME,
                    ERROR_MESSAGE,
                    POSTDATETIME,
                    CREATEDBY

                    )
                   SELECT
                   NEXT VALUE FOR SEQ_ERROR_LOG,
                   'P_CAMPAIGN_ALLOCATOR_PL',
                   @Error_Message+'line '+@ERROR_LINE,
                   GETDATE(),
                   1 
                   ;

    END CATCH
END
