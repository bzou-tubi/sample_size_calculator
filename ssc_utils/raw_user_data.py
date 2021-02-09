class raw_user_data(object):
    """
    Generates a catch-all string: SQL CTE that pulls the standard metrics of active devices in the last 4 weeks.

    In the future, we may want to improve this to allow flexibility for more complex metrics not available in device_metric_daily
    ie. verification rates can only be calculated from analytics_richevent using is_confirmed = 't'            
    """
    def generate_raw_user_data_cte(self):
        return """, raw_user_data AS (
              SELECT 
                     a.device_id,
                     device_first_seen_ts,
                     ds,
                     platform_type,
                     platform,
                     GETDATE() AS last_exposure_ds,
                     DATEADD('week', -2, DATE_TRUNC('week', last_exposure_ds)) AS first_exposure_ds,
                     -- Metrics
                     sum(tvt_sec) as tvt_sec,
                     sum(user_signup_count) as user_signup_count,
                     sum(device_registration_count) as device_registration_count,
                     sum(signup_or_registration_activity_count) as signup_or_registration_activity_count,
                     sum(visit_total_count) as visit_total_count
              FROM tubidw.device_metric_daily as a
              JOIN elig_devices2 as e    -- TODO: make this dynamic, based on if cumul_filter_metric is used or not
                ON a.device_id = e.device_id  
              WHERE DATE_TRUNC('week',ds) >= dateadd('week', -4, DATE_TRUNC('week',GETDATE()))
                AND DATE_TRUNC('week',ds) < DATE_TRUNC('week', GETDATE())
              GROUP BY 1,2,3,4,5,6,7
            )
        """