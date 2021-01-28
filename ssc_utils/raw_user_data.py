class raw_user_data(object):

    def generate_raw_user_data_cte(self):
        return """
            , raw_user_data AS (
              SELECT DISTINCT
                     a.device_id,
                     device_first_seen_ts,
                     ds,
                     platform_type,
                     platform,
                     GETDATE() AS last_exposure_ds,
                     DATEADD('week', -2, DATE_TRUNC('week', last_exposure_ds)) AS first_exposure_ds,
                     -- Metrics
                     tvt_sec,
                     signup_or_registration_activity_count,
                     visit_total_count
              FROM elig_device_metrics as a
              JOIN elig_devices2 as e    -- TODO: make this dynamic, based on if cumul_filter_metric is used or not
                ON a.device_id = e.device_id  
              WHERE DATE_TRUNC('week',ds) >= dateadd('week', -4, DATE_TRUNC('week',GETDATE()))
                AND DATE_TRUNC('week',ds) < DATE_TRUNC('week', GETDATE())
            )
        """