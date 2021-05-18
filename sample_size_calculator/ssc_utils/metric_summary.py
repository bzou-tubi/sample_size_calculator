class metric_summary(object):

    def generate_metric_summary_cte(self):
        """
        Generates the SQL CTE that summarizes the device level metric chosen.
            
        Returns:
            String
        """
        
        return """
           , metrics AS (
              SELECT DISTINCT
                     user_data.device_id,
                     platform_type,
                     platform,
                     metric_name,
                     CASE
                       WHEN metric_collection_method = 'SUM' THEN SUM(CASE WHEN user_data.ds >= user_data.first_exposure_ds THEN metric_value ELSE 0 END) OVER
                        (PARTITION BY user_data.device_id, user_data.platform)
                       WHEN metric_collection_method = 'MAX' THEN MAX(CASE WHEN user_data.ds >= user_data.first_exposure_ds THEN metric_value ELSE 0 END) OVER
                        (PARTITION BY user_data.device_id, user_data.platform)
                      WHEN metric_collection_method = 'AVG' THEN AVG(CASE WHEN user_data.ds >= user_data.first_exposure_ds THEN metric_value ELSE NULL END) OVER
                        (PARTITION BY user_data.device_id, user_data.platform)
                      WHEN metric_collection_method = 'SUMGREATERTHAN' THEN CASE WHEN (SUM(CASE WHEN user_data.ds >= user_data.first_exposure_ds THEN metric_value ELSE 0 END) OVER
                        (PARTITION BY user_data.device_id, user_data.platform)) > 1 THEN 1.0 ELSE 0.0 END
                      ELSE 0 END::float
                     AS metric_result,
                    CASE
                          WHEN metric_collection_method = 'SUM' THEN
                              SUM(CASE WHEN user_data.ds < user_data.first_exposure_ds THEN metric_value ELSE
                          (CASE WHEN device_first_seen_ts < user_data.first_exposure_ds - interval '14 day' THEN 0 ELSE NULL END) END) OVER
                          (PARTITION BY user_data.device_id, user_data.platform)
                          WHEN metric_collection_method = 'MAX' THEN
                              MAX(CASE WHEN user_data.ds < user_data.first_exposure_ds THEN metric_value ELSE
                          (CASE WHEN device_first_seen_ts < user_data.first_exposure_ds - interval '14 day' THEN 0 ELSE NULL END) END) OVER
                          (PARTITION BY user_data.device_id, user_data.platform)
                          WHEN metric_collection_method = 'AVG' THEN
                              AVG(CASE WHEN user_data.ds < user_data.first_exposure_ds THEN metric_value ELSE
                          (CASE WHEN device_first_seen_ts < user_data.first_exposure_ds - interval '14 day' THEN 0 ELSE NULL END) END) OVER
                          (PARTITION BY user_data.device_id, user_data.platform)
                    WHEN metric_collection_method = 'SUMGREATERTHAN' THEN
                      CASE WHEN (SUM(CASE WHEN user_data.ds < user_data.first_exposure_ds THEN metric_value ELSE
                          (CASE WHEN device_first_seen_ts < user_data.first_exposure_ds - interval '14 day' THEN 0 ELSE NULL END) END) OVER
                          (PARTITION BY user_data.device_id, user_data.platform)
                          ) > 1 THEN 1 ELSE 0 END
                          ELSE 0 END::float AS metric_covariate
              FROM user_data
            )
        """