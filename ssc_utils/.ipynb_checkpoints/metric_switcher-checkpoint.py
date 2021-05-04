class metric_switcher(object):
    """
    Contains a set of functions that generates the SQL CTE for a chosen metric.
    """
    
    def generate_user_data_cte(self, metric): # <-- whatever you input here in "metric" will choose one of the CTEs below      
        """
        Generates a string SQL CTE based on the metric chosen. 
        
        Args: 
            metric: a string chosen from the list of metrics in possible_metrics()

        Returns:
            String
        """
        # Get the method from 'self'. Default to a lambda.
        metric_clean = metric.replace('--','_').replace('-','_')
        method = getattr(self, metric_clean, lambda: "Invalid metric")
        
        # Call the method as we return it
        return method()
    
    def possible_metrics(self):
        # Possible metrics to use for MDE (same as current calculator)
        # may want to make this consistent with the primary metrics available in exp dash in the future
        metrics = [
            'tvt',
            'tvt-capped',
            'tvt-capped_new_visitors',
            'registration-did_signup',
            'registration-did_activate', 
            'visits',
            'conversion',
            'conversion--new_visitors',
            'retention--new_viewers',
            'retention',
            'ad_impressions',
            'tvt-vod_series',
            'tvt-vod_movie'
        ]
        return metrics
        
    def choose_metric(self, metric):
        # the most important function in this tool
        return metric

    def tvt(self):
        return """
        , user_data AS (
          SELECT 
            device_id, ds, platform_type, platform, device_first_seen_ts, first_exposure_ds, 
            'tvt'::text AS metric_name,
            'SUM'::text AS metric_collection_method, 
            (tvt_sec + linear_tvt_sec) / 3600.0 AS metric_value
          FROM raw_user_data
        )
        """

    def tvt_capped(self):
        return """
        , user_data AS (
          SELECT 
            device_id, ds, platform_type, platform, device_first_seen_ts, first_exposure_ds, 
            'tvt-capped'::text AS metric_name,
            'SUM'::text AS metric_collection_method, 
            CASE WHEN (SUM((tvt_sec + linear_tvt_sec)/3600.0) OVER (PARTITION BY device_id,ds,platform)) > 4 THEN 4.0 
                 ELSE (SUM((tvt_sec + linear_tvt_sec)/3600.0) OVER (PARTITION BY device_id,ds,platform)) 
            END AS metric_value
          FROM raw_user_data
        )
        """

    def tvt_capped_new_visitors(self):
        return """
        , user_data AS (
          SELECT 
            device_id, ds, platform_type, platform, device_first_seen_ts, first_exposure_ds, 
            'tvt-capped-new_visitors'::text AS metric_name,
            'SUM'::text AS metric_collection_method, 
            CASE WHEN (SUM((tvt_sec + linear_tvt_sec)/3600.0) OVER (PARTITION BY device_id,ds,platform)) > 4 THEN 4.0 ELSE (SUM((tvt_sec + linear_tvt_sec)/3600.0) OVER (PARTITION BY device_id,ds,platform)) END AS metric_value
          FROM raw_user_data
          WHERE ds >= DATE_TRUNC('day',device_first_seen_ts) AND ds < device_first_seen_ts + INTERVAL '7 day'
        )
        """

    def registration_did_signup(self):
        return """
        , user_data AS (
          SELECT 
            device_id, ds, platform_type, platform, device_first_seen_ts, first_exposure_ds, 
            'registration-did_signup'::text AS metric_name,
            'SUM'::text AS metric_collection_method, 
            CASE WHEN user_signup_count > 0 THEN 1.0 ELSE 0.0 END AS metric_value
          FROM raw_user_data
        )
        """

    def registration_did_activate(self):
        return """
        , user_data AS (
          SELECT 
            device_id, ds, platform_type, platform, device_first_seen_ts, first_exposure_ds, 
            'registration-did_activate'::text AS metric_name,
            'SUM'::text AS metric_collection_method, 
            CASE WHEN device_registration_count > 0 THEN 1.0 ELSE 0.0 END AS metric_value
          FROM raw_user_data
        )
        """
    
    def visits(self):
        return """
        , user_data AS (
          SELECT 
            device_id, ds, platform_type, platform, device_first_seen_ts, first_exposure_ds, 
            'visit'::text AS metric_name,
            'SUM'::text AS metric_collection_method, 
            visit_total_count::float AS metric_value
          FROM raw_user_data
        )
        """
    
    def conversion(self):
        return """
        , user_data AS (
          SELECT 
            device_id, ds, platform_type, platform, device_first_seen_ts, first_exposure_ds, 
            'conversion'::text AS metric_name,
            'MAX'::text AS metric_collection_method, 
            CASE WHEN (tvt_sec + linear_tvt_sec) > 10 THEN 1.0 ELSE 0.0 END AS metric_value
          FROM raw_user_data
        )
        """

    def conversion_new_visitors(self):
        return """
        , user_data AS (
          SELECT 
            device_id, ds, platform_type, platform, device_first_seen_ts, first_exposure_ds, 
            'conversion--new_visitors'::text AS metric_name,
            'MAX'::text AS metric_collection_method, 
            CASE WHEN (tvt_sec + linear_tvt_sec) > 10 THEN 1.0 ELSE 0.0 END AS metric_value
          FROM raw_user_data
          WHERE ds >= DATE_TRUNC('day',device_first_seen_ts) AND ds < device_first_seen_ts + INTERVAL '7 day'
        )
        """

    def retention_new_viewers(self):
        return """
        , user_data AS (
          SELECT 
            device_id, ds, platform_type, platform, device_first_seen_ts, first_exposure_ds, 
            'retention--new_viewers'::text AS metric_name,
            'MAX'::text AS metric_collection_method, 
            CASE WHEN ds > device_first_view_ts + INTERVAL '1 day' AND (tvt_sec + linear_tvt_sec) > 10 THEN 1.0 ELSE 0.0 END AS metric_value
          FROM raw_user_data
          WHERE ds >= DATE_TRUNC('day',device_first_seen_ts) AND ds < device_first_seen_ts + INTERVAL '7 day'
        )
        """

    def retention(self):
        return """
        , user_data AS (
          SELECT DISTINCT 
            device_id, ds, platform_type, platform, device_first_seen_ts, first_exposure_ds, 
            'retention'::text AS metric_name,
            'SUMGREATERTHAN'::text AS metric_collection_method,
            1.0 AS metric_value
          FROM raw_user_data
          WHERE (tvt_sec + linear_tvt_sec) > 10
        )
        """

    def ad_impressions(self):
        return """
        , ad_impressions_data AS (
            SELECT ds,
                   device_id,
                   device_first_seen_ts,
                   platform,
                   GETDATE() AS last_exposure_ds,
                   DATEADD('week', -2, DATE_TRUNC('week', last_exposure_ds)) AS first_exposure_ds,
                   COALESCE(ad_impression_total_count, 0)::float AS ad_impression_total_count,
                   COALESCE(gross_revenue, 0)::float AS gross_revenue
            FROM tubidw.revenue_bydevice_daily
            WHERE DATE_TRUNC('week',ds) >= dateadd('week', -4, DATE_TRUNC('week',GETDATE()))
              AND DATE_TRUNC('week',ds) < DATE_TRUNC('week', GETDATE())
        )

        , device_data_impressions AS (
            SELECT  d.device_id,
                    d.ds,
                    d.platform,
                    d.platform_type,
                    d.device_first_seen_ts,
                    d.first_exposure_ds,
                    SUM(rev.ad_impression_total_count) AS ad_impression_total_count,
                    SUM(rev.gross_revenue) AS gross_revenue
            FROM ad_impressions_data AS rev
              RIGHT JOIN raw_user_data AS d
                ON d.device_id = rev.device_id
                AND d.ds = rev.ds
            GROUP BY 1, 2, 3, 4, 5, 6
        )

          -- Impressions
        , user_data AS (
          SELECT 
            device_id, ds, platform_type, platform, device_first_seen_ts, first_exposure_ds, 
            'ad_impressions'::text AS metric_name,
            'SUM'::text AS metric_collection_method,
             COALESCE(ad_impression_total_count,0)::float AS metric_value
          FROM device_data_impressions
        )
        """
    
    def tvt_vod_series(self):
        return """
        , user_data AS (
            SELECT device_id, ds, platform_type, platform, device_first_seen_ts, first_exposure_ds,
                'tvt-vod_series'::text AS metric_name,
                series_tvt_sec/3600.0 AS metric_value,
                'SUM'::text AS metric_collection_method
            FROM raw_user_data
        )
        """
    
    def tvt_vod_movie(self):
        return """
        , user_data AS (
            SELECT device_id, ds, platform_type, platform, device_first_seen_ts, first_exposure_ds,
                'tvt-vod_movie'::text AS metric_name,
                movie_tvt_sec/3600.0 AS metric_value,
                'SUM'::text AS metric_collection_method
            FROM raw_user_data
        )
        """