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
        method = getattr(self, metric, lambda: "Invalid metric")
        # Call the method as we return it
        return method()
    
    def possible_metrics(self):
        # Possible metrics to use for MDE (same as current calculator)
        # may want to make this consistent with the primary metrics available in exp dash in the future
        metrics = [
            'all_tvt_hours',
            'capped_tvt',
            'new_viewer_first_day_capped_tvt',
            'registrations',
            'visits',
            'viewer_conversion',
            'new_viewer_first_day_conversion',
            'new_user_1_to_8_days_retained',
            'all_user_retained_in_experiment_timeframe',
            'ad_impressions'
        ]
        return metrics
        
    def choose_metric(self, metric):
        # the most important function in this tool
        return metric

    def all_tvt_hours(self):
        return """
        , user_data AS (
          SELECT 
            device_id, ds, platform_type, platform, device_first_seen_ts, first_exposure_ds, 
            'all_tvt_hours'::text AS metric_name,
            'SUM'::text AS metric_collection_method, 
            tvt_sec / 3600.0 AS metric_value
          FROM raw_user_data
        )
        """

    def capped_tvt(self):
        return """
        , user_data AS (
          SELECT 
            device_id, ds, platform_type, platform, device_first_seen_ts, first_exposure_ds, 
            'capped_tvt'::text AS metric_name,
            'SUM'::text AS metric_collection_method, 
            LEAST(tvt_sec / 3600.0, 4.0) AS metric_value 
          FROM raw_user_data
        )
        """

    def new_viewer_first_day_capped_tvt(self):
        return """
        , user_data AS (
          SELECT 
            device_id, ds, platform_type, platform, device_first_seen_ts, first_exposure_ds, 
            'new_viewer_first_day_capped_tvt'::text AS metric_name,
            'SUM'::text AS metric_collection_method, 
            LEAST(tvt_sec / 3600.0, 4.0) AS metric_value 
          FROM raw_user_data
          WHERE ds >= DATE_TRUNC('day', device_first_seen_ts) AND ds < device_first_seen_ts + INTERVAL '1 day'
        )
        """

    def registrations(self):
        return """
        , user_data AS (
          SELECT 
            device_id, ds, platform_type, platform, device_first_seen_ts, first_exposure_ds, 
            'registrations'::text AS metric_name,
            'SUM'::text AS metric_collection_method, 
            signup_or_registration_activity_count AS metric_value
          FROM raw_user_data
        )
        """

    def visits(self):
        return """
        , user_data AS (
          SELECT 
            device_id, ds, platform_type, platform, device_first_seen_ts, first_exposure_ds, 
            'visits'::text AS metric_name,
            'SUM'::text AS metric_collection_method, 
            visit_total_count AS metric_value 
          FROM raw_user_data
        )
        """

    def viewer_conversion(self):
        return """
        , user_data AS (
          SELECT 
            device_id, ds, platform_type, platform, device_first_seen_ts, first_exposure_ds, 
            'viewer_conversion'::text AS metric_name,
            'MAX'::text AS metric_collection_method, 
            CASE WHEN tvt_sec > 10 THEN 1.0 ELSE 0.0 END AS metric_value 
          FROM raw_user_data
        )
        """

    def new_viewer_first_day_conversion(self):
        return """
        , user_data AS (
          SELECT 
            device_id, ds, platform_type, platform, device_first_seen_ts, first_exposure_ds, 
            'new_viewer_first_day_conversion'::text AS metric_name,
            'MAX'::text AS metric_collection_method, 
            CASE WHEN tvt_sec > 10 THEN 1.0 ELSE 0.0 END AS metric_value
          FROM raw_user_data
          WHERE ds >= DATE_TRUNC('day', device_first_seen_ts) AND ds < device_first_seen_ts + INTERVAL '1 day'
        )
        """

    def new_user_1_to_8_days_retained(self):
        return """
        , user_data AS (
          SELECT 
            device_id, ds, platform_type, platform, device_first_seen_ts, first_exposure_ds, 
            'new_user_1_to_8_days_retained'::text AS metric_name,
            'MAX'::text AS metric_collection_method, 
            CASE WHEN ds > device_first_seen_ts + INTERVAL '1 day' AND tvt_sec > 10 THEN 1.0 ELSE 0.0 END AS metric_value 
          FROM raw_user_data
          WHERE ds >= DATE_TRUNC('day', device_first_seen_ts) AND ds < device_first_seen_ts + INTERVAL '8 day'
        )
        """

    def all_user_retained_in_experiment_timeframe(self):
        return """
        , user_data AS (
          SELECT DISTINCT 
            device_id, ds, platform_type, platform, device_first_seen_ts, first_exposure_ds, 
            'all_user_retained_in_experiment_timeframe'::text AS metric_name,
            'SUMGREATERTHAN'::text AS metric_collection_method,
            1.0 AS metric_value
          FROM raw_user_data
          WHERE tvt_sec > 10
        )
        """

    def ad_impressions(self):
        return """
        , ad_impressions_data AS (
            SELECT ds,
                   device_id,
                   device_first_seen_ts,
                   {{ platform_type('platform') }} AS platform_type,
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
            SELECT d.device_id,
                   d.ds,
                   d.platform_type,
                   d.platform,
                   d.device_first_seen_ts,
                   d.first_exposure_ds,
                   SUM(COALESCE(rev.ad_impression_total_count, 0))::float AS ad_impression_total_count,
                   SUM(COALESCE(rev.gross_revenue, 0))::float AS gross_revenue
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
            'ad_impressions' AS metric_name,
            'SUM' AS metric_collection_method,
            ad_impression_total_count AS metric_value
          FROM device_data_impressions
        )
        """
    
    # TODO: make the list of available metrics consistent with exp dash
    # Primary experimentation dash metrics:
#     primary = [
#         'activations',
#         'ad_impressions',
#         'capped_linear_and_vod_tvt_hours_4_hr_per_day',
#         'capped_tvt_hours_4_hr_per_day',
#         'linear_and_vod_all_user_retained_in_experiment_timeframe',
#         'linear_and_vod_viewer_conversion',
#         'new_user_1_to_8_days_retained',
#         'new_user_capped_first_day_tvt_hours_4_hr_per_day', 
#         'new_viewer_first_day_conversion',
#         'signups',
#         'viewer_5_min_conversion',
#         'visits'
#     ]