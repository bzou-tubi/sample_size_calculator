import tubi_data_runtime as tdr
import math
import pandas as pd
import numpy as np
from datetime import date
from statsmodels.stats.power import tt_ind_solve_power


class filter_generator(object):
    
    def attribute_conditions_choices(self):
        attribute_conditions = [
            '=',
            '<>',
            'IN',
            'IS',
            'IS NOT'
        ]
        return attribute_conditions 
    
    def metric_conditions_choices(self):
        metric_conditions =  [
            '>',
            '<',
            '>=',
            '<=',
            'BETWEEN'
        ] + self.attribute_conditions_choices()
        return metric_conditions 

    def filter_attributes_choices(self):
        # Possible choices for attribute filtering (from all_metric_hourly): 
        filter_attributes = [
                'no filters',
                'user_id',
                'device_id',
                'platform',
                'platform_type',
                'country',
                'region',
                'city',
                'dma',
                'os',
                'os_version',
                'manufacturer',
                'app_mode',
                'app_version',
                'device_language',
                'content_id',
                'program_id',
                'content_type'
                'tvt_sec' # note: here tvt_sec is treated as an attribute rather than a cumulative metric
            ]
        return filter_attributes 

    def filter_metrics_choices(self):
    # Possible choices for metric filtering (from all_metric_hourly): 
        filter_metrics = [
            'no filters',
            'tvt_sec',
            'movie_non_autoplay_tvt_sec',
            'series_non_autoplay_tvt_sec',
            'autoplay_tvt_sec',
            'non_autoplay_tvt_sec',
            'series_tvt_sec',
            'movie_tvt_sec',
            'series_autoplay_tvt_sec',
            'movie_autoplay_tvt_sec',
            'visit_total_count',
            'view_total_count',
            'autoplay_view_total_count',
            'non_autoplay_view_total_count',
            'series_view_total_count',
            'movie_view_total_count',
            'autoplay_movie_view_total_count',
            'autoplay_series_view_total_count',
            'complete_5p_total_count',
            'complete_30p_total_count',
            'complete_70p_total_count',
            'complete_90p_total_count',
            'episode_complete_30p_total_count',
            'episode_complete_70p_total_count',
            'episode_complete_90p_total_count',
            'movie_complete_30p_total_count',
            'movie_complete_70p_total_count',
            'movie_complete_90p_total_count',
            'ad_impression_total_count',
            'ad_break_total_count',
            'seek_total_count',
            'pause_total_count',
            'subtitles_total_count',
            'search_total_count',
            'user_signup_count',
            'device_registration_count',
            'signup_or_registration_activity_count',
            'cast_count',
            'add_to_queue_total_count',
            'details_page_visit_total_count',
            'onboarding_page_visit_total_count',
            'home_page_visit_total_count',
            'browse_page_visit_total_count',
            'category_page_visit_total_count',
            'trailer_start_count',
            'unattributed_tvt_sec',
            'linear_tvt_sec',
            'linear_view_total_count'
        ]
        return filter_metrics 
    
    def base_amh_query(self):
        amh_filter_query = """
        WITH elig_devices as (
            -- Pull list of devices that were active (has any row; don't need TVT >0) in the past 2 weeks
            -- Using all_metric_hourly for additional filters
            SELECT DISTINCT device_id
            FROM tubidw.all_metric_hourly
            WHERE DATE_TRUNC('week',hs) >= dateadd('week',-2,DATE_TRUNC('week',GETDATE()))
            AND DATE_TRUNC('week',hs) < DATE_TRUNC('week',GETDATE())
            {attr_filter} -- attribute filters dynamically populate here

        --     for example:
        --     AND user_id is not null AND device_id <> user_id   -- Guest vs signed in device
        --     AND platform IN ('ROKU', 'AMAZON')                 -- Platform/Platform type specific
        --     AND country in ('US')                              -- Geo specific
        --     AND os IN ('abcdefg')                              -- OS/version specific
        --     AND content_id IN () AND tvt_sec > 0               -- Browsed/watched specific content/content type
        --     TODO: currently can't get a metric/attribute combo filter, like "devices that watched at least 50% of a specific content_id"
        )

        -- The next 3 CTEs are a waste of processing if cumul_filter_metric is not used.
        -- TODO: Figure out some way to make this dynamic, based on if cumul_filter_metric is used or not
        , elig_device_metrics as (
            -- For eligible devices, pull their whole history
            SELECT DISTINCT
                d.device_id,
                d.device_first_seen_ts,
                d.device_first_view_ts,
                d.platform,
                d.platform_type,
                d.ds,

                -- For filtering devices
                {cumul_filter_metric} as daily_filter_metric,

                -- For calculating metrics
                d.tvt_sec,
                d.signup_or_registration_activity_count,
                d.visit_total_count
            FROM tubidw.device_metric_daily as d
            JOIN elig_devices as e
                ON d.device_id = e.device_id
        )

        , elig_device_cumul_filter as (
            SELECT *, sum(daily_filter_metric) OVER (PARTITION BY device_id, platform_type, platform ORDER BY ds rows between unbounded preceding and current row) as cumul_filter_metric
            FROM elig_device_metrics
        )

        , elig_devices2 as (
            SELECT device_id
            FROM elig_device_cumul_filter

            -- cumulative metric filters dynamically populate below 
            WHERE 1=1                                  
            {metric_filter_where}                      -- for > (greater than) filters, we can use "where"
            -- AND cumul_filter_metric >= 3600.0       -- example: at least 60 mins of cumulative TVT

            GROUP BY 1                                 -- using a group by instead of distinct opens up filtering with "having"

            HAVING 1=1                                 
            {metric_filter_having}                     -- for < (less than) filters, need to use a "having" filter with an aggregation on the metric
            -- AND max(cumul_filter_metric) <= 3600.0  -- example: less than 60 mins of cumulative TVT
        )
        """
        return amh_filter_query
    
    
    def make_sql_where_string(self, field, condition, value):
        if field == 'no filters': 
            return ''
        else: 
            if (condition == '<') | (condition == '<=') | (condition == 'BETWEEN'):
            # for < (less than) filters, need to use a "having" filter with an aggregation on the metric
            # for now, the only aggregation is "MAX" but might want to open up to others in the future
                return 'AND ' + 'MAX(' + field + ')' + ' ' + condition + ' ' + value + ''
            else:
                return 'AND ' + field + ' ' + condition + ' ' + value + ''
    
    
    def set_metric_filter_sql_inputs(self, metric_sql):
        # initialize sql strings
        cumul_metric_str = metric_sql_having = metric_sql_where = ''

        if (metric_sql.children[1].value == '<') | (metric_sql.children[1].value == '<=') | (metric_sql.children[1].value == 'BETWEEN'):
            cumul_metric_str = metric_sql.children[0].value
            metric_sql_having = metric_sql.result
        else:
            cumul_metric_str = 0
            metric_sql_where = metric_sql.result
        
        return [metric_sql_where, cumul_metric_str, metric_sql_having]

    
    def generate_filter_cte(self, attribute_sql, metric_sql):
#         base_query = getattr(self, 'base_amh_query')()
#         base_query_inputs = getattr(self, 'set_metric_filter_sql_inputs')(metric_sql)
        base_query = self.base_amh_query()
        base_query_inputs = self.set_metric_filter_sql_inputs(metric_sql)
        
        elig_devices2 = base_query.format(attr_filter = attribute_sql.result,
                                          metric_filter_where = base_query_inputs[0],
                                          cumul_filter_metric = base_query_inputs[1],
                                          metric_filter_having = base_query_inputs[2])
        
        
        return elig_devices2
    

