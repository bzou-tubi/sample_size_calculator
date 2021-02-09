import tubi_data_runtime as tdr
import pandas as pd

class filter_generator(object):
    """
    Contains a set of functions that generates the SQL CTEs that filter and give a list of eligible device_ids based on user-specified conditions. 

    There are 3 level of filter granularity, from easiest to hardest:
        - device_metric_daily
        - all_metric_hourly
        - analytics_richevent (can be estimated with sampled_analytics_thousandth)

    There are also 3 types of filters, from easiest to hardest:
        - attributes (ie. specific country, platform, signed in users, etc.)
        - metrics (ie. devices with at least 1 hour of TVT)
            - This requires grabbing a device's whole history, and calculating cumulative sums of the metric we desire to filter
            - There is also difficulty in putting the filtering code in the correct place within the SQL
                - For example, for > metric filters, such as "at least 1 hour TVT", we can use the cumulated metric in the "where" clause. 
                - For < metric filters however, we must use a max(cumulated metric) and put the filter in the "having" clause.  
        - events (ie. event_name = 'PlayProgressEvent', etc.)
            - For events filtering, require the user to specify the event_name no matter what
            - There are also sub-conditions that can be associated to any event (ie. page_type)
    """
    
    ##### Choices #####
    
    def choose(self, choice):
        return choice
    
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
        """List of attributes available for filtering (from all_metric_hourly)""" 
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
                'content_type',
                'tvt_sec' # note: here tvt_sec is treated as an attribute rather than a cumulative metric
            ]
        return filter_attributes 

    def filter_metrics_choices(self):
        """List of metrics available for filtering (from all_metric_hourly)"""         
        cols = pd.Series(tdr.get_catalog().tubidw.all_metric_hourly.columns)
        filter_metrics = ['no filters'] + cols[cols.str.endswith(tuple(['_count', '_sec']))].tolist()
        return filter_metrics 
    
    def event_name_choices(self): 
        """List of event_names available for filtering (from sampled_analytics_thousandth)"""
        # TODO: possible to make this faster by preloading it in the import?
        
        query = """
            SELECT DISTINCT event_name
            FROM tubidw.sampled_analytics_thousandth
            WHERE date >= dateadd('day',-7,GETDATE())
        """
        df = tdr.query_redshift(query).to_df()
        return ['no event filter'] + pd.Series(df['event_name']).sort_values().tolist()
    
    def event_sub_cond_field_choices(self):
        """List of events level attributes available for filtering (from sampled_analytics_thousandth)""" 
        event_fields = [
                'no filters',
                'content_completion_pct',
                'component__left_nav_section',
                'component__utility_tile__id',
                'dest_page__category_slug',
                'content_id',
                'program_id',
                'page_type',
                'dest_page_type',
                'container_id',
                'container_slug',
                'query',
                'manip',
                'auth_type',
                'current_auth_type',
                'status'
            ]
        return event_fields 
    

    ##### Base Queries #####
    
    def amh_attr_filter_query(self):
        """
        Returns a string containing a SQL CTE with a list of device_ids to be joined later.  
        
        The resulting string has 1 input that can be specified by the user: 
            attr_filter
        """
        
        attr_filter_query = """
        WITH start_devices as (
            -- Pull list of devices that were active (has any row; don't need TVT >0) in the past 4 weeks
            SELECT DISTINCT device_id
            FROM tubidw.all_metric_hourly
            WHERE DATE_TRUNC('week',hs) >= dateadd('week',-4,DATE_TRUNC('week',GETDATE()))
            AND DATE_TRUNC('week',hs) < DATE_TRUNC('week',GETDATE())
            {attr_filter} -- attribute filters dynamically populate here
            -- TODO: currently can't get a metric/attribute combo filter, like "devices that watched at least 50% of a specific content_id"
        )
        """
        return attr_filter_query
        
            
    def dmd_metric_filter_query(self):            
        """
        The resulting string has 3 input that can be specified by the user: 
            metric_filter_where
            cumul_filter_metric
            metric_filter_having
        """ 
        
        metric_filter_query = """
        , elig_device_metrics as (
            -- For eligible devices, pull their whole history
            SELECT 
                d.device_id,
                d.device_first_seen_ts,
                d.device_first_view_ts,
                d.platform,
                d.platform_type,
                d.ds,

                -- For filtering devices
                sum({cumul_filter_metric}) as daily_filter_metric,

                -- For calculating metrics
                sum(d.tvt_sec) as tvt_sec,
                sum(d.user_signup_count) as user_signup_count,
                sum(d.device_registration_count) as device_registration_count,
                sum(d.signup_or_registration_activity_count) as signup_or_registration_activity_count,
                sum(d.visit_total_count) as visit_total_count
            FROM tubidw.device_metric_daily as d
            JOIN start_devices as e
                ON d.device_id = e.device_id
            GROUP BY 1,2,3,4,5,6
        )

        , elig_device_cumul_filter as (
            SELECT *, sum(daily_filter_metric) OVER (PARTITION BY device_id, platform_type, platform ORDER BY ds rows between unbounded preceding and current row) as cumul_filter_metric
            FROM elig_device_metrics
        )

        , elig_devices as (
            SELECT device_id
            FROM elig_device_cumul_filter

            -- cumulative metric filters dynamically populate below 
            WHERE 1=1
            {metric_filter_where}
            -- example: 
            -- AND cumul_filter_metric >= 3600.0      -- at least 60 mins of cumulative TVT

            GROUP BY 1

            HAVING 1=1
            {metric_filter_having}
            -- example: 
            -- AND max(cumul_filter_metric) <= 3600.0 -- less than 60 mins of cumulative TVT
        )
        """
        return amh_filter_query
    
    def events_sessionized_query(self):
        """One input: attr_filter"""
            
        sessionized_sql = """
        WITH start_devices AS ( 
          -- had to change this CTE name; for some reason "events" throws an error on Redshift... no idea why it works on Periscope
          -- TODO: figure out how to auto-sync this with our Periscope snippets 
          SELECT
            round((position/1000.0)/duration, 2) as content_completion_pct,
            a.device_id,
            a.user_id,
            a.platform,
            case
              when UPPER(a.platform) in (
                'IPHONE', 'IPAD', 'ANDROID', 'FIRETABLET', 'ANDROID-SAMSUNG', 'ANDROID_SAMSUNG', 'FOR_SAMSUNG', 'IOS_WEB', 'IOS') then 'MOBILE'
              when UPPER(a.platform) in (
                'WEB') then 'WEB'
              else
                'OTT'
            end
            as platform_type, -- TODO: figure out how to make this auto-sync with DBT: http://dw-docs.production-public.tubi.io/ux/#!/macro/macro.core_metrics.platform_type
            a.device_first_seen_ts as device_first_seen_ts,
            a.ts,
            a.event_name,
            a.component__left_nav_section,
            a.component__utility_tile__id,
            a.dest_page__category_slug,
            a.content_id,
            case when a.content_type = 'EPISODE' then a.content_series_id else a.content_id end as program_id,
            a.page_type,
            a.dest_page_type,
            a.container_id,
            a.container_slug,
            a.query,
            a.manip,
            a.auth_type,
            a.current_auth_type,
            a.status,
            NVL(content_series_id, content_id) as start_video_content_id,
            LEAD(a.ts, 1) OVER (PARTITION BY a.device_id ORDER BY a.ts ASC) as next_time,
            LEAD(a.event_name, 1) OVER (PARTITION BY a.device_id ORDER BY a.ts ASC) as next_event_name,
            LEAD(a.page_type, 1) OVER (PARTITION BY a.device_id ORDER BY a.ts ASC) as next_source_page_type,

            CASE WHEN next_time > a.ts + interval '30 minutes' then 1 else 0 end as session_counter -- TODO: make the interval user-specified

          FROM tubidw.sampled_analytics_thousandth a
          WHERE DATE_TRUNC('week',ts) >= dateadd('week',-4, DATE_TRUNC('week',GETDATE()))
                    AND DATE_TRUNC('week',ts) < DATE_TRUNC('week',GETDATE())

          {attr_filter} -- attribute filters dynamically populate here
        )

        , sessionized_events AS (
          SELECT
            *,
            1 + coalesce(sum(session_counter) OVER (PARTITION BY device_id ORDER BY ts rows between UNBOUNDED preceding and 1 PRECEDING), 0) as session_num
          FROM next_events
        )
        """
        return sessionized_sql


    def events_2step_window_query(self):
        """
        Two inputs: 
          condition1
          condition2
        """

        window_sql = """
        , window as (
          -- main query to get condition 2 status (funnel output)
          select
            *,
            first_value(case when {condition2} and has_condition1 then 1 else null end IGNORE NULLS) over
            (partition by device_id, session_num order by ts asc rows
            between unbounded preceding and CURRENT ROW) as has_condition1_condition2,
            first_value(case when {condition2} then 1 else null end IGNORE NULLS) over
            (partition by device_id, session_num order by ts asc rows
            between unbounded preceding and CURRENT ROW) as has_condition2,
            first_value(case when {condition2} then ts else null end IGNORE NULLS) over
            (partition by device_id, session_num order by ts asc rows
            between unbounded preceding and CURRENT ROW) as ts_condition2

          from
            -- subquery to get condition 1 status (funnel input)
            (select
              *,
              first_value(case when {condition1} then 1 else null end IGNORE NULLS) over 
              (partition by device_id, session_num order by ts asc rows 
              between unbounded preceding and CURRENT ROW) as has_condition1,
              first_value(case when {condition1} then ts else null end IGNORE NULLS) over
              (partition by device_id, session_num order by ts asc rows
              between unbounded preceding and CURRENT ROW) as ts_condition1
            from sessionized_events
            )

            -- TODO: can we save time by filtering the events in question? 
            -- Does not save much time with sampled_analytics_thousandth 
            -- WHERE {condition1} 
            -- AND {condition2} 
        )
        """
        return window_sql


    def events_summarized_session_query(self):
        """
        One input: 
          time_interval
        """

        summ_session_sql = """
        , base_summarized_sessions AS (
          SELECT
            device_id,
            session_num,
            -- Aggregate values
            MAX(CASE WHEN has_condition1_condition2 THEN 1 ELSE 0 END) > 0 AS has_condition1_condition2,
            MAX(CASE WHEN DATEDIFF('second', ts_condition1, ts_condition2) <= COALESCE( {time_interval}, 999999) THEN 1 ELSE 0 END) > 0 AS time_condition,
            SUM(has_condition1) AS sum_condition1,
            SUM(has_condition2) AS sum_condition2,
            SUM(CASE WHEN has_condition1_condition2 THEN 1 ELSE 0 END) AS sum_both_conditions,
            SUM(CASE WHEN has_condition1 THEN 1 ELSE 0 END) >= SUM(has_condition1_condition2) AS condition1_before_condition2,
            SUM(CASE WHEN has_condition1 THEN 1 ELSE 0 END) - SUM(CASE WHEN has_condition1_condition2 THEN 1 ELSE 0 END) AS condition_difference
          FROM window
          GROUP BY 1, 2
        )

        , summarized_session as (
          SELECT
            *,

            sum_condition1 > 0 AS has_condition1,
            sum_condition2 > 0 AS has_condition2,

            CASE WHEN has_condition1_condition2 AND condition1_before_condition2 THEN TRUE ELSE FALSE END AS has_both_conditions,
            CASE WHEN has_condition1_condition2 AND time_condition AND condition1_before_condition2 THEN TRUE ELSE FALSE END AS has_both_conditions_time

            -- NOTE: Steps interval is commented outs for simplicity. Can add this functionality back if there is a use case in the future.
            -- MAX(CASE WHEN condition_difference <= COALESCE( {steps_interval}, 999999) THEN 1 ELSE 0 END) > 0 AS steps_condition,
            -- CASE WHEN has_condition1_condition2 AND steps_condition AND condition1_before_condition2 THEN TRUE ELSE FALSE END AS has_both_conditions_steps,
            -- CASE WHEN has_condition1_condition2 AND steps_condition AND time_condition AND condition1_before_condition2 THEN TRUE ELSE FALSE END AS has_both_conditions_time_steps
          
          FROM
            base_summarized_sessions
          GROUP BY
            1, 2, 3, 4, 5, 6, 7, 8, 9
        )
        
        , elig_devices as ( 
            SELECT device_id
            FROM summarized_session
            GROUP BY 1
            HAVING BOOL_OR(has_both_conditions_time)
        )
            
        """
        return summ_session_sql
    
    
    
    ##### Helper Functions #####
    # These get used in the notebook to transform user-generated inputs into SQL code
    
    def make_sql_condition_string(self, field, condition, value, filter_type):
        """
        Generates a SQL string with a WHERE/HAVING/CASE WHEN conditional. field, condition, and value to be specified by the user.
        
        Args:
            field: the column name of the attribute/metric the user wants to filter on
            condition: the filtering condition for the field (ie. <, =, BETWEEN, etc.)
            value: the value to filter on for the condition (need quotes around strings)
            filter_type: either 'metric', 'attribute', or 'event'
        
        Returns: string with SQL "where", "having", or "case when" conditional (example output: "tvt_sec > 3600")
        """

        if field == 'no filters': 
            return ''
        else: 
            if filter_type in ('metric', 'attribute', 'event'):
                # for < (less than) filters on metrics, we need to use a "having" filter with an aggregation (only MAX for now) on the metric
                if (filter_type == 'metric') & (condition in ('<', '<=', 'BETWEEN')):
                    return 'AND ' + 'MAX(' + field + ')' + ' ' + condition + ' ' + value + ''
                else:
                    return 'AND ' + field + ' ' + condition + ' ' + value + ''
            else:
                raise NotImplementedError()
                
    def make_sql_event_condition_string(self, event_names, sub_condition_sql):
        """
        Generates a SQL string with an event filtering conditional to be inputted into a CASE WHEN statement.
        
        Args:
            event_name: string to specify the event to filter for (user specified)
            sub_condition_sql: string with sub-condition associated to the event (user specified and transformed by make_sql_condition_string)
        
        Returns: string (example output: "event_name = 'PlayProgressEvent' AND content_completion_pct <= 0.70")
        """

        if event_names[0] == 'no event filter': 
            return 'TRUE'
        else:
            placeholders = ','.join(['%s'] * len(event_names))
            in_events = "{placeholders}".format(placeholders = event_names).replace(',)', ')')
            return "event_name IN " + in_events + " " + sub_condition_sql
            
        
    
    def set_metric_filter_sql_inputs(self, metric_sql_interact):
        """
        Generates a 3-element list that categorizes the metric filter inputed as a "HAVING" or "WHERE" filter. 
        This allows us to put the metric filter in the correct place when forming our filter SQL string. 
        
        Args:
            metric_sql_interact: interactive object (user generated)
        
        Returns: 
            A 3-element list with:
                metric_sql_where: string with full SQL where condition (ie. user_id IS NOT NULL)  
                cumul_metric_str: string field to be used in cumulative filtering (tvt_sec). Need separate so SQL can pre-cumulate the desired metric.
                metric_sql_having: string with full SQL having condition (ie. tvt_sec > 3600)  
            Any/all of these 3 elements can be null/empty if the user does not specify a filter. 
        """
        
        cumul_metric_str = metric_sql_having = metric_sql_where = ''

        if metric_sql_interact.children[1].value in ('<', '<=', 'BETWEEN'):
            cumul_metric_str = metric_sql_interact.children[0].value
            metric_sql_having = metric_sql_interact.result
        else:
            cumul_metric_str = 0
            metric_sql_where = metric_sql_interact.result
        
        return [metric_sql_where, cumul_metric_str, metric_sql_having]

    
    ##### CTE Generator Function #####
    # This glues everything together and generates a CTE with a list of eligible device_ids 

    def generate_nonevent_filter_cte(self, attribute_sql, metric_sql):
        """
        Generates a string, containing a set of SQL CTEs that combines all filtering conditions. 
        The final CTE elig_devices is a list of device_ids eligible under the user-specified filtering conditions. 
        
        Args:
            attribute_sql: interactive object (user generated)
            metric_sql: interactive object (user generated)
        """
        base_query = self.amh_attr_filter_query()
        base_query_inputs = self.set_metric_filter_sql_inputs(metric_sql)
        
        elig_devices = base_query.format(attr_filter = attribute_sql.result, 
                                          metric_filter_where = base_query_inputs[0], 
                                          cumul_filter_metric = base_query_inputs[1], 
                                          metric_filter_having = base_query_inputs[2]) 
        
        
        return elig_devices
    
    def generate_event_filter_cte(self, attribute_sql, condition1, condition2, time_interval):
        sessionized_query = self.events_sessionized_query().format(attr_filter = attribute_sql.result)
        window_query = self.events_2step_window_query().format(condition1 = condition1, 
                                                               condition2 = condition2)
        summ_session_query = self.events_summarized_session_query().format(time_interval = time_interval.result, steps_interval = 'NULL')

        elig_devices = sessionized_query + window_query + summ_session_query
        
        return elig_devices
