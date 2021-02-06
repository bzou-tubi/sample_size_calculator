import tubi_data_runtime as tdr
import pandas as pd

class filter_generator(object):
    """
    Contains a set of functions that generates the SQL CTEs that filter and give a list of eligible device_ids based on user-specified conditions. 
    Should always be the first CTE in the final SQL string. 

    There are 3 levels of filtering available, from easiest to hardest:
        - device_metric_daily
        - all_metric_hourly
        - analytics_richevent (can be estimated with sampled_analytics_thousandth)

    There are also 3 types of device filters, from easiest to hardest:
        - attributes (ie. country, platform, signed in users, etc.)
        - metrics (ie. devices with at least 1 hour of TVT)
            - This requires grabbing a device's whole history, and calculating cumulative sums of the metric we desire to filter
            - There is also difficulty in putting the filtering code in the correct place within the SQL
            - For example, for > metric filters, such as "at least 1 hour TVT", we can use the cumulated metric in the "where" clause. 
            - For < metric filters however, we must use a max(cumulated metric) and put the filter in the "having" clause.  
        - events (ie. event_name = )
    """

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
        metric_conditions.remove('=')
        return metric_conditions 

    def filter_attributes_choices(self):
        """
        List of attributes available for filtering (from all_metric_hourly)
        """ 
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
        """
        List of metrics available for filtering (from all_metric_hourly)
        """         
        cols = pd.Series(tdr.get_catalog().tubidw.all_metric_hourly.columns)
        filter_metrics = ['no filters'] + cols[cols.str.endswith(tuple(['_count', '_sec']))].tolist()
        return filter_metrics 
    
    def base_amh_query(self):
        """
        Returns a SQL CTE string, with the last CTE containing a list of device_ids to be joined later.  
        
        The resulting string has 4 inputs that can be specified by the user: 
            attr_filter
            metric_filter_where
            cumul_filter_metric
            metric_filter_having
        """
        
        amh_filter_query = """
        WITH elig_devices as (
            -- Pull list of devices that were active (has any row; don't need TVT >0) in the past 4 weeks
            -- Using all_metric_hourly for additional filters
            SELECT DISTINCT device_id
            FROM tubidw.all_metric_hourly
            WHERE DATE_TRUNC('week',hs) >= dateadd('week',-4,DATE_TRUNC('week',GETDATE()))
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
            JOIN elig_devices as e
                ON d.device_id = e.device_id
            GROUP BY 1,2,3,4,5,6
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
    
    
    def make_sql_where_string(self, field, condition, value, filter_type = 'attribute'):
        """
        Generates a SQL string with a WHERE/HAVING/CASE WHEN conditional.
        These inputs will be specified by the user. Should work for both attribute and metric filters.        
        
        Args:
            field: the column name of the attribute/metric the user wants to filter on
            condition: the filtering condition for the field (ie. <, =, BETWEEN, etc.)
            value: the value to filter on for the condition (need quotes around strings)
        
        Returns: 
            String to insert a conditional into a "where", "having", or "case when" section of a SQL query.
            For example, this will return something like: "tvt_sec > 3600"
        """

        if field == 'no filters': 
            return ''
        else: 
            # for < (less than) filters, need to use a "having" filter with an aggregation on the metric
            # for now, the only aggregation is "MAX" but might want to open up to others in the future

            if (filter_type == 'metric') & (condition in ('<', '<=', 'BETWEEN')):
                return "AND " + 'MAX(' + field + ')' + ' ' + condition + ' ' + value + ''
            elif filter_type in ('attribute', 'event'):
                return "AND " + field + ' ' + condition + ' ' + value + ''
            else:
                return NotImplementedError()
    
    
    def set_metric_filter_sql_inputs(self, metric_sql_interact):
        """
        Generates a 3-element list that categorizes the metric filter inputed into here as a "HAVING" or "WHERE" filter. 
        This allows us to put the metric filter in the correct place when forming our filter SQL string. 
        
        Args:
            metric_sql_interact: interactive object (user generated)
        
        Returns: 
            A 3-element list with:
                metric_sql_where: string with full SQL where condition (ie. user_id IS NOT NULL)  
                cumul_metric_str: string field to be used in cumulative filtering (tvt_sec). Necessary to have this separate so the SQL can pre-cumulate the desired metric.
                metric_sql_having: string with full SQL having condition (ie. tvt_sec > 3600)  
            Any of the elements can be null/empty, if the user does not specify a filter. 
        """
        
        cumul_metric_str = metric_sql_having = metric_sql_where = ''

        if metric_sql_interact.children[1].value in ('<', '<=', 'BETWEEN'):
            cumul_metric_str = metric_sql_interact.children[0].value
            metric_sql_having = metric_sql_interact.result
        else:
            cumul_metric_str = 0
            metric_sql_where = metric_sql_interact.result
        
        return [metric_sql_where, cumul_metric_str, metric_sql_having]

    
    def generate_filter_cte(self, attribute_sql, metric_sql):
        """
        Generates a string, containing a set of SQL CTEs that combines all filtering conditions. 
        The final CTE elig_devices2 should be a list of device_ids eligible under the filtering specifications. 
        
        Args:
            attribute_sql: interactive object (user generated)
            metric_sql: interactive object (user generated)
        
        Returns: 
            string
        
        TODO: MAKE THIS DYNAMIC TO TAKE EITHER AMH OR EVENTS FILTERING
        """
        base_query = self.base_amh_query()
        base_query_inputs = self.set_metric_filter_sql_inputs(metric_sql)
        
        elig_devices2 = base_query.format(attr_filter = attribute_sql.result, 
                                          metric_filter_where = base_query_inputs[0], 
                                          cumul_filter_metric = base_query_inputs[1], 
                                          metric_filter_having = base_query_inputs[2]) 
        
        
        return elig_devices2
