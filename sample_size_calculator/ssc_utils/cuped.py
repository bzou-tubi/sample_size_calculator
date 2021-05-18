class cuped(object):

    def generate_cuped_cte(self, event2_condition_interact):
        """
        Generates the SQL CTEs that go through CUPED calculations. Should always be the last CTE in the final SQL string. 
            
        Returns: String
        """
        
        if event2_condition_interact.value[0] == 'no event filter':
            sample_multiplier = '1.0'
        else:
            sample_multiplier = '1000.0'
            
        base_cuped_query = """
            -- Cuped values
            , cuped_values_1 AS (
              SELECT
                *,
                AVG(metric_covariate) OVER (PARTITION BY metric_name, platform_type) AS before_covariate_average,
                AVG(metric_result) OVER (PARTITION BY metric_name, platform_type) AS after_covariate_average,
                STDDEV(metric_covariate) OVER (PARTITION BY metric_name, platform_type) AS covariate_standard_dev,
                AVG(metric_covariate) OVER (PARTITION BY metric_name) AS before_covariate_average_total,
                AVG(metric_result) OVER (PARTITION BY metric_name) AS after_covariate_average_total,
                STDDEV(metric_result) OVER (PARTITION BY metric_name) AS covariate_standard_dev_total
              FROM metrics
            )

            , cuped_values_2 AS (
              SELECT
                *,
                AVG(metric_covariate) OVER (PARTITION BY metric_name, platform) AS before_covariate_average,
                AVG(metric_result) OVER (PARTITION BY metric_name, platform) AS after_covariate_average,
                STDDEV(metric_covariate) OVER (PARTITION BY metric_name, platform) AS covariate_standard_dev
              FROM metrics
              WHERE platform in ('ROKU','AMAZON','IPHONE','IPAD','ANDROID','SONY','PS4','COMCAST','VIZIO','XBOXONE','SAMSUNG','COX')
            )

            , cuped_data_1 AS (
              SELECT
                metric_name,
                AVG(metric_covariate) AS covariate_mean,
                1.0 * SUM((metric_covariate - before_covariate_average_total)*(metric_result - after_covariate_average_total)) / NULLIF(STDDEV(metric_covariate)*STDDEV(metric_covariate) * COUNT(*), 0) AS theta
              FROM cuped_values_1
              GROUP BY 1
            )

            , cuped_data_2 as (
              SELECT
                metric_name,
                platform_type,
                AVG(metric_covariate) AS covariate_mean,
                1.0 * SUM((metric_covariate - before_covariate_average)*(metric_result - after_covariate_average)) / NULLIF(STDDEV(metric_covariate)*STDDEV(metric_covariate) * COUNT(*), 0) AS theta
              FROM cuped_values_1
              GROUP BY 1, 2
            )

            , cuped_data_3 as (
              SELECT
                metric_name,
                platform,
                AVG(metric_covariate) AS covariate_mean,
                1.0 * SUM((metric_covariate - before_covariate_average)*(metric_result - after_covariate_average)) / NULLIF(STDDEV(metric_covariate)*STDDEV(metric_covariate) * COUNT(*), 0) AS theta
              FROM cuped_values_2
              GROUP BY 1, 2
            )

            , cuped_metrics_1 as (
              SELECT
                device_id,
                a.metric_name,
                metric_result,
                metric_covariate,
                COALESCE(metric_result - (metric_covariate - covariate_mean) * theta, metric_result) AS cuped_result
              FROM cuped_data_1 AS a
                INNER JOIN cuped_values_1 b
                  ON a.metric_name = b.metric_name
            )

            , cuped_metrics_2 as (
              SELECT
                device_id,
                a.metric_name,
                a.platform_type AS platform,
                metric_result,
                metric_covariate,
                COALESCE(metric_result - (metric_covariate - covariate_mean) * theta, metric_result) AS cuped_result
              FROM cuped_data_2 AS a
                INNER JOIN cuped_values_1 AS b
                  ON a.platform_type = b.platform_type
                  AND a.metric_name = b.metric_name
            )

            , cuped_metrics_3 as (
              SELECT
                device_id,
                a.metric_name,
                a.platform,
                metric_result,
                metric_covariate,
                COALESCE(metric_result - (metric_covariate - covariate_mean) * theta, metric_result) AS cuped_result
              FROM cuped_data_3 AS a
                INNER JOIN cuped_values_2 AS b
                  ON a.platform = b.platform
                  AND a.metric_name = b.metric_name
            )

            , cuped_results AS (
                SELECT metric_name,
                        'ALL' as platform,
                        count(distinct device_id) size,
                        avg(metric_result) result_avg,
                        STDDEV(metric_result) result_std,
                        avg(cuped_result) avg_cuped_result,
                        STDDEV(cuped_result) std_cuped_result
                FROM cuped_metrics_1
                GROUP BY  1, 2

                UNION ALL

                SELECT metric_name,
                       platform,
                       COUNT(distinct device_id) size,
                       AVG(metric_result) result_avg,
                       STDDEV(metric_result) result_std,
                       AVG(cuped_result) avg_cuped_result,
                       STDDEV(cuped_result) std_cuped_result
                FROM cuped_metrics_2
                GROUP BY 1, 2

                UNION ALL

                SELECT metric_name,
                       platform,
                       COUNT(distinct device_id) size,
                       AVG(metric_result) result_avg,
                       STDDEV(metric_result) result_std,
                       AVG(cuped_result) avg_cuped_result,
                       STDDEV(cuped_result) std_cuped_result
                FROM cuped_metrics_3
                GROUP BY 1, 2
            )

            SELECT 
                   metric_name,
                   platform,
                   size * {sampling} AS observations,
                   avg_cuped_result,
                   std_cuped_result
            FROM cuped_results        
            """
        
        return base_cuped_query.format(sampling = sample_multiplier)