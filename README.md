# Sample Size Calculator (WIP)

[**Link to calculator**](https://tdr.production-public.tubi.io/user-redirect/lab/tree/shared/data_science/sample_size_calculator/sample_size_calc_prototype.ipynb)

Purpose: we want a version of the [Periscope dashboard](https://app.periscopedata.com/app/adrise:tubi/676521/(Official)-Experimentation-Sample-Size-Calculator), but with additional flexibility of filtering for a specific set of users. 

The very high level concept of this tool:
1. Dynamically generate a SQL query based on a set of user-generated inputs. Run the query on Redshift to pull into a dataframe.
2. Run through the sample size calculations (with the ability for the user to set custom parameters). Output a table that displays sample required for all chosen platforms. 

The bulk of the work is focused on adding flexibility to #1. This tool is WIP and there are many improvements to be done (in order of priority):
- Adding capability to have more than 1 filter in a specific filter type
- Adding flexibility for the user to define their own primary metrics


## Workflow (for internal)

### 1. User filtering
The goal of this section is to pull a list of specific users that are eligible for the experiment. This is where the bulk of our efforts will be focused. 
- We want this to be dynamic based on the complexity of any filters (see 3 levels below)
- We also want this to be interactable, to minimize the amount of adhoc SQL coding. It's daunting (and unscalable) for our stakeholders to alter the SQL in our current calculator to fit their specific needs. 

#### 3 different levels, in order of complexity (easiest to hardest):
1. device_metric_daily
    - all_metric_hourly covers the same metrics/attributes available for filtering, although device_metric_daily will be more performant
2. all_metric_hourly 
3. analytics_richevent
    
For our first prototype, we're only using all_metric_hourly to cover most filtering cases. To achieve a higher level of filtering flexibilty, we will include analytics_richevent. To reduce run times and processing work, we can include device_metric_daily.

#### In general, there are 3 ways to filter users:
1. Attributes (ie. ROKU+AMAZON, certain os, kids mode only, etc.)
2. Metrics (ie. watched at least 60 mins TVT, completed 3 movies, etc.)
3. Events (ie. user exited the video player after completing 70% of the content)

### 2. Raw user data
Catch-all CTE to pull a list of standard metrics of active devices in the last 4 weeks, from device_metric_daily. 
- In the future, we may want to improve this to allow flexibility for more complex metrics not available in device_metric_daily 
- ie. verification rates can only be calculated from analytics_richevent using is_confirmed = 't'

### 3. User data
Dynamic CTE that calculates a specific user-chosen metric.

### 4. Metric summary
Catch-all CTE that allows us to summarize/prep the data for CUPED.

### 5. CUPED
Catch-all CTE to calculate CUPED for all platforms, platform types, and all Tubi.
