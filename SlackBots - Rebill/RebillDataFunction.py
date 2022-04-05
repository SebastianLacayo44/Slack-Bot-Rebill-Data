### Rebill Function for Slack Command

def rebillsdata():
    """
    * Mapped out current amount of rebills for both dfs
    * Added projected next rebill count for both dfs
    * Filter out people who cancelled before March 1st for both dfs
    * Filter out people who won't be rebilled within 120 days of March 1st for both dfs
    * Keep latest rebill only
    * Validate both dfs
    * Create a function with all of the above
    * Apply the renewal table
    """

    import pandas  as pd
    import numpy as np
    import pyodbc
    from datetime import datetime, timedelta

    pd.options.mode.chained_assignment = None


    # access azsqldb

    server='giddy-salesforce.database.windows.net'
    database='salesforce-data'
    username='sqladmin'
    password='{1209Giddy}'   
    driver='{ODBC Driver 17 for SQL Server}'

    conn = pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)

    ### Queries

    # Query for table containing unique subscriptions 

    query_unqsubs =    """
                  SELECT [orderNumber]
                  ,[customerId]
                  ,[orderStatus]
                  ,[orderStatusId]
                  ,[createdDate]
                  ,[modifiedDate]
                  ,[shippedDate]
                  ,[productId]
                  ,[prodOrderId]
                  ,[productSKU]
                  ,[productName]
                  ,[email]
                  ,[subscriptionId]
                  ,[subOrderId]
                  ,[miniBCPersonId]
                  ,[miniBCProdSKU]
                  ,[subSignUpDate]
                  ,[subCancellationDate]
                  ,[subNextPaymentDate]
                  ,[subPauseDate]
                  ,[subfrq]
                  ,[subTotal]
                  ,[subStatus]
                  ,[subPayMethod]
                  ,[subProdTotal]
                  ,[uniqueEmail]
                  ,[instances]
                  ,[discountAmount]
                  ,[couponDiscount] 
                  FROM [dbo].[SebastianMiniBCView_SubscriptionUnique] 
                  WHERE [subscriptionId] IS NOT NULL AND
                        [productSKU] NOT LIKE '%RS%' AND
                        ([orderStatus] = 'Shipped' OR [orderStatus] = 'Awaiting Fulfillment')
                  """

    # Query for table where users have multiple subs

    query_mult =  """
              SELECT [orderNumber]
              ,[customerId]
              ,[orderStatus]
              ,[orderStatusId]
              ,[createdDate]
              ,[modifiedDate]
              ,[shippedDate]
              ,[productId]
              ,[prodOrderId]
              ,[productSKU]
              ,[productName]
              ,[email]
              ,[subscriptionId]
              ,[subOrderId]
              ,[miniBCPersonId]
              ,[miniBCProdSKU]
              ,[subSignUpDate]
              ,[subCancellationDate]
              ,[subNextPaymentDate]
              ,[subPauseDate]
              ,[subfrq]
              ,[subTotal]
              ,[subStatus]
              ,[subPayMethod]
              ,[subProdTotal]
              ,[uniqueEmail]
              ,[instances]
              ,[discountAmount]
              ,[couponDiscount] 
             FROM [dbo].[SebastianMiniBCView_SubscriptionDupes] 
             WHERE [subscriptionId] IS NOT NULL AND
                   [productSKU] NOT LIKE '%RS%' AND
                   ([orderStatus] = 'Shipped' OR [orderStatus] = 'Awaiting Fulfillment')
             """

    # Query for table containing failed rebill attempts

    query_ren = """
             SELECT [subscription_id]
             ,[order_id]
             ,[payment_status]
             ,[renewal_date]
             FROM [dbo].[MiniBCRenewals]
             """
    
    ### Query and import  the data

    df_unq = pd.read_sql_query(query_unqsubs, conn)
    df_mult =  pd.read_sql_query(query_mult, conn)
    df_ren = pd.read_sql_query(query_ren, conn)


    ### Function we may need later

    def avg(periods):
        averages = []
        for i in periods:
            avg = sum(i) / len(i)
            averages.append(avg/100)
        return averages

    # Define rebill percentages

    all_time_first = [39,39,48,44,50,43,45,44,47,38,36,32,40]
    all_time_second = [28,14,18,13,17,13,14,13,15]
    all_time_third = [7,5,6,5,6]
    all_time_fourth = [4]
    all_time_fifth = [2]
    all_time_else = [2]
    purgatory = [2]

    ### Function to prepare dataset

    def prep(df_unique, df_multiple, df_renewals, start_date, days_after):

        ### Adjust data types

        # Unique Dataset

        df_unique.loc[:,'createdDate'] = pd.to_datetime(df_unique.loc[:,'createdDate'])
        df_unique.loc[:,'subCancellationDate'] = pd.to_datetime(df_unique.loc[:,'subCancellationDate'])
        df_unique.loc[:,'subNextPaymentDate'] = pd.to_datetime(df_unique.loc[:,'subNextPaymentDate'])
        df_unique.loc[:,'subSignUpDate'] = pd.to_datetime(df_unique.loc[:,'subSignUpDate'])
        df_unique.loc[:,'shippedDate'] = pd.to_datetime(df_unique.loc[:,'shippedDate'])

        # Multiple Dataset

        df_multiple.loc[:,'createdDate'] = pd.to_datetime(df_multiple.loc[:,'createdDate'])
        df_multiple.loc[:,'subCancellationDate'] = pd.to_datetime(df_multiple.loc[:,'subCancellationDate'])
        df_multiple.loc[:,'subNextPaymentDate'] = pd.to_datetime(df_multiple.loc[:,'subNextPaymentDate'])
        df_multiple.loc[:,'subSignUpDate'] = pd.to_datetime(df_multiple.loc[:,'subSignUpDate'])
        df_multiple.loc[:,'shippedDate'] = pd.to_datetime(df_multiple.loc[:,'shippedDate'])

        # Add coupon column

        df_unique['discount'] = df_unique['discountAmount'].astype(float) + df_unique['couponDiscount'].astype(float)
        df_multiple['discount'] = df_multiple['discountAmount'].astype(float) +  df_multiple['couponDiscount'].astype(float)


        # Fix rows due to bad join on df multiple

        df_multiple = df_multiple.loc[df_multiple['shippedDate'] >= df_multiple['subSignUpDate']]
        df_multiple.reset_index(drop = True, inplace =True)

        # Impute "next" expected payment date values for cancellations

        df_unique["subNextPaymentDate"] = np.where((df_unique["subStatus"] == "inactive") & (df_unique['subfrq'] == 4), df_unique["createdDate"]  + pd.DateOffset(months=4), df_unique["subNextPaymentDate"])
        df_unique["subNextPaymentDate"] = np.where((df_unique["subStatus"] == "inactive") & (df_unique['subfrq'] == 6), df_unique["createdDate"]  + pd.DateOffset(months=6), df_unique["subNextPaymentDate"])
        df_multiple["subNextPaymentDate"] = np.where((df_multiple["subStatus"] == "inactive") & (df_multiple['subfrq'] == 4), df_multiple["createdDate"]  + pd.DateOffset(months=4), df_multiple["subNextPaymentDate"])
        df_multiple["subNextPaymentDate"] = np.where((df_multiple["subStatus"] == "inactive") & (df_multiple['subfrq'] == 6), df_multiple["createdDate"]  + pd.DateOffset(months=6), df_multiple["subNextPaymentDate"])
        
        # Fill feature with null values

        df_unique['cancellationDate'] = df_unique['subCancellationDate'].replace('', np.NaN)
        df_multiple['cancellationDate'] = df_multiple['subCancellationDate'].replace('', np.NaN)

        ### Count of rebills

        # Rebill count

        df_unique['order count'] = df_unique.sort_values(['subscriptionId', 'createdDate']).groupby(['subscriptionId']).cumcount()
        df_multiple['order count'] = df_multiple.sort_values(['subscriptionId', 'createdDate']).groupby(['subscriptionId']).cumcount()

        # Next rebill

        df_unique['next rebill'] = df_unique['order count'] + 1
        df_multiple['next rebill'] = df_multiple['order count'] + 1

        ### Establish filter parameters

        # Parameters for expected rebills (u = unique, m  = multiple)

        start_date_dt = datetime.strptime(start_date,"%Y-%m-%d")
        cutoff = start_date_dt + timedelta(days = days_after)
        cutoff_date = cutoff.strftime("%Y-%m-%#d")

        next_rebillstart_u = df_unique['subNextPaymentDate'].copy() >= datetime.today().strftime("%Y-%m-%#d")
        next_rebillend_u  = df_unique['subNextPaymentDate'].copy() <= cutoff_date

        next_rebillstart_m =  df_multiple['subNextPaymentDate'].copy() >= datetime.today().strftime("%Y-%m-%#d")
        next_rebillend_m  =  df_multiple['subNextPaymentDate'].copy() <= cutoff_date

        # Parameters for realized rebills

        from_date_u_realized = df_unique['createdDate'].copy() >= start_date 
        to_date_u_realized =  df_unique['createdDate'].copy() <= datetime.today().strftime("%Y-%m-%#d")
        rebills_only_u = df_unique['order count'].copy() != 0
        new_buyers_u = df_unique['order count'].copy() == 0

        from_date_m_realized = df_multiple['createdDate'].copy() >= start_date 
        to_date_m_realized =  df_multiple['createdDate'].copy() <= datetime.today().strftime("%Y-%m-%#d")
        rebills_only_m = df_multiple['order count'].copy() != 0
        new_buyers_m = df_multiple['order count'].copy() == 0

        # Apply the parameters 

        params_u = (next_rebillstart_u & next_rebillend_u)
        params_u_realized_rb = (from_date_u_realized & to_date_u_realized & rebills_only_u)
        params_u_realized_1st = (from_date_u_realized & to_date_u_realized & new_buyers_u)

        params_m = (next_rebillstart_m & next_rebillend_m)
        params_m_realized_rb = (from_date_u_realized & to_date_u_realized & rebills_only_m)
        params_m_realized_1st = (from_date_u_realized & to_date_u_realized & new_buyers_m)

        df_u = df_unique.loc[params_u].copy()
        df_u_realized_rb = df_unique.loc[params_u_realized_rb].copy()
        df_u_realized_1st = df_unique.loc[params_u_realized_1st].copy()

        df_m = df_multiple.loc[params_m].copy()
        df_m_realized_rb = df_unique.loc[params_m_realized_rb].copy()
        df_m_realized_1st = df_unique.loc[params_m_realized_1st].copy()

        # Keep only latest rebill 

        df_u = df_u.loc[df_u.groupby(['subscriptionId'])['order count'].idxmax()]
        assert df_u['customerId'].is_unique

        df_m = df_m.loc[df_m.groupby(['subscriptionId'])['order count'].idxmax()]

        # Concat the dfs

        frames_expected = [df_u, df_m]
        frames_realized_rb = [df_u_realized_rb,df_m_realized_rb]
        frames_realized_1st = [df_u_realized_1st, df_m_realized_1st]

        df_expected = pd.concat(frames_expected)
        df_realized_rb = pd.concat(frames_realized_rb)
        df_realized_1st = pd.concat(frames_realized_1st)

        df_expected.reset_index(drop = True, inplace = True)
        df_realized_rb.reset_index(drop = True, inplace = True)
        df_realized_1st.reset_index(drop = True, inplace = True)

        # Create renewal purgatory flag

        df_renewals = df_renewals.groupby('subscription_id').filter(lambda x: (x['order_id'] == '-').all())
        df_renewals.drop_duplicates(subset = ['subscription_id'], inplace = True)

        df_renewals['subscription_id'] = df_renewals['subscription_id'].astype(int)
        df_expected['subscriptionId'] = df_expected['subscriptionId'].astype(int)

        df_final = pd.merge(df_expected,df_renewals, how = 'left', left_on = 'subscriptionId', right_on = 'subscription_id')
        df_final.drop(['subscription_id','order_id','renewal_date'],axis = 1, inplace = True)

        df_final["next rebill"] = np.where(df_final["payment_status"] == "failed", 'f', df_final["next rebill"])

        # reformat dates

        start_date_full = datetime.strptime(start_date, '%Y-%m-%d').strftime('%B %#d, %Y')
        start_date =  datetime.strptime(start_date, '%Y-%m-%d').strftime('%B %#d')
        cutoff = datetime.strptime(cutoff_date, '%Y-%m-%d').strftime('%B %#d, %Y')

        # Expected revenue after adjustments 

        expected_revenue_series = (df_expected.groupby('next rebill')['subTotal'].sum())
        expected_amount = int(round(df_expected.groupby('next rebill')['subTotal'].sum().sum(),0)) 

        if expected_amount > 0:
            
            realized_rebills = int(round(df_realized_rb['subTotal'].sum(),0))
            first_sub_total  = int(round(df_realized_1st['subTotal'].sum(),0))
            discount_on_subs = int(round(df_realized_1st['discount'].sum(),0))
            realized_1st = first_sub_total - discount_on_subs

            new_subs = df_realized_1st['subscriptionId'].nunique() 

            periods = [all_time_first,all_time_second,all_time_third,all_time_fourth, all_time_fifth, all_time_else]

            rebill_avgs = pd.Series(avg(periods[:len(expected_revenue_series)]), expected_revenue_series.index.tolist(), name = 'rev%' )

            expected_rebill_df = pd.concat([expected_revenue_series, rebill_avgs], axis = 1)

            expected_rebill_df['expected'] = (expected_rebill_df['subTotal'] * expected_rebill_df['rev%'])

            # Print statements 

            today = datetime.today().strftime("%B %#d")

            date = "Period: {} to {}".format(start_date_full, cutoff)
            expected_rebills ="Expected Adjusted Rebill Revenue for the remaining Period: ${:,.0f}".format(int(round(expected_rebill_df['expected'].sum(),0)))
            realized_rebills = "Realized Rebill Revenue from {} to {}: ${:,.0f}".format(start_date, today, realized_rebills)
            new_subscribers = "New Subscriptions from {} to {}: {} for ${:,.0f}".format(start_date, today, new_subs, realized_1st)

            lst = [date, expected_rebills, realized_rebills, new_subscribers]
        
        elif expected_amount == 0:
            
            realized_rebills = int(round(df_realized_rb['subTotal'].sum(),0))
            first_sub_total  = int(round(df_realized_1st['subTotal'].sum(),0))
            discount_on_subs = int(round(df_realized_1st['discount'].sum(),0))
            realized_1st = first_sub_total - discount_on_subs
            new_subs = df_realized_1st['subscriptionId'].nunique() 

            # Print statements 

            today = datetime.today().strftime("%B %#d")

            date = "Period: {} to {}".format(start_date_full, cutoff)
            expected_rebills ="Expected Adjusted Rebill Revenue for the remaining Period: ${:,.0f}".format(0)
            realized_rebills = "Realized Rebill Revenue from {} to {}: ${:,.0f}".format(start_date, cutoff, realized_rebills)
            new_subscribers = "New Subscriptions from {} to {}: {} for ${:,.0f}".format(start_date, cutoff, new_subs, realized_1st)

            lst = [date, realized_rebills, new_subscribers]
        
        return lst
    
    data = prep(df_unq, df_mult, df_ren, start_date = '2022-03-1', days_after = 120)
    
    return data
