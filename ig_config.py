api_config = dict(          
                    max_api_calls  = 1, # maximum number of api calls allowed in n(allowed_period_in_secs) seconds ## switch off: 0
                    allowed_period_in_secs = 0, # time period within which n(max_api_calls) are allowed. ## switch off: 0
                    total_no_of_calls = 20000, # total no of api calls in one run
                    connection_timeout = 5, # how much time it can wait before it stops execution in seconds. 
                    retry_in_secs = 2 # retry every n seconds if there is a connection timeout error
)
s3_config = dict(
                    access_key = 'AKIAJPW5K6TTI5WV4KIQ',
                    secret_key = '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'
)
rds_config = dict(
                    host = 'localhost',
                    user = 'igloo_readonly',    
                    db = 'igloo',
                    pwd = 'hX4M2S4vj2v5qKb8qQqbLIFYMBoSvGZa',
                    port = 22                          
)
redshift_config = dict (
                    host = 'localhost',
                    user = 'igloo',    
                    db = 'igloosense',
                    pwd = 'tL2ZqlSOukYg',
                    port = 22     

)
test_config = dict(
                    enable_manual = 'N', # If enabled, uses the account ids from test_config[account_ids]  
                    account_ids = [10855], # run for one or list of account ids for testing.
                    enable_file = 'N', # If enabled, uses the account ids from s3 bucket
                    enable_db = 'Y', # If enabled, fetches the account ids RDS mysql db
                    account_ids_sql = 'select s.external_id from supply_contracts s limit 5' # Sql to fetch from rds db.
)