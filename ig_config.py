api_config = dict(          
                    max_api_calls  = 1, # maximum number of api calls allowed in n(allowed_period_in_secs) seconds ## switch off: 0
                    allowed_period_in_secs = 0, # time period within which n(max_api_calls) are allowed. ## switch off: 0
                    total_no_of_calls = 20000, # total no of api calls in one run
                    account_ids = [], # run for one or list of account ids for testing. ## switch off: []
                    connection_timeout = 5, # how much time it can wait before it stops execution in seconds. 
                    retry_in_secs = 2 # retry every n seconds if there is a connection timeout error
)
s3_config = dict(
                    access_key = 'AKIAJPW5K6TTI5WV4KIQ',
                    secret_key = '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'
)

