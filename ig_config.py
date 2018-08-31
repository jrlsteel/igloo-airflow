api_config = dict(          
                    max_api_calls  = 1, # max api calls - to be used along with allowed_period_in_secs
                    allowed_period_in_secs = 1, # max api calls in n secs - to be used along with max_api_calls
                    total_no_of_calls = 2, # total no of api calls in one run
                    account_ids = [7571], # account id for testing
                    connection_timeout = 5, # how much time it can wait before it stops execution
                    retry_in_secs = 2 # retry in n secs
)
s3_config = dict(
                    access_key = 'AKIAJPW5K6TTI5WV4KIQ',
                    secret_key = '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'
)

