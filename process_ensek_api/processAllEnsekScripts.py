
# Process All Ensek data
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from process_ensek_api.schema_validation import validateSchema as vs

from conf import config as con


if __name__ == '__main__':
    schema_valid_response = []
    account_ids = []
    '''Enable this to test for 1 account id'''
    if con.test_config['enable_manual'] == 'Y':
        account_ids = con.test_config['account_ids']

    if con.test_config['enable_db'] == 'Y':
        account_ids = vs.get_accountID_fromDB()
    schema_valid_response = vs.processAccounts(account_ids)

    for k in schema_valid_response:
        if not k['valid']:
            print(">>>>> Schema Validation Failed <<<<<<<<")
            print(k['api'] + ' has invalid schema for account id ' + str(k['account_id']))
            valid_schema = False
            break
        else:
            valid_schema = True

    if valid_schema:
        print(">>>> Meter Points <<<<")
        os.system('python3 processEnsekApiMeterPointsData.py')

        print(">>>> Internal Readings <<<<")
        os.system('python3 processEnsekApiInternalReadingsData.py')

        print(">>>> Internal Estimates <<<<")
        os.system('python3 processEnsekApiInternalEstimatesData.py')

        print(">>>> Tariff Histtory <<<<")
        os.system('python3 processEnsekApiTariffsWithHistory.py')

        print(">>>> Direct Debits <<<<")
        os.system('python3 processEnsekApiDirectDebits.py')

        print(">>>> Status Registrations <<<<")
        os.system('python3 processEnsekApiStatusRegistrationsData.py')
