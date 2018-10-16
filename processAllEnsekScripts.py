import os
#Process All Ensek data
while True:
    os.system('python3 processEnsecApiData.py')
    os.system('python3 processEnsekApiInternalReadingsData.py')
    os.system('python3 processEnsekApiInternalEstimatesData.py')
    os.system('python3 processEnsekApiTariffsWithHistory.py')
    os.system('python3 processEnsekApiDirectDebits.py')
    os.system('python3 processEnsekApiStatusRegistrationsData.py')

