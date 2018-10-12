import os
#Process All Ensek data
os.system('python processEnsecApiData.py')
os.system('python processEnsekApiInternalReadingsData.py')
os.system('python processEnsekApiInternalEstimatesData.py')
os.system('python processEnsekApiTariffsWithHistory.py')
os.system('python processEnsekApiDirectDebits.py')
os.system('python processEnsekApiStatusRegistrationsData.py')

