
# coding: utf-8

# ## Update Script

# In[81]:

from ShapeFileToSocrata import *
import logging
from retry import retry
from EmailerLogger import *


# In[73]:

input_dir_shp = '/home/ubuntu/workspace/src_files/'
config_inputdir = '/home/ubuntu/workspace/configs/'
fieldConfigFile = 'fieldConfig.yaml'


# In[74]:

cI =  ConfigItems(config_inputdir ,fieldConfigFile  )
configItems = cI.getConfigs()
sc = SocrataClient(config_inputdir, configItems)
client = sc.connectToSocrata()
clientItems = sc.connectToSocrataConfigItems()
lte = logETLLoad(config_inputdir, configItems)


# In[75]:

scrud = SocrataCRUD(client, clientItems, configItems)
sms = ShpMetaData(client, configItems )
ss = ShpToSocrata()
sss = ShpShemaToSocrata(ss, configItems )
sds = ShpDataToSocrata(client, ss,configItems )
shpio = shpIO(configItems)
fourXFour = configItems['fourXFour']
oldfourXFour = configItems['oldfourXFour'] 
datasets_migration_flag  = configItems['datasets_migration_flag']
meta_csv = configItems['pubtracker']
lg = pyLogger(configItems)
lg.setConfig()


# In[76]:

metaSchema, datasets = sms.make_headers_and_rowobj(config_inputdir, "weekly" + meta_csv)


# In[77]:

for dataset in datasets:
    print "***************"
    print dataset['Name']
    print "****************"


# In[78]:

for dataset in datasets:
    print "***************"
    print dataset['Name']
    print "****************"
    dataset = sms.getMetaData(dataset, datasets_migration_flag)
    if len(dataset[fourXFour]) == 9:
        if (shpio.downloadShp(input_dir_shp + "current/" , dataset)):
            dataset = sds.postShapeData(input_dir_shp, dataset, scrud)
            print "******"
            print dataset['isLoaded'] + ":" + dataset[fourXFour]
        #clean up files
        shpio.removeShpFiles(input_dir_shp + "current/")
    print dataset
    print "*************************************************"
    print "*************************************************"

msg = lte.sendJobStatusEmail(datasets)
client.close()


# In[ ]:



