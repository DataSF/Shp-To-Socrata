
# coding: utf-8

# ## Intial Script to Load ShapeFileDataToSocrata

# In[31]:

from ShapeFileToSocrata import *
from EmailerLogger import *


# In[32]:

input_dir_shp = '/home/ubuntu/workspace/src_files/'
config_inputdir = '/home/ubuntu/workspace/configs/'
meta_csv = 'geo_data_list_short.csv'
fieldConfigFile = 'fieldConfig.yaml'


# In[33]:

cI =  ConfigItems(config_inputdir ,fieldConfigFile  )
configItems = cI.getConfigs()
sc = SocrataClient(config_inputdir, configItems)
client = sc.connectToSocrata()
clientItems = sc.connectToSocrataConfigItems()


# In[34]:

scrud = SocrataCRUD(client, clientItems, configItems)
sms = ShpMetaData(client, configItems )
ss = ShpToSocrata()
sss = ShpShemaToSocrata(ss, configItems )
sds = ShpDataToSocrata(client, ss,configItems )
shpio = shpIO(configItems)
fourXFour = configItems['fourXFour']
oldfourXFour = configItems['oldfourXFour'] 
datasets_migration_flag  = configItems['datasets_migration_flag']


# In[35]:

metaSchema, datasets = sms.make_headers_and_rowobj(config_inputdir, meta_csv)


# In[36]:

for dataset in datasets[24:25]:
    print "***************"
    print dataset['Name']
    print "****************"


# In[37]:

for dataset in datasets[24:25]:
    print "***************"
    print dataset['Name']
    print "****************"
    dataset = sms.getMetaData(dataset, datasets_migration_flag)
    if (shpio.downloadShp(input_dir_shp + "current/" , dataset)):
        if len(dataset[fourXFour]) == 9:
            if dataset['isLoaded'] != 'success':
                print dataset['Name']
                dataset = sds.postShapeData(input_dir_shp, dataset, scrud)
                print dataset['fourXFour']
        else:
            dataset = sss.makeSchemaShp( input_dir_shp, dataset, scrud)
            print "******"
            print dataset[fourXFour]
            print "******"
            print "Created dataset successfullly; now inserting the data"
            if len(dataset[fourXFour]) > 0:
                dataset = sds.postShapeData(input_dir_shp, dataset, scrud)
                #publish the dataset and make it public
                client.publish(dataset[fourXFour])
                client.set_permission(dataset[fourXFour], "public")
                #print dataset 
                print "******"
                print dataset['isLoaded']
        #clean up files
    shpio.removeShpFiles(input_dir_shp + "/current/")
    print "*************************************************"
    print "*************************************************"
client.close()
msg = lte.sendJobStatusEmail(datasets)
sms.writeMetaData(config_inputdir, meta_csv, datasets)


# In[ ]:

#cool = client.get_metadata('ue7t-p29i')
#print cool['metadata']


# In[16]:

#qry = '?$select=count(*)'
#qry = "https://"+ clientItems['url']+"/resource/" +"rxim-5dpc"+ ".json" + qry
#r = requests.get( qry , auth=(clientItems['username'],  base64.b64decode(clientItems['password'])))
#cnt =  r.json()
#print cnt

