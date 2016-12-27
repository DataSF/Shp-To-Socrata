
# coding: utf-8

# ## Update Script

#!/usr/bin/env python
from ConfigUtils import *
from ShapeFileToSocrata import *
from EmailerLogger import *
from optparse import OptionParser
from ConfigUtils import *

#handle update schedule--> we have the following update schedules
#daily, weekly, monthly, quartly, annually, as needed
# will make an option for each. 

helpmsg = 'Use the -u option plus update schedule. Update Schedule choices are daily, weekly, monthly, quarterly, annual, asNeeded'
parser = OptionParser(usage='usage: %prog [options] ')
parser.add_option('-u', '--updateSchedule',
                      type='choice',
                      action='store',
                      dest='updateSchedule',
                      choices=['daily', 'weekly', 'monthly', 'quarterly', 'annual', 'asNeeded'],
                      default=None,
                      help=helpmsg ,)
(options, args) = parser.parse_args()
if  options.updateSchedule is None:
    print "ERROR: You must indicate an update schedule for the geodatasets!"
    print helpmsg
    exit(1)

updateSchedule = options.updateSchedule


input_dir_shp = '/home/ubuntu/geoShps/src_files/'
config_inputdir = '/home/ubuntu/geoShps/configs/'
fieldConfigFile = 'fieldConfig.yaml'


cI =  ConfigItems(config_inputdir ,fieldConfigFile  )
configItems = cI.getConfigs()
sc = SocrataClient(config_inputdir, configItems)
client = sc.connectToSocrata()
clientItems = sc.connectToSocrataConfigItems()
lte = logETLLoad(config_inputdir, configItems)


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
isLoaded =  configItems['isLoaded']
lg = pyLogger(configItems)
lg.setConfig()
counter = 0

metaSchema, datasets = sms.make_headers_and_rowobj(config_inputdir, updateSchedule + meta_csv)

print "****************UPDATING GEODATASETS******************"
for dataset in datasets:
    print "***************"
    print dataset['Name']
    print "****************"
    dataset = sms.getMetaData(dataset, datasets_migration_flag)
    if ((len(dataset[fourXFour]) == 9) and (dataset['isLoaded'] == 'success')):
        if (shpio.downloadShp(input_dir_shp + "current/" , dataset)):
            try:
	    	dataset = sds.postShapeData(input_dir_shp, dataset, scrud)
            	print "******"
            	print dataset['isLoaded'] + ":" + dataset[fourXFour]
        	#clean up files
        	shpio.removeShpFiles(input_dir_shp + "current/")
        	counter = counter + 1
	    except Exception, e:
		print str(e)
    else:
        print "not an line or polygon dataset"
        datasets.pop(counter)
    print "*************************************************"

print "****************FINAL RESULTS************************************"
msg = lte.sendJobStatusEmail(datasets, updateSchedule)
client.close()




