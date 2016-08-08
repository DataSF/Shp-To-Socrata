
# coding: utf-8

# In[1]:

import geopandas as gpd
import fiona
import inflection
import pandas as pd
import re
import csv
import inflection
import re
import datetime
import os
from __future__ import division
import requests
from sodapy import Socrata
import yaml
import base64
import itertools
import datetime
import bson
import json
import time 
import logging
from retry import retry
from shapely.geometry import mapping, shape
import urllib2
import zipfile
from fiona.crs import from_epsg
from retry import retry
from shapely.geometry import Polygon


# In[2]:

def filterDictList( dictList, keysToKeep):
    return  [ {key: x[key] for key in keysToKeep if key in x.keys() } for x in dictList]

class ConfigItems:
    def __init__(self, inputdir, fieldConfigFile):
        self.inputdir = inputdir
        self.fieldConfigFile = fieldConfigFile
        
    def getConfigs(self):
        configItems = 0
        with open(self.inputdir + self.fieldConfigFile ,  'r') as stream:
            try:
                configItems = yaml.load(stream)
            except yaml.YAMLError as exc:
                print(exc)
        return configItems


class SocrataClient:
    def __init__(self, inputdir, configItems):
        self.inputdir = inputdir
        self.configItems = configItems
        
    def connectToSocrata(self):
        clientConfigFile = self.inputdir + self.configItems['socrata_client_config_fname']
        with open(clientConfigFile,  'r') as stream:
            try:
                client_items = yaml.load(stream)
                client = Socrata(client_items['url'],  client_items['app_token'], username=client_items['username'], password=base64.b64decode(client_items['password']))
                return client
            except yaml.YAMLError as exc:
                print(exc)
        return 0
    
    def connectToSocrataConfigItems(self):
        clientConfigFile = self.inputdir + self.configItems['socrata_client_config_fname']
        with open(clientConfigFile,  'r') as stream:
            try:
                clientItems = yaml.load(stream)
                return clientItems
            except yaml.YAMLError as exc:
                print(exc)
        return 0


# In[3]:

class SocrataCRUD:
    
    def __init__(self, client, clientItems, configItems):
        self.client = client
        self.chunkSize = 1000
        self.geotype = configItems['geotype_field']
        self.row_id = configItems['row_id_field']
        self.name = configItems['dataset_name_field']
        self.description = configItems['description_field']
        self.category = configItems['category_field']
        self.fourXFour = configItems['fourXFour']
        self.rowsInserted = configItems['dataset_records_cnt_field'] 
        self.shp_records = configItems['src_records_cnt_field']
        self.clientItems = clientItems
        self.isLoaded = configItems['isLoaded']

    @retry( tries=5, delay=1, backoff=2)   
    def createGeodataSet(self, dataset, cols):
        if dataset[self.geotype] == 'Point':
            new_backend = False
        else:
            new_backend = True
        row_id = None
        if dataset[self.row_id]:
            row_id = dataset[self.row_id]
            #really need to check to make sure that row id exists....
        socrata_dataset = self.client.create(dataset[self.name], description=dataset[self.description], columns=cols, tags=dataset["tags"], category=dataset[self.category], new_backend=new_backend, row_identifier=row_id)

        try:
            #socrata_dataset = self.client.create(dataset[self.name], description=dataset[self.description], columns=cols, tags=dataset["tags"], category=dataset[self.category], new_backend=new_backend, row_identifier=row_id)
            fourXFour = str(socrata_dataset['id'])
            dataset[self.fourXFour] = fourXFour
            print "4x4 "+ dataset[self.fourXFour]
        except:
            print "*****ERROR*******"
            print dataset
            dataset['Dataset URL'] = ''
            dataset[self.fourXFour] = 'Error: did not create dataset'
            print "4x4 "+ dataset[self.fourXFour]
            print "***************"
        del dataset['tags']
        return dataset
    
    def insertGeodataSet(self, dataset, insertDataSet):
        insertChunks = self.makeChunks(insertDataSet)
        #overwrite the dataset on the first insert chunk[0] if there is no row id
        if dataset[self.rowsInserted] == 0 and dataset[self.row_id ] == '':
            rejectedChunk = self.replaceDataSet(dataset, insertChunks[0])
            if len(insertChunks) > 1:
                for chunk in insertChunks[1:]:
                    rejectedChunk = self.insertData(dataset, chunk)
        else:
            print "upserting..."
            for chunk in insertChunks:
                rejectedChunk = self.insertData(dataset, chunk)
        return dataset
    
    
    @retry( tries=10, delay=1, backoff=2)
    def replaceDataSet(self, dataset, chunk):
        result = self.client.replace( dataset[self.fourXFour], chunk ) 
        dataset[self.rowsInserted] = dataset[self.rowsInserted] + int(result['Rows Created'])
        time.sleep(0.25)
        
        
    @retry( tries=10, delay=1, backoff=2)
    def insertData(self, dataset, chunk):
        result = self.client.upsert(dataset[self.fourXFour], chunk) 
        dataset[self.rowsInserted] = dataset[self.rowsInserted] + int(result['Rows Created'])
        time.sleep(0.25)
       

    def makeChunks(self, insertDataSet):
        return [insertDataSet[x:x+ self.chunkSize] for x in xrange(0, len(insertDataSet), self.chunkSize)]
    
    
    def postDataToSocrata(self, dataset, insertDataSet ):
        if dataset[self.fourXFour]!= 0:
            dataset = self.insertGeodataSet(dataset, insertDataSet)
            dataset = self.checkCompleted(dataset)
        else: 
            print "dataset does not exist"
        return dataset
    
    def checkCompleted(self, dataset):
        if dataset[self.rowsInserted] == dataset[self.shp_records]:
            dataset[self.isLoaded] = 'success'
        else:
            dataset[self.isLoaded] = 'failed'
        if dataset[self.isLoaded] == 'failed':
            dataset[self.rowsInserted] = self.getRowCnt(dataset)
            if dataset[self.rowsInserted] == dataset[self.shp_records]:
                dataset[self.isLoaded] = 'success' 
        if dataset[self.isLoaded] == 'success':
            print "data insert success!" + " Loaded " + str(dataset[self.rowsInserted]) + "rows!"
        return dataset
    
    def getRowCnt(self, dataset):
        time.sleep(1)
        qry = '?$select=count(*)'
        qry = "https://"+ self.clientItems['url']+"/resource/" +dataset[self.fourXFour]+ ".json" + qry
        r = requests.get( qry , auth=( self.clientItems['username'],  base64.b64decode(self.clientItems['password'])))
        cnt =  r.json()
        print cnt
        return int(cnt[0]['count'])
        


# In[4]:

class ShpMetaData:
    def __init__(self, client, configItems):
        self.client = client
        self.oldfourXFour = configItems['oldfourXFour'] 
        self.data_src_url =  configItems['data_src_url_field']
        self.category =  configItems['category_field']
        self.description =  configItems['description_field']
        self.shp_records = configItems['src_records_cnt_field']
        self.rowsInserted = configItems['dataset_records_cnt_field']
        self.dataset_src_url =  configItems['data_src_url_field']
        
    @staticmethod
    def make_headers_and_rowobj(inputdir,  fname, keysToKeep=None ):
        with open(inputdir+ fname, 'rb') as inf:
            dictList = []
            reader = csv.DictReader(inf)
            for row in reader:
                dictList.append(row)
            schemaLayout = dictList[0].keys()
            if(keysToKeep == None):
                keysToKeep = schemaLayout
            useful = filterDictList(dictList, keysToKeep) 
        return schemaLayout,useful
    
    def writeMetaData(self, inputdir, fname, datasets):
        keys = datasets[0].keys()
        with open(inputdir+ "new" + fname, 'wb') as output_file:
            dict_writer = csv.DictWriter(output_file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(datasets)
            
    def getMetaData(self, dataset, shp_migration=False):
        '''gets metadata off socrata or a spreadsheet. uses the optional shp_migration field. If migration is true, then will hit the old dataset on socrata, else will hit spreadsheet'''
        if shp_migration:
            dataset_metaData = self.client.get_metadata(dataset[self.oldfourXFour])
            dataset_metaData_keys = dataset_metaData.keys()
            metadata_keys = dataset_metaData['metadata'].keys()
            if "additionalAccessPoints" in metadata_keys:
                dataset[self.data_src_url] =  dataset_metaData['metadata']['additionalAccessPoints'][0]['urls']['zip']
            if ("accessPoints" in metadata_keys) and ( "additionalAccessPoints" not in  metadata_keys):
                accessPointKeys = dataset_metaData['metadata']['accessPoints'].keys()
                if "zip" in accessPointKeys:
                    dataset[self.data_src_url] =  dataset_metaData['metadata']['accessPoints']['zip']
                if "zipped shapefile" in accessPointKeys: 
                     dataset[self.data_src_url] =  dataset_metaData['metadata']['accessPoints']['zipped shapefile']        
            if "tags" in dataset_metaData_keys:
                dataset['tags'] = dataset_metaData['tags']
            else:
                dataset['tags'] = []
            dataset[self.category] = dataset_metaData['category']
            if "description" in dataset_metaData_keys:
                dataset[self.description] =  dataset_metaData['description']
        else:
            dataset['tags'] = []
        return dataset


# In[5]:

class ShpToSocrata:
    def __init__(self):
        pass
    
    def readShpFile(self, input_dir, fname):
        #need to make sure that cols are lower
        shp = gpd.read_file(input_dir+ fname)
        cols =  shp.columns.values.tolist()
        colsLower = [col.lower() for col in cols]
        shp.rename(columns=dict(zip(cols,colsLower)), inplace=True)
        return shp
    
    @staticmethod
    def getShapeCols(shp): 
        return shp.columns.values.tolist()


# In[6]:

class ShpShemaToSocrata(ShpToSocrata):
    
    def __init__(self,  ShpToSocrata, configItems ):
        self.publishing_dept = configItems['publishing_dept_field']
    
    def makeDatasetColsShp(self, shp):
        name_map = { 'int64': 'number', 'float64' : 'number', 'Polygon': 'polygon', 'MultiPolygon': 'polygon', 'LineString':'line', 'Polyline': 'multiline',  'Line':'line'}
        shpCols = self.getShapeCols(shp)
        cols = []
        for col in shpCols:
            scol = {}
            colType = shp[col].dtype.name
            if colType != 'object':
                scol['fieldName'] =  col
                scol['name'] = col
                scol['dataTypeName'] = name_map[colType]
            else:
                if col != 'geometry':
                    scol['fieldName'] =  col
                    scol['name'] = col
                    scol['dataTypeName'] = "text"
                else:    
                    scol['fieldName'] =  col
                    scol['name'] = col
                    scol['dataTypeName'] = name_map[shp.geom_type[0]]
            cols.append(scol)
            #add a boolean flag for multipolygons that are being exploded
            if scol['name'] == 'geometry' and ( scol['dataTypeName'] == 'polygon' or scol['dataTypeName'] == 'line'):
                scolMulti = {}
                scolMulti['fieldName'] =  'multigeom'
                scolMulti['name'] = 'multigeom'
                scolMulti['dataTypeName'] = 'checkbox'
                cols.append(scolMulti)
        return cols
    
    def makeSchemaShp(self, input_dir_shp, dataset, socrataCRUD):
        shp = self.readShpFile(input_dir_shp, 'current')
        cols = self.makeDatasetColsShp(shp)
        #print cols
        dataset['tags'] = [dataset[self.publishing_dept]] + dataset['tags']
        dataset = socrataCRUD.createGeodataSet(dataset, cols)
        return dataset


# In[7]:

class ShpDataToSocrata(ShpToSocrata):
    
    def __init__(self, client,  ShpToSocrata, configItems ):
        self.rowsInserted = configItems['dataset_records_cnt_field']
        self.shp_records = configItems['src_records_cnt_field']
        self.client = client
        self.fourXFour = configItems['fourXFour']

    @staticmethod
    def getShpSize(shp):
        return len(shp.index)
    
    def getShpCol(self, shp):
        '''dynamically gets the name of the shape/geom column'''
        shp_cols = self.getShapeCols(shp)
        #shp_col = [ col for col in shp_cols if shp[col].dtype.name == 'object' and shp[col] ]
        #shp_col = [ col for col in shp_cols if shp[col] == 'geometry' ]
        shp_col = ['geometry']
        if not len(shp_col) == 1:
            raise MyException
        shp_col = shp_col[0]
        return shp_col
    
    
    @staticmethod
    def isNullGeom(shp):
        '''tests the df for empty shapes in the rows'''
        if len(shp[shp['geometry'].isnull()]) > 0:
            return True
        return False
    
    @staticmethod
    def getNullShpGeom(shp):
        '''returns rows in df with Null Geometry'''
        nullShp =  shp[shp['geometry'].isnull()].reset_index(drop=True)
        return nullShp
    
    @staticmethod
    def getNotNullShpGeom(shp):
        '''returns rows in df with NOT Null Geometry'''
        return shp[shp['geometry'].notnull()].reset_index(drop=True)
    
    @staticmethod
    def reprojectShp(shp):
        '''reproject shp data epsg=4326 aka web mercator '''
        try:
            shp = shp.to_crs(epsg=4326)
        except:
            print "didn't reproject shape properly"
        return shp
    
    def getShpData(self, shp, shp_col):
        '''tests to see if there is null geom. If so it splits out the df 
           and then reprojects to espg 4326 and then transforms it into a dictionarylist;  '''
        shp_dictList = None
        if self.isNullGeom(shp):
            print "in here has null geom"
            #get the shapes with null geom
            shpNullGeom = self.getNullShpGeom(shp)
            shpNullGeom_dictList = shp.to_dict('records')
            shpNullGeom_dictList = self.setGeomToNone(shpNullGeom_dictList)
            
            #get the shapes with NOT null geom
            shpNotNullGeom = self.getNotNullShpGeom(shp)
            shpNotNullGeom = self.reprojectShp(shpNotNullGeom)
            #explode the multi polygons, return geojson
            shpNotNullGeom_dictListJson = self.getShpGeoJson(shpNotNullGeom, shp_col)
            
            #combine everything 
            shp_dictList =  shpNotNullGeom_dictListJson + shpNullGeom_dictList
        else:
            print "all geoms have vals"
            shp = self.reprojectShp(shp)
            shp_dictList = self.getShpGeoJson(shp, shp_col)
        return shp_dictList
    
    def getShpGeoJson(self, shp, shp_col):
        '''
          map the geom in dictlist to geojson; explode multipolygons if they exist
        '''
        shp_dictList = shp.to_dict('records')
        isNotPolyShp = self.getShpDatasetType(dataset)
        if isNotPolyShp:
            print "in here: exploding multi-polygons"
            shp_dictList = self.explodeMulti(shp_dictList)
        for shape_row in shp_dictList:
            shape_row[shp_col] = json.dumps(mapping(shape_row[shp_col]))
            shape_row[shp_col] = json.loads(shape_row[shp_col])
        return shp_dictList
    
    def postShapeData(self,  input_dir_shp, dataset, socrataCRUD):
        shp = self.readShpFile(input_dir_shp, "current")
        shp_col = self.getShpCol(shp)
        shp_dictList = self.getShpData(shp, shp_col)
        dataset[self.shp_records] = len(shp_dictList)
        dataset[self.rowsInserted] = 0
        dataset = socrataCRUD.postDataToSocrata(dataset, shp_dictList)
        return dataset
    

   
    def getShpDatasetType(self, dataset):
        '''checks dataset to make sure its not a multipolygon so it can it can explode it'''
        newDatasetMetaData = self.client.get_metadata(dataset[ self.fourXFour])
        geom_column_info = [ item for item in newDatasetMetaData['columns'] if item['name'] == 'geometry'  or item['name'] == 'shape' ]
        if (geom_column_info[0]['dataTypeName'] == 'multipolygon') or (geom_column_info[0]['dataTypeName'] == 'multiline'):
            return False
        else:
            return True
   
    @staticmethod
    def setGeomToNone(shp_dict):
        newShp_dict = []
        for shp_item in shp_dict:
            shp_item.pop("geometry", None)
            newShp_dict.append(shp_item)
        return newShp_dict
                
            
        
    @staticmethod
    def explodeMulti(shp_dict):
        '''explode multi-shapes if they exist'''
        explodedShapes = []
        counter = 0
        for shp_item in shp_dict:
            pattern = re.compile("(multi+)|(Multi+)")
            matchObj = False
            if not( shp_item['geometry'] is None):
                matchObj = re.search(pattern, shp_item['geometry'].geom_type)
            if matchObj:
                multi = shp_item['geometry']
                for pol in multi:
                    newShpItem = shp_item.copy()
                    newShpItem.pop("geometry", None)
                    newShpItem['geometry'] = pol
                    newShpItem['multigeom'] = True
                    explodedShapes.append(newShpItem)
            else:
                shp_item['multigeom'] = False
                explodedShapes.append(shp_item)
        return explodedShapes


# In[8]:

class shpIO:
    def __init__(self, configItems):
        self.dataset_src_url =  configItems['data_src_url_field']
    
    @staticmethod
    def downloadFile(url, path, fname):
        print url
        if url:
            attempts = 0
            while attempts < 3:
                try:
                    response = urllib2.urlopen(url, timeout = 5)
                    content = response.read()
                    f = open( path+fname, 'wb' )
                    f.write( content )
                    f.close()
                    if os.path.exists(path+ fname):
                        return True
                    else:
                        print "ERROR: Something weird happened: did not download shp file"
                        return False
                except urllib2.URLError as e:
                    attempts += 1
                    print type(e)
                    print "Could not download shp files"
        else:
            print "EEROR: URL for SRC Download is missing!"
        return False
        
    @staticmethod
    def unzipShpFile(path, fname):
        if os.path.exists(path+ fname):
            try:
                zf = zipfile.ZipFile(path+ fname)
                zf.extractall(path)
                return True
            except:
                print "ERROR: Could not unzip shapeFile"
        return False
        
    
    @staticmethod
    def getFileCnt(path):
        return len(os.listdir( path ))

    def downloadShp(self, path, dataset):
        if self.downloadFile(dataset[self.dataset_src_url], path, "shape.zip"):
            if self.unzipShpFile(path, "shape.zip"):
                self.moveShpFilesToCurrentDir(path)
                if self.getFileCnt(path) > 2:
                    print "Files were downloaded"
                    return True
                else:
                    print "ERROR: something happened! Shp files aren't there!"
                    return False
        return False
    

    def removeShpFiles(self, path):
        '''removes files from current directory'''
        filelist = [ path+f for f in os.listdir(path) ]
        for f in filelist:
            if os.path.isdir(f):
                os.rmdir(f)
            else:
                if os.path.isfile(f):
                    os.remove(f)
        if self.getFileCnt(path) == 0:
            return True
        else:
            return false
    
    @staticmethod
    def moveShpFilesToCurrentDir( path):
        '''moves files into a particular directory'''
        for root, directories, filenames in os.walk(path):
            for filename in filenames: 
            #split the file to get the name, returns a tuple of the path, filename
                dirFname = os.path.split(os.path.join(root,filename))
                fn =  dirFname[1]
                if not(os.path.isfile(path + fn)):
                    #move the file to the current path
                    os.rename(os.path.join(root,filename), path +fn)
              


# In[79]:

input_dir_shp = '/home/ubuntu/workspace/src_files/'
config_inputdir = '/home/ubuntu/workspace/configs/'
meta_csv = 'geo_data_list_short.csv'
fieldConfigFile = 'fieldConfig.yaml'


# In[80]:

cI =  ConfigItems(config_inputdir ,fieldConfigFile  )
configItems = cI.getConfigs()
sc = SocrataClient(config_inputdir, configItems)
client = sc.connectToSocrata()
clientItems = sc.connectToSocrataConfigItems()


# In[81]:

scrud = SocrataCRUD(client, clientItems, configItems)
sms = ShpMetaData(client, configItems )
ss = ShpToSocrata()
sss = ShpShemaToSocrata(ss, configItems )
sds = ShpDataToSocrata(client, ss,configItems )
shpio = shpIO(configItems)
fourXFour = configItems['fourXFour']
oldfourXFour = configItems['oldfourXFour'] 
datasets_migration_flag  = configItems['datasets_migration_flag']


# In[82]:

metaSchema, datasets = sms.make_headers_and_rowobj(config_inputdir, meta_csv)


# In[83]:

for dataset in datasets[72:73]:
    print "***************"
    print dataset['Name']
    print "****************"


# In[84]:

for dataset in datasets[72:73]:
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
sms.writeMetaData(config_inputdir, meta_csv, datasets)


# In[72]:

#cool = client.get_metadata('qcxd-tp4u')
#print cool





# In[14]:

#shp[shp.isnull().any(axis=1)]
#print len(shp[shp['geometry'].isnull()])
#shp[shp['geometry'].notnull()]

#shp = self.readShpFile(input_dir_shp, "current")
#shp_col = self.getShpCol(shp)
#shp_dictList = self.getShpData(shp, shp_col)


# In[13]:

#shp = sds.readShpFile(input_dir_shp, "current")
#shp_dict = sds.getShpData(shp)
#explodeMulti(shp_dict)


# In[67]:

#qry = '?$select=count(*)'
#qry = "https://"+ clientItems['url']+"/resource/" +"bmjt-kdwv"+ ".json" + qry
#r = requests.get( qry , auth=(clientItems['username'],  base64.b64decode(clientItems['password'])))
#cnt =  r.json()
print cnt
#


# In[262]:

#client.get("srq6-hmpi")


# In[ ]:



