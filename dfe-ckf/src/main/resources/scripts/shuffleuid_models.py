import sys
import os
import json
import subprocess

temp_dir = sys.argv[1]
modelPath = sys.argv[2]
temp_out = sys.argv[3]


uniqueIdMap = {}

for r,_,files in os.walk(temp_dir):
  for _file in files:
     f = open('%s/%s'%(temp_dir,_file))
     models = json.loads(f.readlines()[0])     
     models = json.loads(models)     

    
     for k,v in models.items():
        splitKey = k.split("_")
        unique_id = splitKey[0]
        item_id = splitKey[1]      

        if unique_id not in uniqueIdMap:
           uniqueIdMap[unique_id] = {item_id:v}           
        else:
           _map = uniqueIdMap.get(unique_id)
           _map[item_id] = v

     f.close()

for k,v in uniqueIdMap.items():
   f = open("%s/%s.json"%(temp_out,k),"w")
   jstr = json.dumps(v)
   f.write(jstr)
   f.close()
