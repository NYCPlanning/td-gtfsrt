import requests
import zipfile
import os



#path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2019/GTFS-RT/'
#path='C:/Users/Y_Ma2/Desktop/GTFS-RT/'
path='/home/mayijun/GTFS-RT/'



#months=['201808','201809','201810','201811','201812','201901','201902','201903','201904']
months=['201808','201809']
for m in months:
#    url='https://s3.amazonaws.com/gtfsarchive/Data/'+str(m)+'.zip'
#    with requests.get(url, stream=True) as r:
#        r.raise_for_status()
#        with open(path+'Archive/'+str(m)+'.zip', 'wb') as f:
#            for chunk in r.iter_content(chunk_size=100): 
#                if chunk: # filter out keep-alive new chunks
#                    f.write(chunk)
    zip_ref=zipfile.ZipFile(path+'Archive/'+str(m)+'.zip','r')
    zip_ref.extractall(path+'Archive/'+str(m))
    zip_ref.close()
    for i in sorted(os.listdir(path+'Archive/'+str(m))):
        if i.endswith('.zip'):
            zip_ref=zipfile.ZipFile(path+'Archive/'+str(m)+'/'+str(i),'r')
            zip_ref.extractall(path+'Archive/'+str(m)+'/'+str(i.replace('.zip','')))
            zip_ref.close()