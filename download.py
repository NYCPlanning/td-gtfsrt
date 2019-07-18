import requests
import zipfile


#path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2019/GTFS-RT/'
#path='C:/Users/Y_Ma2/Desktop/GTFS-RT/'
path='/home/mayijun/GTFS-RT/'

url='https://s3.amazonaws.com/gtfsarchive/Data/201905.zip'

with requests.get(url, stream=True) as r:
    r.raise_for_status()
    with open(path+'201905.zip', 'wb') as f:
        for chunk in r.iter_content(chunk_size=100): 
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)


zip_ref=zipfile.ZipFile(path+'201905.zip','r')
zip_ref.extractall(path+'201905')
zip_ref.close()
