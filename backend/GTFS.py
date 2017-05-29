import urllib2
import auth
import os
import glob
import zipfile
import MySQLdb
import warnings

warnings.filterwarnings('ignore', category=MySQLdb.Warning)

class GTFS:
    
    def __init__(self, conn, db, url, directory, filename):
        self.ftp_url = url
        self.directory = directory
        self.filename = filename
        self.conn = conn
        self.db = db
     
    def executeQuery(self, query, params=()):
        self.conn.execute(query, params)
        return self.conn.fetchall()
        
    def fullUpdate(self):
        self.clearSqlGtfs()
        self.getGtfs()
        self.uploadGtfs()
        
    def delFromDir(self,path):
        filelist = glob.glob(path)
        for f in filelist:
            os.remove(f)
            
    def getGtfs(self):
        if not os.path.exists(self.directory):
          os.makedirs(self.directory)
        self.delFromDir(self.directory + "/*")
        response = urllib2.urlopen(self.ftp_url)
        zipcontent = response.read()
        with open(self.directory+"/"+self.filename, 'w') as f:
            f.write(zipcontent)
        self.extractZip(self.directory+"/"+self.filename, self.directory)
        
    def clearSqlGtfs(self):
        self.conn.execute('delete from trips;')
        self.conn.execute('delete from routes;')
        self.conn.execute('delete from shapes;')
        self.conn.execute('delete from stops;')
        self.conn.execute('delete from stop_times;')
        self.db.commit()
        
    def extractZip(self,in_path, out_path):
        zip_file = zipfile.ZipFile(in_path, 'r')
        zip_file.extractall(out_path)
        zip_file.close()
        
    def loadFile(self,conn, name, table, col_str):
        conn.execute('LOAD DATA LOCAL INFILE "/root/dev/spartaride_backend/backend/gtfs/'+name+'"INTO TABLE '+table+' FIELDS TERMINATED BY "," IGNORE 1 LINES '+col_str)
        self.db.commit()
        
    def uploadGtfs(self):
        self.loadFile(self.conn,"shapes.txt","shapes","")
        self.loadFile(self.conn,"trips.txt","trips","(route_id,service_id,trip_id,head_sign,@col5,direction,block_id,shape_id,@col9,@col10)")
        self.loadFile(self.conn,"stops.txt","stops","(id,code,name,description,latitude,longitude)")
        self.loadFile(self.conn,"routes.txt","routes","(id,@col2,@col3,name,@col5,@col6,@col7,color,@col9)")
        self.loadFile(self.conn,"stop_times.txt","stop_times","(trip_id,arrival,departure,stop_id,stop_sequence,@col6,@col7,@col8,@col9)")