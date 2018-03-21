import urllib2
import auth
import os
import glob
import zipfile
import MySQLdb
import warnings

warnings.filterwarnings('ignore', category=MySQLdb.Warning)

class GTFS:
    
    
    
    def __init__(self, auth, directory, filename):
        self.ftp_url = auth.ftp_url
        self.directory = directory
        self.filename = filename
        self.db = self.getDatabase(auth)
        self.conn = self.getDatabaseConnection(self.db)
     
    def getDatabaseConnection(self,db):
        try:
            print("Attempting to get cursor(conn)")
            return db.cursor()
        except Exception, e:
            print("ERROR getting cursor(conn): " + str(e))
     
    def getDatabase(self,auth):
        db_host=auth.db_host
        db_user=auth.db_user
        db_pass=auth.db_pass
        db_db=auth.db_db
        try:
          print("Attempting to get database")
          db = MySQLdb.connect(host=db_host, user=db_user, passwd=db_pass, db=db_db)
          print("Connection successful")
          return db
        except Exception, e:
          print("ERROR connection to databse: " + str(e))
          exit()
     
    def executeQuery(self, query, params=()):
        self.conn.execute(query, params)
        return self.conn.fetchall()
        
    def fullUpdate(self):
        self.clearSqlGtfs()
        self.getGtfs()
        self.uploadGtfs()
        
    def delFromDir(self,path):
        try:
            print("Deleting files from " + str(path))
            filelist = glob.glob(path)
            for f in filelist:
                os.remove(f)
        except Exception, e:
            print("ERROR deleting files: " + str(e))
            
    def getGtfs(self):
        try:
            print("Checking for GTFS directory")
            if not os.path.exists(self.directory):
              print("Creating Directory")
              os.makedirs(self.directory)
        except Exception, e:
            print("ERROR checking/creating GTFS directory: " + str(e))
            
        self.delFromDir(self.directory + "/*")
        
        try:
            print("Attempting to download and extract GTFS zip")
            response = urllib2.urlopen(self.ftp_url)
            zipcontent = response.read()
            with open(self.directory+"/"+self.filename, 'w') as f:
                f.write(zipcontent)
            self.extractZip(self.directory+"/"+self.filename, self.directory)
        except Exception, e:
            print("ERROR download/extracting GTFS zip: " + str(e))
        
    def clearSqlGtfs(self):
        try:
            print("Clearing cata tables")
            self.conn.execute('delete from trips;')
            self.conn.execute('delete from routes;')
            self.conn.execute('delete from shapes;')
            self.conn.execute('delete from stops;')
            self.conn.execute('delete from stop_times;')
            self.db.commit()
            print("Cata tables cleared")
        except Exception, e:
            print("ERROR clearing cata tables: " + str(e))
        
    def extractZip(self,in_path, out_path):
        try:
            print("Attempting to extract zip")
            zip_file = zipfile.ZipFile(in_path, 'r')
            zip_file.extractall(out_path)
            zip_file.close()
        except Exception, e:
            print("ERROR, Failed to extract zip: " + str(e))
        
    def loadFile(self,conn, name, table, col_str):
        try:
            print("Attempting to load file " + str(name) + " into " + str(table))
            conn.execute('LOAD DATA LOCAL INFILE "/home/brandon/dev/pycata/gtfs/'+name+'"INTO TABLE '+table+' FIELDS TERMINATED BY "," IGNORE 1 LINES '+col_str)
            self.db.commit()
            print("Upload complete")
        except Exception, e:
            print("ERROR, Failed to upload file " + str(name) + " into " + str(table))
        
    def uploadGtfs(self):
        self.loadFile(self.conn,"shapes.txt","shapes","")
        self.loadFile(self.conn,"trips.txt","trips","(block_id,@col2,route_id,@col4,direction_id,trip_headsign,shape_id,service_id,trip_id,@col10)")
        self.loadFile(self.conn,"stops.txt","stops","(stop_lat,@col2,stop_code,stop_lon,@col5,@col6,@col7,stop_desc,stop_name,@col10,stop_id,@col12)")
        self.loadFile(self.conn,"routes.txt","routes","(route_long_name,@col2,@col3,route_color,@col5,route_id,@col7,@col8,@col9)")
        self.loadFile(self.conn,"stop_times.txt","stop_times","(trip_id,arrival_time,departure_time,stop_id,stop_sequence,@col6,@col7,@col8,@col9,@col10)")