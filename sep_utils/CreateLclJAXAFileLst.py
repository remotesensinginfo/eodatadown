from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy
import json
import gzip

Base = declarative_base()


class EDDJAXASARTiles(Base):
    __tablename__ = "EDDJAXASARTiles"

    PID = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True, autoincrement=True)
    Tile_Name = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    Parent_Tile = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    Year = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
    File_Name = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    Server_File_Path = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    InstrumentName = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    Start_Date = sqlalchemy.Column(sqlalchemy.DateTime, nullable=True)
    End_Date = sqlalchemy.Column(sqlalchemy.DateTime, nullable=True)
    Incident_Angle_Low = sqlalchemy.Column(sqlalchemy.Float, nullable=True)
    Incident_Angle_High = sqlalchemy.Column(sqlalchemy.Float, nullable=True)
    North_Lat = sqlalchemy.Column(sqlalchemy.Float, nullable=True)
    South_Lat = sqlalchemy.Column(sqlalchemy.Float, nullable=True)
    East_Lon = sqlalchemy.Column(sqlalchemy.Float, nullable=True)
    West_Lon = sqlalchemy.Column(sqlalchemy.Float, nullable=True)
    Remote_URL = sqlalchemy.Column(sqlalchemy.String, nullable=True)
    Remote_URL_MD5 = sqlalchemy.Column(sqlalchemy.String, nullable=True)
    Total_Size = sqlalchemy.Column(sqlalchemy.Integer, nullable=True)
    Query_Date = sqlalchemy.Column(sqlalchemy.DateTime, nullable=False)
    Download_Start_Date = sqlalchemy.Column(sqlalchemy.DateTime, nullable=True)
    Download_End_Date = sqlalchemy.Column(sqlalchemy.DateTime, nullable=True)
    Downloaded = sqlalchemy.Column(sqlalchemy.Boolean, nullable=False, default=False)
    Download_Path = sqlalchemy.Column(sqlalchemy.String, nullable=False, default="")
    ARDProduct_Start_Date = sqlalchemy.Column(sqlalchemy.DateTime, nullable=True)
    ARDProduct_End_Date = sqlalchemy.Column(sqlalchemy.DateTime, nullable=True)
    ARDProduct = sqlalchemy.Column(sqlalchemy.Boolean, nullable=False, default=False)
    ARDProduct_Path = sqlalchemy.Column(sqlalchemy.String, nullable=False, default="")


dbEng = sqlalchemy.create_engine("sqlite:///EODataDownJAXATiles.db")
Session = sqlalchemy.orm.sessionmaker(bind=dbEng)
ses = Session()

avail_years = [1996, 2007, 2008, 2009, 2010, 2015, 2016, 2017]
jaxafileslst = dict()

for cyear in avail_years:
    jaxafileslst[cyear] = []
    query_rtn = ses.query(EDDJAXASARTiles).filter(EDDJAXASARTiles.Year == cyear).all()
    
    for rec in query_rtn:
        jaxafileslst[cyear].append(rec.Server_File_Path)



json_str = json.dumps(jaxafileslst) + "\n"                       # 1. string (i.e. JSON)
json_bytes = json_str.encode('utf-8')                            # 2. bytes (i.e. UTF-8)

with gzip.GzipFile("JAXASARMosaicFiles.json.gz", 'w') as fout:   # 3. gzip
    fout.write(json_bytes)                   

ses.close()

