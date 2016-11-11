"""save_to_parquet.py"""
import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
#from pyspark.rdd import RDD

if __name__ == "__main__":

	conf = (SparkConf()
	         .setMaster("local")
	         .setAppName("My app")
	         .set("spark.executor.memory", "1g"))
	session = SparkSession.builder.config(conf = conf).getOrCreate()
	sc = session.sparkContext

	if len(sys.argv) >= 2:
		source = sys.argv[1]
	else:
		source = 'noaa/'

	if len (sys.argv) >= 3:
		output = sys.argv[2]
	else:
		output = 'p_/'

	def make_file_rdd(path):
		return sc.textFile(path).cache()

	isd_hystory = make_file_rdd(source + "isd-history.csv")

	class Station(object):
		_src = []
		_usaw = None
		_wban = None
		_name = None
		_lat = None
		_lon = None	

		def __init__(self, usaw, wban, name, lat, lon):
			self._usaw = usaw
			self._wban = wban
			self._name = name
			self._lat = lat
			self._lon = lon
		
		def show(self):
			return '{}, {}-{}, {}, {}, {}'.format(self._name, str(self._usaw), str(self._wban), str(self._lat), str(self._lon), str(self._src).strip('[]')) 
		
		def src_path(self):
			result = []

			for year in range(2013, 2017):
				file_path = '{}{}/{}-{}-{}.gz'.format(source, str(year), str(self._usaw), str(self._wban), year) 

				import os
				if(os.path.exists(file_path) and os.path.isfile(file_path)):
					result.append(file_path)
			year += 1
			self._src = result

			return result

			
		def src(self):
			return self._src


		def name(self):
			return self._name


		def wban(self):
			return self._wban


		def usaf(self):
			return self._usaf


		def lat(self):
			return self._lat

		def lon(self):
			return self._lon


	def find_sign(str_float):
		if str_float.find("+") > -1: 
			sign = 1.0
		elif str_float.find("-") > -1:
			sign = -1.0
		else:
			sign = 0.0

		try:
			f = float(str_float[2:-1])
		except ValueError:
			f = 0.0
		
		return f * sign

	def get_stations(raw_isd):
		usaw = raw_isd[0].replace("\\","")[1:-1]
		wban = raw_isd[1].replace("\\","")[1:-1]
		name = raw_isd[2].replace("\\","")[1:-1]
		lat = find_sign(raw_isd[6].replace("[\"()]",""))
		lon = find_sign(raw_isd[7].replace("[\"()]",""))
		
		s = Station(usaw, wban, name, lat, lon)
		
		paths = s.src_path()	
		return s
			

	stations = isd_hystory.map(lambda line: line.split(","))\
		.map(lambda row: get_stations(row))\
	 	.filter(lambda x: x.src())


	src = stations.map(lambda x: x.src()).collect()

	def get_df(str_line):
		usaf = str_line[4: 10]
      		wban = str_line[10: 15]
		date = str_line[15: 23]
	      	time = str_line[23: 27]
		lat = str_line[28: 34]
		lng = str_line[34: 41]
      
		
		str_tmp = str_line[87:92]
		if str_tmp.find("+") > -1:
                        sign = 1.0
                elif str_tmp.find("-") > -1:
                        sign = -1.0
                else:
                        sign = 1.0

		try:
                	flt_tmp = float(str_tmp[2:-1])
                except ValueError:
                        flt_tmp = 9999.0

		return Row(usaf, wban, lat, lng, date, time, flt_tmp * sign)

	for i in src:
		rdd = sc.textFile(str(i).strip('[]').replace(" ","").replace("'", ""))	
		df = rdd.map(lambda line: get_df(line)).collect()
	        data = session.createDataFrame(df).toDF("usaf", "wban", "lat", "lng", "date", "time", "temperature")
		data.show()
                data.write.parquet(output + str(i[0][9:-3]) + ".parquet")

	sc.stop()
