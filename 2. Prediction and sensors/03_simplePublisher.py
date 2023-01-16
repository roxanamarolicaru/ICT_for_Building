import paho.mqtt.client as PahoMQTT
import time
import pandas as pd
import json
import datetime
import time

class MyPublisher:
	def __init__(self, clientID):
		self.clientID = clientID
		# create an instance of paho.mqtt.client
		self._paho_mqtt = PahoMQTT.Client(self.clientID, False) 
		# register the callback
		self._paho_mqtt.on_connect = self.myOnConnect
		self.messageBroker = 'localhost'

	def start (self):
		#manage connection to broker
		self._paho_mqtt.connect(self.messageBroker, 1883)
		self._paho_mqtt.loop_start()

	def stop (self):
		self._paho_mqtt.loop_stop()
		self._paho_mqtt.disconnect()

	def myPublish(self, topic, message):
		# publish a message with a certain topic
		self._paho_mqtt.publish(topic, message, 2)

	def myOnConnect (self, paho_mqtt, userdata, flags, rc):
		print ("Connected to %s with result code: %d" % (self.messageBroker, rc))


if __name__ == "__main__":
	test = MyPublisher("MyPublisher")
	test.start()
	df=pd.read_csv('./data.csv')
	
	#Set timestamps
	day_of_year_vector=[]
	hour_of_day_vector=[]
	day_of_week_vector=[]
	for i in df.index:
		
		#Compute timestamp
		aux = df["Date/Time"].iloc[i]
		date_str=aux.split()[0]+" "+str(int(aux.split()[1].split(':')[0])-1)+":"+aux.split()[1].split(':')[1]+":"+aux.split()[1].split(':')[2]
		try:
			date_object = datetime.datetime.strptime(date_str, "%m/%d %H:%M:%S")
		except:
			print("---PARSING ERROR----")
		day_of_year_vector.append(date_object.timetuple().tm_yday)
		hour_of_day_vector.append(date_object.hour)
		day_of_week_vector.append(date_object.weekday())
	df["DayOfYear"] = day_of_year_vector
	df["HourOfDay"] = hour_of_day_vector
	df["DayOfWeek"] = day_of_week_vector

	features = df[[
		#Date
		#'Date/Time',
		'DayOfYear',
		'HourOfDay',
		'DayOfWeek',
		#Environment variables
		'Environment:Site Outdoor Air Drybulb Temperature [C](Hourly)',
		'Environment:Site Outdoor Air Dewpoint Temperature [C](Hourly)',
		'Environment:Site Wind Speed [m/s](Hourly)',
		'Environment:Site Diffuse Solar Radiation Rate per Area [W/m2](Hourly)',
		#Lights
		'GROUND:ZONE1:Zone Lights Electricity Rate [W](Hourly)',
		'GROUND:ZONE2:Zone Lights Electricity Rate [W](Hourly)',
		'GROUND:ZONE3:Zone Lights Electricity Rate [W](Hourly)',
		'GROUND:ZONE4:Zone Lights Electricity Rate [W](Hourly)',
		'GROUND:ZONE5:Zone Lights Electricity Rate [W](Hourly)',
		'GROUND:ZONE6:Zone Lights Electricity Rate [W](Hourly)',
		'GROUND:ZONE7:Zone Lights Electricity Rate [W](Hourly)',
		'GROUND:ZONE8:Zone Lights Electricity Rate [W](Hourly)',
		'GROUND:ZONE9:Zone Lights Electricity Rate [W](Hourly)',
		'GROUND:ZONE10:Zone Lights Electricity Rate [W](Hourly)',
		'GROUND:ZONE11:Zone Lights Electricity Rate [W](Hourly)',
		'GROUND:ZONE12:Zone Lights Electricity Rate [W](Hourly)',
		#Operative temperatures
		'GROUND:ZONE1:Zone Operative Temperature [C](Hourly:ON)',
		'GROUND:ZONE2:Zone Operative Temperature [C](Hourly:ON)',
		'GROUND:ZONE3:Zone Operative Temperature [C](Hourly:ON)',
		'GROUND:ZONE4:Zone Operative Temperature [C](Hourly:ON)',
		'GROUND:ZONE5:Zone Operative Temperature [C](Hourly:ON)',
		'GROUND:ZONE6:Zone Operative Temperature [C](Hourly:ON)',
		'GROUND:ZONE7:Zone Operative Temperature [C](Hourly:ON)',
		'GROUND:ZONE8:Zone Operative Temperature [C](Hourly:ON)',
		'GROUND:ZONE9:Zone Operative Temperature [C](Hourly:ON)',
		'GROUND:ZONE10:Zone Operative Temperature [C](Hourly:ON)',
		'GROUND:ZONE11:Zone Operative Temperature [C](Hourly:ON)',
		'GROUND:ZONE12:Zone Operative Temperature [C](Hourly:ON)',
		#Heating
		'GROUND:ZONE1:Zone Air System Sensible Heating Rate [W](Hourly)',
		'GROUND:ZONE2:Zone Air System Sensible Heating Rate [W](Hourly)',
		'GROUND:ZONE3:Zone Air System Sensible Heating Rate [W](Hourly)',
		'GROUND:ZONE4:Zone Air System Sensible Heating Rate [W](Hourly)',
		'GROUND:ZONE5:Zone Air System Sensible Heating Rate [W](Hourly)',
		'GROUND:ZONE6:Zone Air System Sensible Heating Rate [W](Hourly)',
		'GROUND:ZONE7:Zone Air System Sensible Heating Rate [W](Hourly)',
		'GROUND:ZONE8:Zone Air System Sensible Heating Rate [W](Hourly)',
		'GROUND:ZONE9:Zone Air System Sensible Heating Rate [W](Hourly)',
		'GROUND:ZONE10:Zone Air System Sensible Heating Rate [W](Hourly)',
		'GROUND:ZONE11:Zone Air System Sensible Heating Rate [W](Hourly)',
		'GROUND:ZONE12:Zone Air System Sensible Heating Rate [W](Hourly)',
		#Cooling
		'GROUND:ZONE1:Zone Air System Sensible Cooling Rate [W](Hourly)',
		'GROUND:ZONE2:Zone Air System Sensible Cooling Rate [W](Hourly)',
		'GROUND:ZONE3:Zone Air System Sensible Cooling Rate [W](Hourly)',
		'GROUND:ZONE4:Zone Air System Sensible Cooling Rate [W](Hourly)',
		'GROUND:ZONE5:Zone Air System Sensible Cooling Rate [W](Hourly)',
		'GROUND:ZONE6:Zone Air System Sensible Cooling Rate [W](Hourly)',
		'GROUND:ZONE7:Zone Air System Sensible Cooling Rate [W](Hourly)',
		'GROUND:ZONE8:Zone Air System Sensible Cooling Rate [W](Hourly)',
		'GROUND:ZONE9:Zone Air System Sensible Cooling Rate [W](Hourly)',
		'GROUND:ZONE10:Zone Air System Sensible Cooling Rate [W](Hourly)',
		'GROUND:ZONE11:Zone Air System Sensible Cooling Rate [W](Hourly)',
		'GROUND:ZONE12:Zone Air System Sensible Cooling Rate [W](Hourly)',
		#Ventilation
		'GROUND:ZONE1:Zone Mechanical Ventilation Standard Density Volume Flow Rate [m3/s](Hourly)',
		'GROUND:ZONE2:Zone Mechanical Ventilation Standard Density Volume Flow Rate [m3/s](Hourly)',
		'GROUND:ZONE3:Zone Mechanical Ventilation Standard Density Volume Flow Rate [m3/s](Hourly)',
		'GROUND:ZONE4:Zone Mechanical Ventilation Standard Density Volume Flow Rate [m3/s](Hourly)',
		'GROUND:ZONE5:Zone Mechanical Ventilation Standard Density Volume Flow Rate [m3/s](Hourly)',
		'GROUND:ZONE6:Zone Mechanical Ventilation Standard Density Volume Flow Rate [m3/s](Hourly)',
		'GROUND:ZONE7:Zone Mechanical Ventilation Standard Density Volume Flow Rate [m3/s](Hourly)',
		'GROUND:ZONE8:Zone Mechanical Ventilation Standard Density Volume Flow Rate [m3/s](Hourly)',
		'GROUND:ZONE9:Zone Mechanical Ventilation Standard Density Volume Flow Rate [m3/s](Hourly)',
		'GROUND:ZONE10:Zone Mechanical Ventilation Standard Density Volume Flow Rate [m3/s](Hourly)',
		'GROUND:ZONE11:Zone Mechanical Ventilation Standard Density Volume Flow Rate [m3/s](Hourly)',
		'GROUND:ZONE12:Zone Mechanical Ventilation Standard Density Volume Flow Rate [m3/s](Hourly)',
		#Energy consumption
		'DistrictCooling:Facility [J](Hourly)',
		'DistrictHeating:Facility [J](Hourly)',
		'Electricity:Facility [J](Hourly)'
	]]
    
	#Mini preprocessing
	features['DistrictCooling:Facility [J](Hourly)'] = features['DistrictCooling:Facility [J](Hourly)'] / 3.6e3
	features['DistrictHeating:Facility [J](Hourly)'] = features['DistrictHeating:Facility [J](Hourly)'] / 3.6e3
	features['Electricity:Facility [J](Hourly)'] = features['Electricity:Facility [J](Hourly)'] / 3.6e3

    #Load data to message broker
	idx = 0
	for i in features.index:
        
        #Compute timestamp
		aux = df["Date/Time"].iloc[i]
		date_str=aux.split()[0]+" "+f"{(int(aux.split()[1].split(':')[0])-1):02}"+":"+aux.split()[1].split(':')[1]+":"+aux.split()[1].split(':')[2]
		date_str = "2022/"+date_str
		try:
			date_object = datetime.datetime.strptime(date_str, "%Y/%m/%d %H:%M:%S")
		except:
			print("---PARSING ERROR---- for date:" + date_str)
        
        #Measurements to dictonary
		measurements = {}
		measurements["measurement"] = 'Sensors'
		measurements["fields"] = dict(features.iloc[idx])
		measurements["time"] = str(date_object)
		#measurements["time"] = time.mktime(date_object.timetuple())
        
        #Publish to message broker
		print("Message to publish = " + json.dumps(measurements))
		test.myPublish ('ict4bd', json.dumps(measurements))
		
        #Wait and proceed
		time.sleep(0.1)
		idx = idx + 1

	test.stop()


