import paho.mqtt.client as PahoMQTT
import time
import json
#from influxdb import InfluxDBClient
#from influxdb_client import InfluxDBClient
from influxdb import InfluxDBClient
import numpy as np
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_squared_error
from pickle import load
import pandas as pd
import tensorflow as tf
from tensorflow import keras
import datetime

class MySubscriber:
		def __init__(self, clientID):
			self.clientID = clientID
			# create an instance of paho.mqtt.client
			self._paho_mqtt = PahoMQTT.Client(clientID+"_Predictor", False) 

			# register the callback
			self._paho_mqtt.on_connect = self.myOnConnect
			self._paho_mqtt.on_message = self.myOnMessageReceived
			self.topic = 'ict4bd'
			self.messageBroker = 'localhost'
			token="13NdY5O9D2KEDSY8YkBgmFG3frh5TD3fhB3A4x1NHKIl4ZEPq0lsl8jLl9nOhozGl7nEtLGqezf4t-ddRptJOQ==-ddRptJOQ=="
			org = "root"
			bucket = "root"
			#self.client = InfluxDBClient(url="http://127.0.0.1:8086", token=token, org=org)
			#self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
			#self.client = InfluxDBClient(host='192.168.1.143', port=8086, username='user', password='password')
			#self.client = InfluxDBClient('192.168.1.143', 8086, 'user', 'password', clientID)
			self.client = InfluxDBClient('smart-school.ddns.net', 8086, 'user', 'password', clientID)
			if {'name': clientID} not in self.client.get_list_database():
				self.client.create_database(clientID)


		def start (self):
			# manage connection to broker
			self._paho_mqtt.connect(self.messageBroker, 1883)
			self._paho_mqtt.loop_start()
			# subscribe for a topic
			self._paho_mqtt.subscribe(self.topic, 2)

		def stop (self):
			self._paho_mqtt.unsubscribe(self.topic)
			self._paho_mqtt.loop_stop()
			self._paho_mqtt.disconnect()

		def myOnConnect (self, paho_mqtt, userdata, flags, rc):
			#print ("Connected to %s with result code: %d" % (self.messageBroker, rc))
			pass
        
		def myOnMessageReceived (self, paho_mqtt , userdata, msg):
			# A new message is received
			#print ("Topic:'" + msg.topic+"', QoS: '"+str(msg.qos)+"' Message: '"+str(msg.payload) + "'")
			data=json.loads(msg.payload)
			print("Received message = ", data)
			data["measurement"] = "Predictions"
			input_vector = np.array([*data['fields'].values()])
			input_vector = input_vector.reshape(1, -1)
			scaled_in = self.scaler_in.transform(input_vector)
			prediction = self.model_imported.predict(scaled_in)
			scaled_out = self.scaler_out.inverse_transform(prediction)
			output_variables = [
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
				#Energy consumption
				'Q [J](Hourly)',
			]
			zip_iterator = zip(output_variables, scaled_out[0])
			a_dictionary = dict(zip_iterator)
			data['fields'] = a_dictionary
			delta_h = datetime.timedelta(hours = 4)
			actual_time = datetime.datetime.strptime(data['time'], "%Y-%d-%m %H:%M:%S")
			future_time = actual_time + delta_h
			data["time"] = str(future_time)
			print("--- Prediction data ---")
			print(data)
			dicty_list = [data]
			self.client.write_points(dicty_list)






if __name__ == "__main__":
	#dicty = {"measurement": "Prova", "fields": {"Environment:Site Outdoor Air Drybulb Temperature [C](Hourly)": 7.225, "Environment:Site Outdoor Air Dewpoint Temperature [C](Hourly)": 6.825000000000001, "Environment:Site Wind Speed [m/s](Hourly)": 2.1, "Environment:Site Diffuse Solar Radiation Rate per Area [W/m2](Hourly)": 0.0, "GROUND:ZONE1:Zone Lights Electricity Rate [W](Hourly)": 0.0, "GROUND:ZONE2:Zone Lights Electricity Rate [W](Hourly)": 0.0, "GROUND:ZONE3:Zone Lights Electricity Rate [W](Hourly)": 0.0, "GROUND:ZONE4:Zone Lights Electricity Rate [W](Hourly)": 0.0, "GROUND:ZONE5:Zone Lights Electricity Rate [W](Hourly)": 0.0, "GROUND:ZONE6:Zone Lights Electricity Rate [W](Hourly)": 0.0, "GROUND:ZONE7:Zone Lights Electricity Rate [W](Hourly)": 0.0, "GROUND:ZONE8:Zone Lights Electricity Rate [W](Hourly)": 0.0, "GROUND:ZONE9:Zone Lights Electricity Rate [W](Hourly)": 0.0, "GROUND:ZONE10:Zone Lights Electricity Rate [W](Hourly)": 0.0, "GROUND:ZONE11:Zone Lights Electricity Rate [W](Hourly)": 0.0, "GROUND:ZONE12:Zone Lights Electricity Rate [W](Hourly)": 0.0, "GROUND:ZONE1:Zone Operative Temperature [C](Hourly:ON)": 14.258945013966482, "GROUND:ZONE2:Zone Operative Temperature [C](Hourly:ON)": 17.46461214831728, "GROUND:ZONE3:Zone Operative Temperature [C](Hourly:ON)": 17.44971190550958, "GROUND:ZONE4:Zone Operative Temperature [C](Hourly:ON)": 17.657739879970737, "GROUND:ZONE5:Zone Operative Temperature [C](Hourly:ON)": 15.352023207756915, "GROUND:ZONE6:Zone Operative Temperature [C](Hourly:ON)": 17.820913783611964, "GROUND:ZONE7:Zone Operative Temperature [C](Hourly:ON)": 17.565125290095402, "GROUND:ZONE8:Zone Operative Temperature [C](Hourly:ON)": 17.576887643772462, "GROUND:ZONE9:Zone Operative Temperature [C](Hourly:ON)": 17.581196539817, "GROUND:ZONE10:Zone Operative Temperature [C](Hourly:ON)": 17.2943075033187, "GROUND:ZONE11:Zone Operative Temperature [C](Hourly:ON)": 17.533732444432637, "GROUND:ZONE12:Zone Operative Temperature [C](Hourly:ON)": 17.47645000992874, "GROUND:ZONE1:Zone Air System Sensible Heating Rate [W](Hourly)": 0.0, "GROUND:ZONE2:Zone Air System Sensible Heating Rate [W](Hourly)": 464.2966885388137, "GROUND:ZONE3:Zone Air System Sensible Heating Rate [W](Hourly)": 133.0562040816136, "GROUND:ZONE4:Zone Air System Sensible Heating Rate [W](Hourly)": 110.99226005614014, "GROUND:ZONE5:Zone Air System Sensible Heating Rate [W](Hourly)": 0.0, "GROUND:ZONE6:Zone Air System Sensible Heating Rate [W](Hourly)": 0.0, "GROUND:ZONE7:Zone Air System Sensible Heating Rate [W](Hourly)": 425.3591846071781, "GROUND:ZONE8:Zone Air System Sensible Heating Rate [W](Hourly)": 419.9575303322208, "GROUND:ZONE9:Zone Air System Sensible Heating Rate [W](Hourly)": 417.9582257350171, "GROUND:ZONE10:Zone Air System Sensible Heating Rate [W](Hourly)": 1404.5765443035607, "GROUND:ZONE11:Zone Air System Sensible Heating Rate [W](Hourly)": 119.54674793403058, "GROUND:ZONE12:Zone Air System Sensible Heating Rate [W](Hourly)": 170.11299057202734, "GROUND:ZONE1:Zone Air System Sensible Cooling Rate [W](Hourly)": 0.0, "GROUND:ZONE2:Zone Air System Sensible Cooling Rate [W](Hourly)": 0.0, "GROUND:ZONE3:Zone Air System Sensible Cooling Rate [W](Hourly)": 0.0, "GROUND:ZONE4:Zone Air System Sensible Cooling Rate [W](Hourly)": 0.0, "GROUND:ZONE5:Zone Air System Sensible Cooling Rate [W](Hourly)": 0.0, "GROUND:ZONE6:Zone Air System Sensible Cooling Rate [W](Hourly)": 0.0, "GROUND:ZONE7:Zone Air System Sensible Cooling Rate [W](Hourly)": 0.0, "GROUND:ZONE8:Zone Air System Sensible Cooling Rate [W](Hourly)": 0.0, "GROUND:ZONE9:Zone Air System Sensible Cooling Rate [W](Hourly)": 0.0, "GROUND:ZONE10:Zone Air System Sensible Cooling Rate [W](Hourly)": 0.0, "GROUND:ZONE11:Zone Air System Sensible Cooling Rate [W](Hourly)": 0.0, "GROUND:ZONE12:Zone Air System Sensible Cooling Rate [W](Hourly)": 0.0, "GROUND:ZONE1:Zone Mechanical Ventilation Standard Density Volume Flow Rate [m3/s](Hourly)": 0.0, "GROUND:ZONE2:Zone Mechanical Ventilation Standard Density Volume Flow Rate [m3/s](Hourly)": 0.0, "GROUND:ZONE3:Zone Mechanical Ventilation Standard Density Volume Flow Rate [m3/s](Hourly)": 0.0, "GROUND:ZONE4:Zone Mechanical Ventilation Standard Density Volume Flow Rate [m3/s](Hourly)": 0.0, "GROUND:ZONE5:Zone Mechanical Ventilation Standard Density Volume Flow Rate [m3/s](Hourly)": 0.0, "GROUND:ZONE6:Zone Mechanical Ventilation Standard Density Volume Flow Rate [m3/s](Hourly)": 0.0, "GROUND:ZONE7:Zone Mechanical Ventilation Standard Density Volume Flow Rate [m3/s](Hourly)": 0.0, "GROUND:ZONE8:Zone Mechanical Ventilation Standard Density Volume Flow Rate [m3/s](Hourly)": 0.0, "GROUND:ZONE9:Zone Mechanical Ventilation Standard Density Volume Flow Rate [m3/s](Hourly)": 0.0, "GROUND:ZONE10:Zone Mechanical Ventilation Standard Density Volume Flow Rate [m3/s](Hourly)": 0.0, "GROUND:ZONE11:Zone Mechanical Ventilation Standard Density Volume Flow Rate [m3/s](Hourly)": 0.0, "GROUND:ZONE12:Zone Mechanical Ventilation Standard Density Volume Flow Rate [m3/s](Hourly)": 0.0, "DistrictCooling:Facility [J](Hourly)": 0.0, "DistrictHeating:Facility [J](Hourly)": 3665.856376160602, "Electricity:Facility [J](Hourly)": 114.84182581789176}, "time": "1900-01-01 05:00:00"}
	#input_vector = np.array([*dicty['fields'].values()])
	#input_vector = input_vector.reshape(1, -1)
    
    #Re-import models and scalers
	model_name = 'ANN_model_1hr'
	scaler_in = load(open('./models/scaler_in_' + model_name + '.pkl', 'rb'))
	model_imported = tf.keras.models.load_model('./models/' + model_name + '.h5')
	scaler_out = load(open('./models/scaler_out_' + model_name + '.pkl', 'rb'))
	
	#scaled_in = scaler_in.transform(input_vector)
	#prediction = model_imported.predict(scaled_in)
	#scaled_out = scaler_out.inverse_transform(prediction)
    
	test = MySubscriber('VirtualBuilding')
	test.scaler_in = scaler_in
	test.scaler_out = scaler_out
	test.model_imported = model_imported
	test.start()
	while (True):
		pass