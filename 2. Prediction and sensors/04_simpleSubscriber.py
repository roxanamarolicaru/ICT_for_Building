import paho.mqtt.client as PahoMQTT
import time
import json
#from influxdb import InfluxDBClient
#from influxdb_client import InfluxDBClient
from influxdb import InfluxDBClient

class MySubscriber:
		def __init__(self, clientID):
			self.clientID = clientID
			# create an instance of paho.mqtt.client
			self._paho_mqtt = PahoMQTT.Client(clientID, False) 

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
			print("--------Received json--------")
			print(data)
			print("keys=")
			print(data.keys())
			#self.client.write(data, params=None, expected_response_code=204, protocol=u'json')
			dicty_list = [data]
			self.client.write_points(dicty_list)

			#Modifications for the dashboard
			dashboard_dict = {}
			dashboard_dict["measurement"] = "Dashboard"
			dashboard_dict["fields"] = {}
			names_T = [
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
			'GROUND:ZONE12:Zone Operative Temperature [C](Hourly:ON)']
			out_T = 'Environment:Site Outdoor Air Drybulb Temperature [C](Hourly)'


			#Real energy consumption for each room 
			cons_real1=data["fields"]['GROUND:ZONE1:Zone Lights Electricity Rate [W](Hourly)']+data["fields"]['GROUND:ZONE1:Zone Air System Sensible Heating Rate [W](Hourly)']+data["fields"]['GROUND:ZONE1:Zone Air System Sensible Cooling Rate [W](Hourly)']
			cons_real2=data["fields"]['GROUND:ZONE2:Zone Lights Electricity Rate [W](Hourly)']+data["fields"]['GROUND:ZONE2:Zone Air System Sensible Heating Rate [W](Hourly)']+data["fields"]['GROUND:ZONE2:Zone Air System Sensible Cooling Rate [W](Hourly)']
			cons_real3=data["fields"]['GROUND:ZONE3:Zone Lights Electricity Rate [W](Hourly)']+data["fields"]['GROUND:ZONE3:Zone Air System Sensible Heating Rate [W](Hourly)']+data["fields"]['GROUND:ZONE3:Zone Air System Sensible Cooling Rate [W](Hourly)']
			cons_real4=data["fields"]['GROUND:ZONE4:Zone Lights Electricity Rate [W](Hourly)']+data["fields"]['GROUND:ZONE4:Zone Air System Sensible Heating Rate [W](Hourly)']+data["fields"]['GROUND:ZONE4:Zone Air System Sensible Cooling Rate [W](Hourly)']
			cons_real5=data["fields"]['GROUND:ZONE5:Zone Lights Electricity Rate [W](Hourly)']+data["fields"]['GROUND:ZONE5:Zone Air System Sensible Heating Rate [W](Hourly)']+data["fields"]['GROUND:ZONE5:Zone Air System Sensible Cooling Rate [W](Hourly)']
			cons_real6=data["fields"]['GROUND:ZONE6:Zone Lights Electricity Rate [W](Hourly)']+data["fields"]['GROUND:ZONE6:Zone Air System Sensible Heating Rate [W](Hourly)']+data["fields"]['GROUND:ZONE6:Zone Air System Sensible Cooling Rate [W](Hourly)']
			cons_real7=data["fields"]['GROUND:ZONE7:Zone Lights Electricity Rate [W](Hourly)']+data["fields"]['GROUND:ZONE7:Zone Air System Sensible Heating Rate [W](Hourly)']+data["fields"]['GROUND:ZONE7:Zone Air System Sensible Cooling Rate [W](Hourly)']
			cons_real8=data["fields"]['GROUND:ZONE8:Zone Lights Electricity Rate [W](Hourly)']+data["fields"]['GROUND:ZONE8:Zone Air System Sensible Heating Rate [W](Hourly)']+data["fields"]['GROUND:ZONE8:Zone Air System Sensible Cooling Rate [W](Hourly)']
			cons_real9=data["fields"]['GROUND:ZONE9:Zone Lights Electricity Rate [W](Hourly)']+data["fields"]['GROUND:ZONE9:Zone Air System Sensible Heating Rate [W](Hourly)']+data["fields"]['GROUND:ZONE9:Zone Air System Sensible Cooling Rate [W](Hourly)']
			cons_real10=data["fields"]['GROUND:ZONE10:Zone Lights Electricity Rate [W](Hourly)']+data["fields"]['GROUND:ZONE10:Zone Air System Sensible Heating Rate [W](Hourly)']+data["fields"]['GROUND:ZONE10:Zone Air System Sensible Cooling Rate [W](Hourly)']
			cons_real11=data["fields"]['GROUND:ZONE11:Zone Lights Electricity Rate [W](Hourly)']+data["fields"]['GROUND:ZONE11:Zone Air System Sensible Heating Rate [W](Hourly)']+data["fields"]['GROUND:ZONE11:Zone Air System Sensible Cooling Rate [W](Hourly)']
			cons_real12=data["fields"]['GROUND:ZONE12:Zone Lights Electricity Rate [W](Hourly)']+data["fields"]['GROUND:ZONE12:Zone Air System Sensible Heating Rate [W](Hourly)']+data["fields"]['GROUND:ZONE12:Zone Air System Sensible Cooling Rate [W](Hourly)']
			
			
			mean_inner = 0
			zone_idx = 1
			for key_T in names_T:
				mean_inner += data["fields"][key_T]
				t_name = "Temperature of Zone " + str(zone_idx) + " [C]"
				dashboard_dict["fields"][t_name] = data["fields"][key_T]
				zone_idx += 1
			mean_inner = mean_inner/len(names_T)
			out_T = data["fields"][out_T]
			dashboard_dict["fields"]["Mean inner Temperature [C]"] = mean_inner
			dashboard_dict["fields"]["Outside Temperature [C]"] = out_T

			dashboard_dict["fields"]["Total conumption room 1 [W]"] = cons_real1
			dashboard_dict["fields"]["Total conumption room 2 [W]"] = cons_real2
			dashboard_dict["fields"]["Total conumption room 3 [W]"] = cons_real3
			dashboard_dict["fields"]["Total conumption room 4 [W]"] = cons_real4
			dashboard_dict["fields"]["Total conumption room 5 [W]"] = cons_real5
			dashboard_dict["fields"]["Total conumption room 6 [W]"] = cons_real6
			dashboard_dict["fields"]["Total conumption room 7 [W]"] = cons_real7
			dashboard_dict["fields"]["Total conumption room 8 [W]"] = cons_real8
			dashboard_dict["fields"]["Total conumption room 9 [W]"] = cons_real9
			dashboard_dict["fields"]["Total conumption room 10 [W]"] = cons_real10
			dashboard_dict["fields"]["Total conumption room 11 [W]"] = cons_real11
			dashboard_dict["fields"]["Total conumption room 12 [W]"] = cons_real12

			dashboard_dict["time"] = data["time"]
			dicty_list = [dashboard_dict]
			self.client.write_points(dicty_list)

if __name__ == "__main__":
	test = MySubscriber('VirtualBuilding')
	test.start()
	while (True):
		pass