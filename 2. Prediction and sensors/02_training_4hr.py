import matplotlib.pyplot as plt
from seaborn import heatmap, pairplot
from besos import eppy_funcs as ef
from besos import sampling
from besos.evaluator import EvaluatorEP
from besos.parameters import RangeParameter, FieldSelector, Parameter, expand_plist, wwr, CategoryParameter
from besos.problem import EPProblem
import pandas as pd
import numpy as np
import glob
import os
import esoreader
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_squared_error
import tensorflow as tf
from tensorflow import keras
import math
from pickle import dump
from pickle import load
from datetime import datetime

#%% Import data

#Import data and display columns
sim_data = pd.read_csv('./data.csv')
sim_data['Q [J](Hourly)'] = sim_data['DistrictCooling:Facility [J](Hourly)'] + sim_data['DistrictHeating:Facility [J](Hourly)'] + sim_data['Electricity:Facility [J](Hourly)']
columns = sim_data.columns
print(columns)

#Load data to message broker
day_of_year_vector=[]
hour_of_day_vector=[]
day_of_week_vector=[]
for i in sim_data.index:
    
    #Compute timestamp
    aux = sim_data["Date/Time"].iloc[i]
    date_str=aux.split()[0]+" "+str(int(aux.split()[1].split(':')[0])-1)+":"+aux.split()[1].split(':')[1]+":"+aux.split()[1].split(':')[2]
    date_object = datetime.strptime(date_str, "%m/%d %H:%M:%S")
    day_of_year_vector.append(date_object.timetuple().tm_yday)
    hour_of_day_vector.append(date_object.hour)
    day_of_week_vector.append(date_object.weekday())
sim_data["DayOfYear"] = day_of_year_vector
sim_data["HourOfDay"] = hour_of_day_vector
sim_data["DayOfWeek"] = day_of_week_vector

#Output variables
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

#Selection of features
features = sim_data[[
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

#Selection of output
output = sim_data[output_variables]

#%% DATA PREPARATION AND PREPROCESSING

#Convert energy consumption from Jh to Watt
features['DistrictCooling:Facility [J](Hourly)'] = features['DistrictCooling:Facility [J](Hourly)'] / 3.6e3
features['DistrictHeating:Facility [J](Hourly)'] = features['DistrictHeating:Facility [J](Hourly)'] / 3.6e3
features['Electricity:Facility [J](Hourly)'] = features['Electricity:Facility [J](Hourly)'] / 3.6e3
#output['DistrictCooling:Facility [J](Hourly)'] = output['DistrictCooling:Facility [J](Hourly)'] / 3.6e3
#output['DistrictHeating:Facility [J](Hourly)'] = output['DistrictHeating:Facility [J](Hourly)'] / 3.6e3
output['Q [J](Hourly)'] = output['Q [J](Hourly)'] / 3.6e3

#Shift time by the desired value
epoch_shift = 4
features.drop(features.tail(epoch_shift).index,inplace=True)
output.drop(output.head(epoch_shift).index,inplace=True)
#temps = features[output_variables]

# Normalize the dataset
scaler_in = MinMaxScaler(feature_range=(0, 1))
features = scaler_in.fit_transform(features)
scaler_out = MinMaxScaler(feature_range=(0, 1))
output = scaler_out.fit_transform(output)

# Split into train and test sets
train_size = int(len(features) * 0.8)
test_size = len(features) - train_size
train_X, test_X = features[0:train_size,:], features[train_size:features.shape[0],:]
train_Y, test_Y = output[0:train_size,:], output[train_size:features.shape[0],:]

#%% FITTING

# Create Keras model
print("Generating model...")

'''
#Sequetial model (not used)
model = tf.keras.models.Sequential([
    tf.keras.layers.Dense(20, activation='sigmoid'),
    tf.keras.layers.Dense(len(output_variables))
])
'''

#Input layer
inputs = tf.keras.Input(shape=(features.shape[1],))

#Layers for Temps
t1 = tf.keras.layers.Dense(50, activation='relu')(inputs)
t_out = tf.keras.layers.Dense(len(output_variables)-1)(t1)

#Layers for Energy
e0 = tf.keras.layers.Dense(100, activation='relu')(inputs)
e1 = tf.keras.layers.Dense(100, activation='relu')(e0)
e_out = tf.keras.layers.Dense(1, activation='relu')(e1)

#Output layer
out = tf.keras.layers.Concatenate()([t_out, e_out])
model = tf.keras.Model(inputs=inputs, outputs=out)


#Compile model
print("Compiling...")
model.compile(optimizer=tf.keras.optimizers.Adam(3e-5), loss=tf.keras.losses.MeanSquaredError())
#model.compile(loss=tf.keras.losses.MeanSquaredError())

#Fit the model
print("Training...")
#history = model.fit(train_X, train_Y, validation_split=0.33, batch_size=2000, epochs=5000)
history = model.fit(train_X, train_Y, validation_split=0.33, batch_size=32, epochs=1500)


#%% ANALYZE RESULT

# Make predictions
trainPredict = model.predict(train_X)
testPredict = model.predict(test_X)

# Invert predictions
trainPredict = scaler_out.inverse_transform(trainPredict)
train_Y = scaler_out.inverse_transform(train_Y)
testPredict = scaler_out.inverse_transform(testPredict)
test_Y = scaler_out.inverse_transform(test_Y)

# Calculate root mean squared error
trainScore = math.sqrt(mean_squared_error(train_Y, trainPredict))
print('Train Score: %.2f RMSE' % (trainScore))
testScore = math.sqrt(mean_squared_error(test_Y, testPredict))
print('Test Score: %.2f RMSE' % (testScore))
#testScore = math.sqrt(mean_squared_error(test_Y, np.array(temps.iloc[train_size:,:])))
#print('Same Temp Score: %.2f RMSE' % (testScore))

#Plot learning curve
plt.xlabel(r'Training epoch',fontsize=13)
plt.ylabel(r'Mean Squared Error',fontsize=13)
plt.title("Learning curve for 4 hr",fontsize=13)
plt.semilogx(history.history['val_loss'], label='Validation')
plt.semilogx(history.history['loss'], label='Training')
plt.yscale('log')
plt.grid(linestyle='--')
plt.legend()
plt.show()

print('--- Results by feature ---')
idx = 0
for output_var in output_variables:
    err = np.array(test_Y[:,idx] - testPredict[:,idx])
    print(output_var + ' , Mean = ' + str(np.mean(err)) + ' , Std = ' + str(np.std(err)))
    idx = idx+1

#%% PLOT RESULTS

epoch_init = 4000
N = 200

#Plot predictions
feature = 1
plt.xlabel(r'Time [h]',fontsize=13)
plt.ylabel(r'T [C]',fontsize=13)
plt.title("Prediction for operative temperatures of Zone "+str(feature+1)+" for 4 hr",fontsize=13)
#plt.plot(np.array(temps.iloc[train_size+epoch_init:train_size+epoch_init+N,feature]), label='Actual value')
plt.plot(test_Y[epoch_init:epoch_init+N,feature], label='Real future value')
plt.plot(testPredict[epoch_init:epoch_init+N,feature], label='Predicted future value')
plt.grid(linestyle='--')
plt.legend()
plt.show()
#Plot errors
err = pd.DataFrame(np.array(test_Y[:,feature] - testPredict[:,feature]))
err.plot(kind = 'kde', legend = False)
plt.title("Error probability density for 4 hr",fontsize=13)
plt.grid(linestyle='--')
plt.xlabel(r'Delta T [C]',fontsize=13)
plt.show()

#Plot predictions
feature = 12
plt.xlabel(r'Time [h]',fontsize=13)
plt.ylabel(r'Q [W]',fontsize=13)
plt.title("Prediction of Energy consumption Q for 4 hr",fontsize=13)
#plt.plot(np.array(temps.iloc[train_size+epoch_init:train_size+epoch_init+N,feature]), label='Actual value')
plt.plot(test_Y[epoch_init:epoch_init+N,feature], label='Real future value')
plt.plot(testPredict[epoch_init:epoch_init+N,feature], label='Predicted future value')
plt.grid(linestyle='--')
plt.legend()
plt.show()
#Plot errors
err = pd.DataFrame(np.array(test_Y[:,feature] - testPredict[:,feature]))
err.plot(kind = 'kde', legend = False)
plt.title("Error probability density for 4 hr",fontsize=13)
plt.grid(linestyle='--')
plt.xlabel(r'Delta Q [W]',fontsize=13)
plt.show()

#%% EXPORT MODEL

model_name = 'ANN_model_4hr'
model.save('./models/' + model_name + '.h5')
dump(scaler_in, open('./models/scaler_in_' + model_name + '.pkl', 'wb'))
dump(scaler_out, open('./models/scaler_out_' + model_name + '.pkl', 'wb'))
