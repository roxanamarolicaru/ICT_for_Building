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
from eppy import modeleditor
from eppy.modeleditor import IDF

#Simulation running
iddfile='/Applications/EnergyPlus-9-6-0/Energy+.idd' #select which version of EnergyPlus we use
outputPath= './outputdir/BESOS_Output'
my_directory='./IDF/'
my_idf='G2_V1_S1_optimal.idf'
weather='./ITA_Rome.162420_IWEC.epw'
IDF.setiddname(iddfile)
idf = IDF(my_directory + my_idf,weather)
idf.run(readvars=True)




