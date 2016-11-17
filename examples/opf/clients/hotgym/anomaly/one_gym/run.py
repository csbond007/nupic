#!/usr/bin/env python
# ----------------------------------------------------------------------
# Numenta Platform for Intelligent Computing (NuPIC)
# Copyright (C) 2013, Numenta, Inc.  Unless you have an agreement
# with Numenta, Inc., for a separate license for this software code, the
# following terms and conditions apply:
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero Public License version 3 as
# published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
# See the GNU Affero Public License for more details.
#
# You should have received a copy of the GNU Affero Public License
# along with this program.  If not, see http://www.gnu.org/licenses.
#
# http://numenta.org/licenses/
# ----------------------------------------------------------------------
"""
Groups together code used for creating a NuPIC model and dealing with IO.
(This is a component of the One Hot Gym Anomaly Tutorial.)
"""
import importlib
import sys
import csv
#import datetime
from datetime import datetime
import pandas as pd
from cassandra.cluster import Cluster
from nupic.frameworks.opf.modelfactory import ModelFactory
import time
import nupic_anomaly_output


DESCRIPTION = (
  "Starts a NuPIC model from the model params returned by the swarm\n"
  "and pushes each line of input from the gym into the model. Results\n"
  "are written to an output file (default) or plotted dynamically if\n"
  "the --plot option is specified.\n"
)

DATA_FILE = "heart-beat"
DATA_DIR = "." # "/home/ksaha/Data/Data_Run"
MODEL_PARAMS_DIR = "./model_params"
INPUT_DATA = "%s/%s.csv" % (DATA_DIR, DATA_FILE.replace(" ", "_"))
# '7/2/10 0:00'
DATE_FORMAT = "%Y-%m-%d %H:%M:%S.%f"
#DATE_FORMAT = "%m/%d/%y %H:%M"
#INPUT_DATA = "%s/%s.csv" % (DATA_DIR, DATA_FILE.replace(" ", "_"))

def createModel(modelParams):
  """
  Given a model params dictionary, create a CLA Model. Automatically enables
  inference for kw_energy_consumption.
  :param modelParams: Model params dict
  :return: OPF Model object
  """
  model = ModelFactory.create(modelParams)
  model.enableInference({"predictedField": "heartbeat"})
  return model



def getModelParamsFromName():
  """
  Given a gym name, assumes a matching model params python module exists within
  the model_params directory and attempts to import it.
  :return: OPF Model params dictionary
  """
  importName = "model_params.%s_model_params" % (
    DATA_FILE.replace(" ", "_").replace("-", "_")
  )
  print "Importing model params from %s" % importName
  try:
    importedModelParams = importlib.import_module(importName).MODEL_PARAMS
  except ImportError:
    raise Exception("No model params exist for '%s'. Run swarm first!"
                    % DATA_FILE)
  return importedModelParams



def runIoThroughNupic(model):
  """
  Handles looping over the input data and passing each row into the given model
  object, as well as extracting the result object and passing it into an output
  handler.
  :param inputData: file path to input data CSV
  :param model: OPF Model object
  """

  output = nupic_anomaly_output.NuPICFileOutput(DATA_FILE)

  cluster = Cluster(['10.10.40.138'])
  session = cluster.connect('hotgym')  # key-space = hotgym
  print ("Connected!")

  num_records_index = 0
  total_records = 767 # -1 
  patient_id = '101'
  
  # Master Data Source
  df = pd.read_csv(INPUT_DATA)

  while(num_records_index < total_records):

        print "Processing record = %i ..." % num_records_index
#        print datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
	
#        dt = datetime.now()
        val_time_stamp = str(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])
#        print val_time_stamp
#        str(datetime.datetime.now())  # dt.microsecond
#        print val_time_stamp
#        print(datetime.now())#str(int(float(datetime.now().strftime("%s.%f"))) * 1000)
        
        val_heartbeat = str(df.iloc[num_records_index,1])


        CQL_Insert_String = "INSERT INTO data_input_11_17_3 (patient_id,time_stamp, heartbeat) " + \
                             "VALUES ('" + patient_id + "','"+ val_time_stamp + "','" + \
                            val_heartbeat + "');"

        session.execute(CQL_Insert_String)
        
        # Add the record from the actual data source to the Cassandra input table
        ## Read back the latest row added from Cassandra input table
        CQLString = "SELECT * FROM data_input_11_17_3  LIMIT 1;" # Need to check the actual syntax to get the latest row
        rows = session.execute(CQLString)
        for user_row in rows:
                data_df = pd.DataFrame({'col_1' : [user_row.time_stamp],'col_2' : [user_row.heartbeat]})

        timestamp = datetime.strptime(str(data_df.iloc[0,0]), DATE_FORMAT)

        heartbeat = float(data_df.iloc[0,1])

        #print timestamp,heartbeat
        
             
        result = model.run({
                            "timestamp": timestamp,
                            "heartbeat": heartbeat
                           })
        
        prediction = result.inferences["multiStepBestPredictions"][1]
        anomalyScore = result.inferences["anomalyScore"]
        ## Write this anomaly score to the Cassandra output table
        ## For the time-being writing to flat file
        
        anomalyLikelihood = output.write(timestamp, heartbeat, prediction, anomalyScore)

        timestamp = str(timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])
        print timestamp, heartbeat, prediction, anomalyScore, anomalyLikelihood 
        
        CQL_Output_String = "INSERT INTO data_output_11_17_3 (patient_id,timestamp,heartbeat, \
                             prediction,anomalyScore,anomalyLikelihood) " + "VALUES ('" + \
                             patient_id + "','"+ str(timestamp) + "','" + str(heartbeat) + \
			     "','" + str(prediction) + "','" + str(anomalyScore) + "','" + \
			     str(anomalyLikelihood) +"');"

        session.execute(CQL_Output_String)
                
        # To simulate real-time case we add delay here

        print("Waiting for the next record. Delay is 1 sec")

        time.sleep(.001)

        num_records_index += 1

  #output.close()


def runModel():
  """
  Assumes the gynName corresponds to both a like-named model_params file in the
  model_params directory, and that the data exists in a like-named CSV file in
  the current directory.
  """
  print "Creating model from %s..." % DATA_FILE
  model = createModel(getModelParamsFromName())
  runIoThroughNupic(model)

if __name__ == "__main__":
  print DESCRIPTION
  runModel()
