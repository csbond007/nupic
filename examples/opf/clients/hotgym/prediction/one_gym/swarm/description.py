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
Template file used by the OPF Experiment Generator to generate the actual
description.py file by replacing $XXXXXXXX tokens with desired values.

This description.py file was generated by:
'/home/ksaha/anomaly_detection/fork/11_16/code_11_17_1/lib/python2.7/site-packages/nupic/swarming/exp_generator/ExpGenerator.pyc'
"""

from nupic.frameworks.opf.expdescriptionapi import ExperimentDescriptionAPI

from nupic.frameworks.opf.expdescriptionhelpers import (
  updateConfigFromSubConfig,
  applyValueGettersToContainer
  )

from nupic.frameworks.opf.clamodelcallbacks import *
from nupic.frameworks.opf.metrics import MetricSpec
from nupic.swarming.experimentutils import (InferenceType, InferenceElement)
from nupic.support import aggregationDivide

from nupic.frameworks.opf.opftaskdriver import (
                                            IterationPhaseSpecLearnOnly,
                                            IterationPhaseSpecInferOnly,
                                            IterationPhaseSpecLearnAndInfer)


# Model Configuration Dictionary:
#
# Define the model parameters and adjust for any modifications if imported
# from a sub-experiment.
#
# These fields might be modified by a sub-experiment; this dict is passed
# between the sub-experiment and base experiment
#
#
config = {
    # Type of model that the rest of these parameters apply to.
    'model': "CLA",

    # Version that specifies the format of the config.
    'version': 1,

    # Intermediate variables used to compute fields in modelParams and also
    # referenced from the control section.
    'aggregationInfo': {   'days': 0,
    'fields': [],
    'hours': 0,
    'microseconds': 0,
    'milliseconds': 0,
    'minutes': 0,
    'months': 0,
    'seconds': 0,
    'weeks': 0,
    'years': 0},
    'predictAheadTime': None,

    # Model parameter dictionary.
    'modelParams': {
        # The type of inference that this model will perform
        'inferenceType': 'TemporalAnomaly',

        'sensorParams': {
            # Sensor diagnostic output verbosity control;
            # if > 0: sensor region will print out on screen what it's sensing
            # at each step 0: silent; >=1: some info; >=2: more info;
            # >=3: even more info (see compute() in py/regions/RecordSensor.py)
            'verbosity' : 0,

            # Example:
            #     'encoders': {'field1': {'fieldname': 'field1', 'n':100,
            #                  'name': 'field1', 'type': 'AdaptiveScalarEncoder',
            #                  'w': 21}}
            #
            'encoders': {
                u'timestamp_timeOfDay':     {   'fieldname': u'timestamp',
    'name': u'timestamp_timeOfDay',
    'timeOfDay': (21, 1),
    'type': 'DateEncoder'},
  u'timestamp_dayOfWeek':     {   'dayOfWeek': (21, 1),
    'fieldname': u'timestamp',
    'name': u'timestamp_dayOfWeek',
    'type': 'DateEncoder'},
  u'timestamp_weekend':     {   'fieldname': u'timestamp',
    'name': u'timestamp_weekend',
    'type': 'DateEncoder',
    'weekend': 21},
  u'heartbeat':     {   'clipInput': True,
    'fieldname': u'heartbeat',
    'maxval': 300.0,
    'minval': 0.0,
    'n': 100,
    'name': u'heartbeat',
    'type': 'ScalarEncoder',
    'w': 21},
  u'V3':     {   'clipInput': True,
    'fieldname': u'V3',
    'n': 100,
    'name': u'V3',
    'type': 'AdaptiveScalarEncoder',
    'w': 21},
  u'V4':     {   'clipInput': True,
    'fieldname': u'V4',
    'n': 100,
    'name': u'V4',
    'type': 'AdaptiveScalarEncoder',
    'w': 21},
  u'V5':     {   'clipInput': True,
    'fieldname': u'V5',
    'n': 100,
    'name': u'V5',
    'type': 'AdaptiveScalarEncoder',
    'w': 21},
  u'V6':     {   'clipInput': True,
    'fieldname': u'V6',
    'n': 100,
    'name': u'V6',
    'type': 'AdaptiveScalarEncoder',
    'w': 21},
  u'V7':     {   'clipInput': True,
    'fieldname': u'V7',
    'n': 100,
    'name': u'V7',
    'type': 'AdaptiveScalarEncoder',
    'w': 21},
  u'V8':     {   'clipInput': True,
    'fieldname': u'V8',
    'n': 100,
    'name': u'V8',
    'type': 'AdaptiveScalarEncoder',
    'w': 21},
  u'V9':     {   'clipInput': True,
    'fieldname': u'V9',
    'n': 100,
    'name': u'V9',
    'type': 'AdaptiveScalarEncoder',
    'w': 21},
  u'V10':     {   'clipInput': True,
    'fieldname': u'V10',
    'n': 100,
    'name': u'V10',
    'type': 'AdaptiveScalarEncoder',
    'w': 21},
  u'V11':     {   'clipInput': True,
    'fieldname': u'V11',
    'n': 100,
    'name': u'V11',
    'type': 'AdaptiveScalarEncoder',
    'w': 21},
  u'V12':     {   'clipInput': True,
    'fieldname': u'V12',
    'n': 100,
    'name': u'V12',
    'type': 'AdaptiveScalarEncoder',
    'w': 21},
  u'V13':     {   'clipInput': True,
    'fieldname': u'V13',
    'n': 100,
    'name': u'V13',
    'type': 'AdaptiveScalarEncoder',
    'w': 21},
  u'V14':     {   'clipInput': True,
    'fieldname': u'V14',
    'n': 100,
    'name': u'V14',
    'type': 'AdaptiveScalarEncoder',
    'w': 21},
  u'V15':     {   'clipInput': True,
    'fieldname': u'V15',
    'n': 100,
    'name': u'V15',
    'type': 'AdaptiveScalarEncoder',
    'w': 21},
  u'V16':     {   'clipInput': True,
    'fieldname': u'V16',
    'n': 100,
    'name': u'V16',
    'type': 'AdaptiveScalarEncoder',
    'w': 21},
  u'V17':     {   'clipInput': True,
    'fieldname': u'V17',
    'n': 100,
    'name': u'V17',
    'type': 'AdaptiveScalarEncoder',
    'w': 21},
  u'V18':     {   'clipInput': True,
    'fieldname': u'V18',
    'n': 100,
    'name': u'V18',
    'type': 'AdaptiveScalarEncoder',
    'w': 21},
  u'V19':     {   'clipInput': True,
    'fieldname': u'V19',
    'n': 100,
    'name': u'V19',
    'type': 'AdaptiveScalarEncoder',
    'w': 21},
  u'V20':     {   'clipInput': True,
    'fieldname': u'V20',
    'n': 100,
    'name': u'V20',
    'type': 'AdaptiveScalarEncoder',
    'w': 21},
  u'V21':     {   'clipInput': True,
    'fieldname': u'V21',
    'n': 100,
    'name': u'V21',
    'type': 'AdaptiveScalarEncoder',
    'w': 21},
  u'V22':     {   'clipInput': True,
    'fieldname': u'V22',
    'n': 100,
    'name': u'V22',
    'type': 'AdaptiveScalarEncoder',
    'w': 21},
  u'V23':     {   'clipInput': True,
    'fieldname': u'V23',
    'n': 100,
    'name': u'V23',
    'type': 'AdaptiveScalarEncoder',
    'w': 21},
  u'V24':     {   'clipInput': True,
    'fieldname': u'V24',
    'n': 100,
    'name': u'V24',
    'type': 'AdaptiveScalarEncoder',
    'w': 21},
  u'V25':     {   'clipInput': True,
    'fieldname': u'V25',
    'n': 100,
    'name': u'V25',
    'type': 'AdaptiveScalarEncoder',
    'w': 21},
  u'V26':     {   'clipInput': True,
    'fieldname': u'V26',
    'n': 100,
    'name': u'V26',
    'type': 'AdaptiveScalarEncoder',
    'w': 21},
  u'V27':     {   'clipInput': True,
    'fieldname': u'V27',
    'n': 100,
    'name': u'V27',
    'type': 'AdaptiveScalarEncoder',
    'w': 21},
  u'V28':     {   'clipInput': True,
    'fieldname': u'V28',
    'n': 100,
    'name': u'V28',
    'type': 'AdaptiveScalarEncoder',
    'w': 21},
  u'V29':     {   'clipInput': True,
    'fieldname': u'V29',
    'n': 100,
    'name': u'V29',
    'type': 'AdaptiveScalarEncoder',
    'w': 21},
  u'V30':     {   'clipInput': True,
    'fieldname': u'V30',
    'n': 100,
    'name': u'V30',
    'type': 'AdaptiveScalarEncoder',
    'w': 21},
  u'V31':     {   'clipInput': True,
    'fieldname': u'V31',
    'n': 100,
    'name': u'V31',
    'type': 'AdaptiveScalarEncoder',
    'w': 21},
  u'V32':     {   'clipInput': True,
    'fieldname': u'V32',
    'n': 100,
    'name': u'V32',
    'type': 'AdaptiveScalarEncoder',
    'w': 21},
  u'V33':     {   'clipInput': True,
    'fieldname': u'V33',
    'n': 100,
    'name': u'V33',
    'type': 'AdaptiveScalarEncoder',
    'w': 21},
  u'V34':     {   'clipInput': True,
    'fieldname': u'V34',
    'n': 100,
    'name': u'V34',
    'type': 'AdaptiveScalarEncoder',
    'w': 21},
  u'V35':     {   'clipInput': True,
    'fieldname': u'V35',
    'n': 100,
    'name': u'V35',
    'type': 'AdaptiveScalarEncoder',
    'w': 21},
  u'V36':     {   'clipInput': True,
    'fieldname': u'V36',
    'n': 100,
    'name': u'V36',
    'type': 'AdaptiveScalarEncoder',
    'w': 21},
  u'V37':     {   'clipInput': True,
    'fieldname': u'V37',
    'n': 100,
    'name': u'V37',
    'type': 'AdaptiveScalarEncoder',
    'w': 21},
  u'V38':     {   'clipInput': True,
    'fieldname': u'V38',
    'n': 100,
    'name': u'V38',
    'type': 'AdaptiveScalarEncoder',
    'w': 21},
  u'V39':     {   'clipInput': True,
    'fieldname': u'V39',
    'n': 100,
    'name': u'V39',
    'type': 'AdaptiveScalarEncoder',
    'w': 21},
  u'V40':     {   'clipInput': True,
    'fieldname': u'V40',
    'n': 100,
    'name': u'V40',
    'type': 'AdaptiveScalarEncoder',
    'w': 21},
  u'V41':     {   'clipInput': True,
    'fieldname': u'V41',
    'n': 100,
    'name': u'V41',
    'type': 'AdaptiveScalarEncoder',
    'w': 21},
  u'V42':     {   'clipInput': True,
    'fieldname': u'V42',
    'n': 100,
    'name': u'V42',
    'type': 'AdaptiveScalarEncoder',
    'w': 21},
  u'V43':     {   'clipInput': True,
    'fieldname': u'V43',
    'n': 100,
    'name': u'V43',
    'type': 'AdaptiveScalarEncoder',
    'w': 21},
  u'V44':     {   'clipInput': True,
    'fieldname': u'V44',
    'n': 100,
    'name': u'V44',
    'type': 'AdaptiveScalarEncoder',
    'w': 21},
  u'V45':     {   'clipInput': True,
    'fieldname': u'V45',
    'n': 100,
    'name': u'V45',
    'type': 'AdaptiveScalarEncoder',
    'w': 21},
  u'V46':     {   'clipInput': True,
    'fieldname': u'V46',
    'n': 100,
    'name': u'V46',
    'type': 'AdaptiveScalarEncoder',
    'w': 21},
  u'V47':     {   'clipInput': True,
    'fieldname': u'V47',
    'n': 100,
    'name': u'V47',
    'type': 'AdaptiveScalarEncoder',
    'w': 21},
  u'V48':     {   'clipInput': True,
    'fieldname': u'V48',
    'n': 100,
    'name': u'V48',
    'type': 'AdaptiveScalarEncoder',
    'w': 21},
  u'V49':     {   'clipInput': True,
    'fieldname': u'V49',
    'n': 100,
    'name': u'V49',
    'type': 'AdaptiveScalarEncoder',
    'w': 21},
  u'V50':     {   'clipInput': True,
    'fieldname': u'V50',
    'n': 100,
    'name': u'V50',
    'type': 'AdaptiveScalarEncoder',
    'w': 21},
  u'V51':     {   'clipInput': True,
    'fieldname': u'V51',
    'n': 100,
    'name': u'V51',
    'type': 'AdaptiveScalarEncoder',
    'w': 21},
  u'V52':     {   'clipInput': True,
    'fieldname': u'V52',
    'n': 100,
    'name': u'V52',
    'type': 'AdaptiveScalarEncoder',
    'w': 21},
  u'V53':     {   'clipInput': True,
    'fieldname': u'V53',
    'n': 100,
    'name': u'V53',
    'type': 'AdaptiveScalarEncoder',
    'w': 21},
  u'V54':     {   'clipInput': True,
    'fieldname': u'V54',
    'n': 100,
    'name': u'V54',
    'type': 'AdaptiveScalarEncoder',
    'w': 21},
            },

            # A dictionary specifying the period for automatically-generated
            # resets from a RecordSensor;
            #
            # None = disable automatically-generated resets (also disabled if
            # all of the specified values evaluate to 0).
            # Valid keys is the desired combination of the following:
            #   days, hours, minutes, seconds, milliseconds, microseconds, weeks
            #
            # Example for 1.5 days: sensorAutoReset = dict(days=1,hours=12),
            #
            # (value generated from SENSOR_AUTO_RESET)
            'sensorAutoReset' : None,
        },

        'spEnable': True,

        'spParams': {
            # Spatial pooler implementation to use. 
            # Options: "py" (slow, good for debugging), and "cpp" (optimized).
            'spatialImp': 'cpp',

            # SP diagnostic output verbosity control;
            # 0: silent; >=1: some info; >=2: more info;
            'spVerbosity' : 0,

            'globalInhibition': 1,

            # Number of cell columns in the cortical region (same number for
            # SP and TP)
            # (see also tpNCellsPerCol)
            'columnCount': 2048,

            'inputWidth': 0,

            # SP inhibition control (absolute value);
            # Maximum number of active columns in the SP region's output (when
            # there are more, the weaker ones are suppressed)
            'numActiveColumnsPerInhArea': 40,

            'seed': 1956,

            # potentialPct
            # What percent of the columns's receptive field is available
            # for potential synapses. 
            'potentialPct': 0.8,

            # The default connected threshold. Any synapse whose
            # permanence value is above the connected threshold is
            # a "connected synapse", meaning it can contribute to the
            # cell's firing. Typical value is 0.10. Cells whose activity
            # level before inhibition falls below minDutyCycleBeforeInh
            # will have their own internal synPermConnectedCell
            # threshold set below this default value.
            # (This concept applies to both SP and TP and so 'cells'
            # is correct here as opposed to 'columns')
            'synPermConnected': 0.1,

            'synPermActiveInc': 0.05,

            'synPermInactiveDec': 0.0005,
            
            'maxBoost': 1.0
        },

        # Controls whether TP is enabled or disabled;
        # TP is necessary for making temporal predictions, such as predicting
        # the next inputs.  Without TP, the model is only capable of
        # reconstructing missing sensor inputs (via SP).
        'tpEnable' : True,

        'tpParams': {
            # TP diagnostic output verbosity control;
            # 0: silent; [1..6]: increasing levels of verbosity
            # (see verbosity in nupic/trunk/py/nupic/research/TP.py and TP10X*.py)
            'verbosity': 0,

            # Number of cell columns in the cortical region (same number for
            # SP and TP)
            # (see also tpNCellsPerCol)
            'columnCount': 2048,

            # The number of cells (i.e., states), allocated per column.
            'cellsPerColumn': 32,

            'inputWidth': 2048,

            'seed': 1960,

            # Temporal Pooler implementation selector (see _getTPClass in
            # CLARegion.py).
            'temporalImp': 'cpp',

            # New Synapse formation count
            # NOTE: If None, use spNumActivePerInhArea
            'newSynapseCount': 20,

            # Maximum number of synapses per segment
            'maxSynapsesPerSegment': 32,

            # Maximum number of segments per cell
            'maxSegmentsPerCell': 128,

            # Initial Permanence
            'initialPerm': 0.21,

            # Permanence Increment
            'permanenceInc': 0.1,

            # Permanence Decrement
            # If set to None, will automatically default to tpPermanenceInc
            # value.
            'permanenceDec' : 0.1,

            'globalDecay': 0.0,

            'maxAge': 0,

            # Minimum number of active synapses for a segment to be considered
            # during search for the best-matching segments.
            # None=use default
            # Replaces: tpMinThreshold
            'minThreshold': 12,

            # Segment activation threshold.
            # A segment is active if it has >= tpSegmentActivationThreshold
            # connected synapses that are active due to infActiveState
            # None=use default
            # Replaces: tpActivationThreshold
            'activationThreshold': 16,

            'outputType': 'normal',

            # "Pay Attention Mode" length. This tells the TP how many new
            # elements to append to the end of a learned sequence at a time.
            # Smaller values are better for datasets with short sequences,
            # higher values are better for datasets with long sequences.
            'pamLength': 1,
        },

        'clParams': {
            'regionName' : 'SDRClassifierRegion',
            
            # Classifier diagnostic output verbosity control;
            # 0: silent; [1..6]: increasing levels of verbosity
            'verbosity' : 0,

            # This controls how fast the classifier learns/forgets. Higher values
            # make it adapt faster and forget older patterns faster.
            'alpha': 0.001,

            # This is set after the call to updateConfigFromSubConfig and is
            # computed from the aggregationInfo and predictAheadTime.
            'steps': '1',
        },

        'anomalyParams': {   u'anomalyCacheRecords': None,
    u'autoDetectThreshold': None,
    u'autoDetectWaitRecords': None},

        'trainSPNetOnlyIfRequested': False,
    },
}
# end of config dictionary


# Adjust base config dictionary for any modifications if imported from a
# sub-experiment
updateConfigFromSubConfig(config)


# Compute predictionSteps based on the predictAheadTime and the aggregation
# period, which may be permuted over.
if config['predictAheadTime'] is not None:
  predictionSteps = int(round(aggregationDivide(
      config['predictAheadTime'], config['aggregationInfo'])))
  assert (predictionSteps >= 1)
  config['modelParams']['clParams']['steps'] = str(predictionSteps)


# Adjust config by applying ValueGetterBase-derived
# futures. NOTE: this MUST be called after updateConfigFromSubConfig() in order
# to support value-getter-based substitutions from the sub-experiment (if any)
applyValueGettersToContainer(config)



control = {
  # The environment that the current model is being run in
  "environment": 'nupic',

  # Input stream specification per py/nupic/frameworks/opf/jsonschema/stream_def.json.
  #
  'dataset' : {   u'info': u'heart-beat',
        u'streams': [   {   u'columns': [u'*'],
                            u'info': u'Rec Center',
                            u'source': u'file://heart-beat.csv'}],
        u'version': 1},

  # Iteration count: maximum number of iterations.  Each iteration corresponds
  # to one record from the (possibly aggregated) dataset.  The task is
  # terminated when either number of iterations reaches iterationCount or
  # all records in the (possibly aggregated) database have been processed,
  # whichever occurs first.
  #
  # iterationCount of -1 = iterate over the entire dataset
  'iterationCount' : -1,


  # A dictionary containing all the supplementary parameters for inference
  "inferenceArgs":{u'inputPredictedField': 'yes',
 u'predictedField': u'heartbeat',
 u'predictionSteps': [1]},

  # Metrics: A list of MetricSpecs that instantiate the metrics that are
  # computed for this experiment
  'metrics':[
    MetricSpec(field=u'heartbeat', metric='multiStep', inferenceElement='multiStepBestPredictions', params={'window': 1000, 'steps': [1], 'errorMetric': 'aae'}),
    MetricSpec(field=u'heartbeat', metric='multiStep', inferenceElement='multiStepBestPredictions', params={'window': 1000, 'steps': [1], 'errorMetric': 'altMAPE'})
  ],

  # Logged Metrics: A sequence of regular expressions that specify which of
  # the metrics from the Inference Specifications section MUST be logged for
  # every prediction. The regex's correspond to the automatically generated
  # metric labels. This is similar to the way the optimization metric is
  # specified in permutations.py.
  'loggedMetrics': ['.*'],
}



descriptionInterface = ExperimentDescriptionAPI(modelConfig=config,
                                                control=control)