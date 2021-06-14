# -*- coding: utf-8 -*-
"""
Created on Thu Mar 11 15:15:24 2020

@author: frisby-justin
"""

import pandas as pd
pd.set_option('display.max_columns', None)
import pyodbc
pyodbc.pooling = False
import datetime as dt
import sys
import os
import traceback
import numpy as np
sys.path.append('//fs02/Medical-Informatics/shared_utilities/')
from database import formatMrn, generate_mrns_to_insert, generate_csns_to_insert
import matplotlib.pyplot as plt
from matplotlib import mlab

def time_f(t_secs):
    try:
        val = int(t_secs)
    except ValueError:
        return "!!!ERROR: ARGUMENT NOT AN INTEGER!!!"
    pos = abs( int(t_secs) )
    day = pos / (3600*24)
    rem = pos % (3600*24)
    hour = rem / 3600
    rem = rem % 3600
    mins = rem / 60
    secs = rem % 60
    res = '%02dd:%02dh:%02dm:%02ds' % (day, hour, mins, secs)
    if int(t_secs) < 0:
        res = "-%s" % res
    return res

def timeDiffBetweenSepsisDx(parameter, new_column):
    if ((sep.dtypes[parameter] == np.dtype('datetime64[ns]')) |\
    (sep.dtypes[parameter] == np.dtype('<M8[ns]'))):
    
        mask_parameter_exam_C = (((sep[parameter].isnull() == False) & (sep['Severe Sepsis Presentation Time'].isnull() == False))
                             &
                         ((sep[parameter] - sep['Severe Sepsis Presentation Time']).dt.total_seconds() > 0))
        mask_parameter_exam_0 = (((sep[parameter].isnull() == False) & (sep['Severe Sepsis Presentation Time'].isnull() == False))
                         &
                         ((sep[parameter] - sep['Severe Sepsis Presentation Time']).dt.total_seconds() < 0))
    
        sep.loc[mask_parameter_exam_C, new_column+'temp'] = (sep[parameter] - (sep['Severe Sepsis Presentation Time'])).dt.total_seconds()
        sep.loc[mask_parameter_exam_0, new_column+'temp'] = 0
    
    else:
        sep[new_column+'temp'] = np.nan
        
    sep[new_column] = '0'
    
    for i in range(0,len(sep)):
        if sep.iloc[i][new_column+'temp'] > 0:
            sep.at[i,new_column]= time_f(sep.iloc[i][new_column+'temp'])
        elif sep.iloc[i][new_column+'temp'] == 0:
            sep.at[i,new_column]='00d:00h:00m:00s'
        else:
            sep.at[i,new_column]= np.nan   
            
    del sep[new_column+'temp']

os.chdir('//fs02/Medical-Informatics/justin/SepsisAbs/')
        ######## GRAB MAIN QUERY WITH ENCOUNTERS ########
with open ('Sepsis Abstractions_v5.sql', 'r') as f:
    sql = ''.join(f.readlines()).format()
    f.close()
con = pyodbc.connect('DSN=Clarity;DATABASE=Clarity;Trusted_Connection=yes')
sep = pd.read_sql_query(sql, con) 
con.close()

sep['Fluid Resuscitation Required'] = np.where(((sep['Fluid_Resuscitation_SBP_Under_90'] == 1)
                                      |
                                      (sep['Fluid_Resuscitation_Lactate_Over_4'] == 1)
                                      |
                                      (sep['Fluid_Resuscitation_MAP_Under_65'] == 1)
                                      |
                                      (sep['Fluid_Resuscitation_SBP_decrease_40mmHg'] == 1)), 'Yes', 'No')
    
sep['Organ Dysfunction'] = np.where(((sep['Organ_Dysfunction_SBP_Under_90'] == 1)
                                      |
                                      (sep['Organ_Dysfunction_Lactate_Over_0'] == 1)
                                      |
                                      (sep['Organ_Dysfunction_Respiratory_Failure'] == 1)
                                      |
                                      (sep['Organ_Dysfunction_Creatinine_Urinary'] == 1)
                                      |
                                      (sep['Organ_Dysfunction_MAP_Under_65'] == 1)
                                      |
                                      (sep['Organ_Dysfunction_Bilirubin_Over_2'] == 1)
                                      |
                                      (sep['Organ_Dysfunction_Elevated_INR_PTT'] == 1)
                                      |
                                      (sep['Organ_Dysfunction_PLT_Under_100k'] == 1)), 'Yes', 'No')    

#### ICU LENGTH OF STAY
mask_ICU = ((sep.ICUAdmitTime.isnull() == False) & (sep.ICUDCTime.isnull() == False)) 
sep['ICU Length of Stay_Temp'] = np.where(mask_ICU,
                                     (sep['ICUDCTime'] - sep['ICUAdmitTime']).dt.total_seconds(),
                                     np.nan)
sep['ICU Length of Stay'] = '0'
for i in range(0,len(sep)):
        if sep.iloc[i]['ICU Length of Stay_Temp'] > 0:
            sep.at[i,'ICU Length of Stay']= time_f(sep.iloc[i]['ICU Length of Stay_Temp'])
        elif sep.iloc[i]['ICU Length of Stay_Temp'] == 0:
            sep.at[i,'ICU Length of Stay']='00d:00h:00m:00s'
        else:
            sep.at[i,'ICU Length of Stay']= np.nan   
            
del sep['ICU Length of Stay_Temp']

#### ADMISSION LENGTH OF STAY
#mask_Admit = ((sep['Admit Time'].isnull() == False) & (sep['DCTime'].isnull() == False)) 
#sep['Admit Length of Stay_Temp'] = np.where(mask_Admit,
#                                     (sep['DCTime']- sep['Admit Time']).dt.total_seconds(),
#                                     np.nan)
#sep['Admit Length of Stay'] = '0'
#for i in range(0,len(sep)):
#        if sep.iloc[i]['Admit Length of Stay_Temp'] > 0:
#            sep.at[i,'Admit Length of Stay']= time_f(sep.iloc[i]['Admit Length of Stay_Temp'])
#        elif sep.iloc[i]['Admit Length of Stay_Temp'] == 0:
#            sep.at[i,'Admit Length of Stay']='00d:00h:00m:00s'
#        else:
#            sep.at[i,'Admit Length of Stay']= np.nan   
#            
#del sep['Admit Length of Stay_Temp']

##### PARAMETER PERFORMED
## Formula: IF paramater time - severe sepsis time <0 THEN 0
#           ELSE parameter time - severe sepsis time
et_time_dicts = {'Lactate Time' : 'LactateET',
                 'RepeatLactateTime':'RepeatLactateET',
                 'Culture Time':'CultureET',
                 'CrystalloidTime':'CrystalloidET',
                 'CVP8Time':'CVP8TET',
                 'ScvO2Time':'ScvO2ET'}

for parameter,new_column in et_time_dicts.items():
    timeDiffBetweenSepsisDx(parameter, new_column)
    
#### ELEVATED LACTATE
# Formula: IF Lactate.isin(>4, >2 but <4) THEN 'Yes',
#          ELSE 'NO'
sep.loc[sep['Lactate'].isin(['>4', '>2 but <4']), 'ElevatedLactate'] = 'Yes'
sep.loc[sep['Lactate'] == '<2', 'ElevatedLactate'] = 'No'

#### 3 HOUR ELIGIBLE
# Formula: IF SEVERE SEPSIS TIME IN NOT NULL THEN YES ELSE NO
sep.loc[sep['Severe Sepsis Presentation Time'].isnull() == False, '3_Hour_Eligible'] = 'Yes'
sep.loc[sep['Severe Sepsis Presentation Time'].isnull(), '3_Hour_Eligible'] = 'No'

#### BY, BZ, CA, CC
by_to_cc = {'Lactate Time' : 'B3_1 Lactate Time',
            'Culture Time':'B3_2 Culture Time',
            'Antibx Time':'B3_3 Antibiotic Time',
            'CrystalloidTime':'B3_4 Crystalloid Time'}
for parameter,new_column in by_to_cc.items():
    timeDiffBetweenSepsisDx(parameter, new_column)

#### LACTATE >4
sep.loc[sep['Lactate'] == '>4', 'Lactate'] = '>/=4'
sep.loc[sep['Lactate Actual Result'] == '4.0', 'Lactate'] = '>/=4'
sep.loc[sep['Lactate'] == '>/=4', 'Lactate >/=4'] = 'Yes'
sep.loc[sep['Lactate'] != '>/=4', 'Lactate >/=4'] = 'No'

#### B3_2 CULTURE BEFORE ANTIBIOTICS
# Formula: IF ABX == YES - MONOTHERAPY OR YES - COMBINATION THERATPY
#          AND
#          CULTURE DONE == YES
#          AND
#          ANTIBIOTICS TIME IS GREATER THAN CULTURE TIME THEN YES
#       ELSE NO
mask_abx_aft_cult_Y = (
                 (sep['Abx'].isin(['Yes - Monotherapy','Yes - Combination Therapy']))
                 & 
                 (sep['Culture Done'] == 'Yes')
                 &
                 (sep['Antibx Time'] > sep['Culture Time'])
                )

mask_abx_aft_cult_N = ((sep['Abx'].isin(['Yes - Monotherapy','Yes - Combination Therapy']))
                        & 
                        (sep['Culture Done'] == 'Yes')
                        &
                        (sep['Antibx Time'] < sep['Culture Time']))
sep.loc[mask_abx_aft_cult_Y, 'B3_2 Culture before Anti'] = 'Yes'
sep.loc[mask_abx_aft_cult_N, 'B3_2 Culture before Anti'] = 'No'
sep['B3_2 Culture before Anti'] = np.where(sep['Blood Culture Delay'] == 'Yes','Yes',sep['B3_2 Culture before Anti'])

testDf = sep[sep['Blood Culture Delay'].isnull() == False]
##### LACTATE SCORE --  NEED TO REVISIT, UNCLEAR DEFINITION
# Formula: IF 3 HOUR ELIGIBLE == 'YES' AND LACTATE DONE == 'YES' AND LACTATE TIME WITHIN 3 HOURS THEN 1
#           
#          ABX == YES - MONOTHERAPY OR YES - COMBINATION THERATPY
#          AND
#          CULTURE DONE == YESsep['status']
#          AND
#          ANTIBIOTICS TIME IS GREATER THAN CULTURE TIME THEN YES
#       ELSE NO
three_hours = 3600 * 3

### LACTATE SCORE
mask_score_elig_N = (sep['3_Hour_Eligible']== 'No')

mask_score_fail1 = ((sep['3_Hour_Eligible']== 'Yes')
                 & 
                 (sep['Lactate Done'] == 'No'))

mask_score_fail2 = ((sep['3_Hour_Eligible']== 'Yes')
                 & 
                 (sep['Lactate Done'] == 'Yes')
                &
                ((sep['Lactate Time'] - sep['Severe Sepsis Presentation Time'])\
                 .dt.total_seconds() > three_hours))
                
mask_score_success_Y = ((sep['3_Hour_Eligible']== 'Yes')
                 & 
                 (sep['Lactate Done'] == 'Yes')
                &
                ((sep['Lactate Time'] - sep['Severe Sepsis Presentation Time'])\
                 .dt.total_seconds() <= three_hours))
sep.loc[mask_score_success_Y, 'B3_1 Lactate Score'] =  1
sep.loc[mask_score_fail1, 'B3_1 Lactate Score'] =  0
sep.loc[mask_score_fail2, 'B3_1 Lactate Score'] =  0
sep.loc[mask_score_elig_N, 'B3_1 Lactate Score'] =  np.nan

## CULTURE SCORE
mask_score_fail1 = ((sep['3_Hour_Eligible']== 'Yes')
                 & 
                 (sep['Culture Done'] == 'No'))

mask_score_fail2 = ((sep['3_Hour_Eligible']== 'Yes')
                 & 
                 (sep['Culture Done'] == 'Yes')
                &
                ((sep['Culture Time'] - sep['Severe Sepsis Presentation Time'])\
                 .dt.total_seconds() > three_hours))
                
mask_score_success_Y = ((sep['3_Hour_Eligible']== 'Yes')
                 & 
                 (sep['Culture Done'] == 'Yes')
                &
                ((sep['Culture Time'] - sep['Severe Sepsis Presentation Time'])\
                 .dt.total_seconds() <= three_hours))
sep.loc[mask_score_success_Y, 'B3_2 Blood Score'] =  1
sep.loc[mask_score_fail1, 'B3_2 Blood Score'] =  0
sep.loc[mask_score_fail2, 'B3_2 Blood Score'] =  0
sep.loc[mask_score_elig_N, 'B3_2 Blood Score'] =  np.nan

## ANTIBIOTIC SCORE
mask_score_fail1 = ((sep['3_Hour_Eligible']== 'Yes')
                 & 
                 (sep['Abx'] == 'No'))

mask_score_fail2 = ((sep['3_Hour_Eligible']== 'Yes')
                 & 
                 (sep['Abx'].isin(['Yes - Combination Therapy', 'Yes - Monotherapy']) == True)
                &
                ((sep['Antibx Time'] - sep['Severe Sepsis Presentation Time'])\
                 .dt.total_seconds() > three_hours))
                
mask_score_success_Y = ((sep['3_Hour_Eligible']== 'Yes')
                 & 
                 (sep['Abx'].isin(['Yes - Combination Therapy', 'Yes - Monotherapy']) == True)
                &
                ((sep['Antibx Time'] - sep['Severe Sepsis Presentation Time'])\
                 .dt.total_seconds() <= three_hours))
sep.loc[mask_score_success_Y, 'B3_3 Antibiotic Score'] =  1
sep.loc[mask_score_fail1, 'B3_3 Antibiotic Score'] =  0
sep.loc[mask_score_fail2, 'B3_3 Antibiotic Score'] =  0
sep.loc[mask_score_elig_N, 'B3_3 Antibiotic Score'] =  np.nan

## CRYSTALLOID SCORE 
mask_crystal_elig_N = ((sep['Lactate >/=4'] != 'Yes') & ((sep['Fluid_Resuscitation_SBP_Under_90'] != 1)
                                                       &
                                                       (sep['Fluid_Resuscitation_MAP_Under_65'] != 1)
                                                       &
                                                       (sep['Fluid_Resuscitation_SBP_decrease_40mmHg'] != 1)))
mask_crystal_elig = ((sep['Lactate >/=4'] == 'Yes') | ((sep['Fluid_Resuscitation_SBP_Under_90'] == 1)
                                                       |
                                                       (sep['Fluid_Resuscitation_MAP_Under_65'] == 1)
                                                       |
                                                       (sep['Fluid_Resuscitation_SBP_decrease_40mmHg'] == 1)))

mask_score_fail1 = (mask_crystal_elig
                 & 
                 (sep['Fluid Resuscitation Required'] == 'No'))

mask_score_fail2 = (mask_crystal_elig
                 & 
                 (sep['Fluid Resuscitation Required'] == 'Yes')
                &
                ((sep['CrystalloidTime'] - sep['Severe Sepsis Presentation Time'])\
                 .dt.total_seconds() > three_hours))
                
mask_score_success_Y = (mask_crystal_elig
                 & 
                 (sep['Fluid Resuscitation Required'] == 'Yes')
                &
                ((sep['CrystalloidTime'] - sep['Severe Sepsis Presentation Time'])\
                 .dt.total_seconds() <= three_hours))

sep.loc[mask_score_success_Y, 'B3_4 Crystalloid Score'] =  1
sep.loc[mask_score_fail1, 'B3_4 Crystalloid Score'] =  0
sep.loc[mask_score_fail2, 'B3_4 Crystalloid Score'] =  0
sep.loc[mask_crystal_elig_N, 'B3_4 Crystalloid Score'] =  np.nan

# NEXT SET OF TIME DIFFERENCES WITH SEPSIS PRESENTATION
cl_to_cr = {'Vasopressor Time' : 'B6_1 Vaso Time',
            'Focused Exam Time':'Focused Exam Performed Time',
            'CVP8Time':'B6_2 CVP Achieved 6Hr Time',
            'ScvO2Time':'B6_2 Scv0 Achieved 6Hr Time',
            'Bedside Ultrasound Time':'Bedside Ultrasound Performed Time',
            'Passive Leg Raise Time':'Passive Leg Raise Performed',
            'Fluid Challenge Time' : 'Fluid Challenge Time Performed',
            'RepeatLactateTime' : 'B6_4 Lactate Remeasure'}
for parameter,new_column in cl_to_cr.items():
    timeDiffBetweenSepsisDx(parameter, new_column)

sep['Passive Leg Raise Performed or Fluid Challenge Time'] = np.where(
                                                sep['Passive Leg Raise Performed'].isnull() == False,
                                                sep['Passive Leg Raise Performed'],
                                                sep['Fluid Challenge Time Performed'])

del sep['Fluid Challenge Time Performed']
del sep['Passive Leg Raise Performed']

##### MORTALITY
sep['Mortality'] = np.where(sep['status'] == 'Deceased', 'Yes', 'No')

##### VASO PRESSURE ELIG
## Formula: IF Organ_Dysfunction_SBP_Under_90 == 1 AND  Organ_Dysfunction_MAP_Under_65 == 0
sep['VasoPressor Eligible'] = np.where(((sep['Organ_Dysfunction_SBP_Under_90'] == 1) &\
                                        (sep['Organ_Dysfunction_MAP_Under_65'] == 0)),'Yes', 'No')

##### HYPO OR LACTATE ELIGIBLE
## Formula: IF Organ Dysfunction == 'SBP <90' OR LACTATE >4 THEN YES ELSE NO
sep['Hypo or Lactate Elig'] = np.where(((sep['Organ Dysfunction'] == 'SBP <90') | (sep['Lactate'] == '>/=4')),'Yes', 'No')

##### LACTATE REMEASURE ELIGIBLE
## Formula: IF Lactate.isin(>2 and <4, >4) THEN YES ELSE NO
sep.loc[sep['Lactate'].isin(['>/=4', '>2 but <4']), 'Lactate Remeasure Eligible'] = 'Yes'
sep.loc[sep['Lactate'] == '<2', 'Lactate Remeasure Eligible'] = 'No'

six_hours = 3600 * 6
timeParameters= ['Passive Leg Raise Time', 'Fluid Challenge Time', 'RepeatLactateTime',
                 'Focused Exam Time', 'CVP8Time', 'ScvO2Time', 'Bedside Ultrasound Time',
                 'Vasopressor Time']
for parameter in timeParameters:
    if ((sep.dtypes[parameter] == np.dtype('datetime64[ns]')) |\
    (sep.dtypes[parameter] == np.dtype('<M8[ns]'))):
        sep[parameter] = sep[parameter] 
    else:
        sep[parameter] = np.datetime64('NaT')
        
#### VASO SCORE
mask_score_eligible = sep['VasoPressor Eligible'] == 'Yes'
mask_score_ineligible = sep['VasoPressor Eligible'] == 'No'

mask_score_failure1 = ((mask_score_eligible)
                &
                (sep['Pressors'] != 'Yes'))
mask_score_failure2 = ((mask_score_eligible)
                &
                (sep['Pressors'] == 'Yes')
                &
                ((sep['Vasopressor Time'] - sep['Severe Sepsis Presentation Time'])\
                 .dt.total_seconds() > six_hours))
mask_score_successful = ((mask_score_eligible)
                &
                (sep['Pressors'] == 'Yes')
                &
                ((sep['Vasopressor Time'] - sep['Severe Sepsis Presentation Time'])\
                 .dt.total_seconds() <= six_hours))

sep.loc[mask_score_successful, 'B6_1 Vaso Score'] =  1
sep.loc[mask_score_failure1, 'B6_1 Vaso Score'] =  0
sep.loc[mask_score_failure2, 'B6_1 Vaso Score'] =  0
sep.loc[mask_score_ineligible, 'B6_1 Vaso Score'] =  np.nan

#### LACTATE REMEASURE SCORE
mask_score_eligible = sep['Lactate Remeasure Eligible'] == 'Yes'
mask_score_ineligible = sep['Lactate Remeasure Eligible'] == 'No'

mask_score_failure1 = ((mask_score_eligible)
                &
                (sep['RepeatLactate'].isnull() == True))
mask_score_failure2 = ((mask_score_eligible)
                &
                (sep['RepeatLactate'].isnull() == False)
                &
                ((sep['RepeatLactateTime'] - sep['Severe Sepsis Presentation Time'])\
                 .dt.total_seconds() > six_hours))
mask_score_successful = ((mask_score_eligible)
                 &
                (sep['RepeatLactate'].isnull() == False)                
                &
                ((sep['RepeatLactateTime'] - sep['Severe Sepsis Presentation Time'])\
                 .dt.total_seconds() <= six_hours))

sep.loc[mask_score_successful, 'B6_4 Lactate Remeasure Score'] =  1
sep.loc[mask_score_failure1, 'B6_4 Lactate Remeasure Score'] =  0
sep.loc[mask_score_failure2, 'B6_4 Lactate Remeasure Score'] =  0
sep.loc[mask_score_ineligible, 'B6_4 Lactate Remeasure Score'] =  np.nan

#### FOCUSED EXAM PERFORMED SCORE
mask_score_eligible = sep['Hypo or Lactate Elig'] == 'Yes'
mask_score_ineligible = sep['Hypo or Lactate Elig'] == 'No'

mask_score_failure1 = ((mask_score_eligible)
                &
                (sep['Focused Exam Performed'] != 'Yes'))
mask_score_failure2 = ((mask_score_eligible)
                &
                (sep['Focused Exam Performed'].isnull() == False)
                &
                ((sep['Focused Exam Time'] - sep['Severe Sepsis Presentation Time'])\
                 .dt.total_seconds() > six_hours))
mask_score_successful = ((mask_score_eligible)
                 &
                (sep['Focused Exam Performed'].isnull() == False)                
                &
                ((sep['Focused Exam Time'] - sep['Severe Sepsis Presentation Time'])\
                 .dt.total_seconds() <= six_hours))

sep.loc[mask_score_successful, 'Focused Exam Performed Score'] =  1
sep.loc[mask_score_failure1, 'Focused Exam Performed Score'] =  0
sep.loc[mask_score_failure2, 'Focused Exam Performed Score'] =  0
sep.loc[mask_score_ineligible, 'Focused Exam Performed Score'] =  np.nan

#### CVP SCORE
mask_score_eligible = sep['Hypo or Lactate Elig'] == 'Yes'
mask_score_ineligible = sep['Hypo or Lactate Elig'] == 'No'

mask_score_failure1 = ((mask_score_eligible)
                &
                (sep['CVP8Time'].isnull() == True))
mask_score_failure2 = ((mask_score_eligible)
                &
                (sep['CVP8Time'].isnull() == False)
                &
                ((sep['CVP8Time'] - sep['Severe Sepsis Presentation Time'])\
                 .dt.total_seconds() > six_hours))
mask_score_successful = ((mask_score_eligible)
                 &
                (sep['CVP8Time'].isnull() == False)                
                &
                ((sep['CVP8Time'] - sep['Severe Sepsis Presentation Time'])\
                 .dt.total_seconds() <= six_hours))

sep.loc[mask_score_successful, 'B6_2 CVP Score'] =  1
sep.loc[mask_score_failure1, 'B6_2 CVP Score'] =  0
sep.loc[mask_score_failure2, 'B6_2 CVP Score'] =  0
sep.loc[mask_score_ineligible, 'B6_2 CVP Score'] =  np.nan


#### SCV0 SCORE
mask_score_failure1 = ((mask_score_eligible)
                &
                (sep['ScvO2Time'].isnull() == True))
mask_score_failure2 = ((mask_score_eligible)
                &
                (sep['ScvO2Time'].isnull() == False)
                &
                ((sep['ScvO2Time'] - sep['Severe Sepsis Presentation Time'])\
                 .dt.total_seconds() > six_hours))
mask_score_successful = ((mask_score_eligible)
                 &
                (sep['ScvO2Time'].isnull() == False)                
                &
                ((sep['ScvO2Time'] - sep['Severe Sepsis Presentation Time'])\
                 .dt.total_seconds() <= six_hours))

sep.loc[mask_score_successful, 'B6_3 Scv0 Achieved Score'] =  1
sep.loc[mask_score_failure1, 'B6_3 Scv0 Achieved Score'] =  0
sep.loc[mask_score_failure2, 'B6_3 Scv0 Achieved Score'] =  0
sep.loc[mask_score_ineligible, 'B6_3 Scv0 Achieved Score'] =  np.nan

#### BEDSIDE ULTRASOUND SCORE
mask_score_failure1 = ((mask_score_eligible)
                &
                (sep['Bedside Ultrasound Time'].isnull() == True))
mask_score_failure2 = ((mask_score_eligible)
                &
                (sep['Bedside Ultrasound Time'].isnull() == False)
                &
                ((sep['Bedside Ultrasound Time'] - sep['Severe Sepsis Presentation Time'])\
                 .dt.total_seconds() > six_hours))
mask_score_successful = ((mask_score_eligible)
                 &
                (sep['Bedside Ultrasound Time'].isnull() == False)                
                &
                ((sep['Bedside Ultrasound Time'] - sep['Severe Sepsis Presentation Time'])\
                 .dt.total_seconds() <= six_hours))

sep.loc[mask_score_successful, 'Bedside Ultrasound Score'] =  1
sep.loc[mask_score_failure1, 'Bedside Ultrasound Score'] =  0
sep.loc[mask_score_failure2, 'Bedside Ultrasound Score'] =  0
sep.loc[mask_score_ineligible, 'Bedside Ultrasound Score'] =  np.nan


#### Passive Leg Raise Performed or Fluid Challenge Time SCORE        
sep['Passive Leg Raise Performed or Fluid Challenge Time_tempForCalc1'] = np.where(
                                                sep['Fluid Challenge Time'].isnull() == False,
                                                sep['Fluid Challenge Time'],
                                                np.datetime64('NaT'))
sep['Passive Leg Raise Performed or Fluid Challenge Time_tempForCalc2'] = np.where(((
                                                sep['Passive Leg Raise Time'].isnull() == False)
                                                & 
                                                (sep['Passive Leg Raise Performed or Fluid Challenge Time_tempForCalc1'].isnull())),
                                                sep['Passive Leg Raise Time'],
                                                sep['Passive Leg Raise Performed or Fluid Challenge Time_tempForCalc1'])
del sep['Passive Leg Raise Performed or Fluid Challenge Time_tempForCalc1']

mask_score_failure1 = ((mask_score_eligible)
                &
                (sep['Passive Leg Raise Performed or Fluid Challenge Time_tempForCalc2'].isnull() == True))
mask_score_failure2 = ((mask_score_eligible)
                 &
                (sep['Passive Leg Raise Performed or Fluid Challenge Time_tempForCalc2'].isnull() == False)                
                &
                ((sep['Passive Leg Raise Performed or Fluid Challenge Time_tempForCalc2'] - sep['Severe Sepsis Presentation Time'])\
                 .dt.total_seconds() > six_hours))
mask_score_successful = ((mask_score_eligible)
                 &
                (sep['Passive Leg Raise Performed or Fluid Challenge Time_tempForCalc2'].isnull() == False)                
                &
                ((sep['Passive Leg Raise Performed or Fluid Challenge Time_tempForCalc2'] - sep['Severe Sepsis Presentation Time'])\
                 .dt.total_seconds() <= six_hours))

sep.loc[mask_score_successful, 'Passive Leg Raise Performed or Fluid Challenge Score'] =  1
sep.loc[mask_score_failure1, 'Passive Leg Raise Performed or Fluid Challenge Score'] =  0
sep.loc[mask_score_failure2, 'Passive Leg Raise Performed or Fluid Challenge Score'] =  0
sep.loc[mask_score_ineligible, 'Passive Leg Raise Performed or Fluid Challenge Score'] =  np.nan
del sep['Passive Leg Raise Performed or Fluid Challenge Time_tempForCalc2']

#### CALCULATIONS
# 3hr all, if all of them are 1 then 1 if not, 0

sep['3HR_Eligible_Scores'] = sep.filter(items=['B3_1 Lactate Score', 'B3_2 Blood Score', 'B3_3 Antibiotic Score',
           'B3_4 Crystalloid Score']).count(axis=1)
sep['3HR_Scores_Passed'] = sep.filter(items=['B3_1 Lactate Score', 'B3_2 Blood Score', 'B3_3 Antibiotic Score',
           'B3_4 Crystalloid Score']).sum(axis=1)
sep['B3_All'] = np.where(((sep['3HR_Eligible_Scores'] > 0) &
                          (sep['3HR_Eligible_Scores'] == sep['3HR_Scores_Passed'])),1,0)
sep.loc[(sep['3HR_Eligible_Scores'] == 0), 'B3_All'] = np.nan

#### 6hr all, Count yes for Hypo or lactate elgigible, lactate remeasure, vasopressor eligible for denominator
#### 6hr all, sum vasoScore and Lactate remeasure, then either Focused exam =1 OR (CVP,scv0,ultrasound,passive) > 1
isY = lambda x:int(x=='Yes')
countSixHrEligible = lambda row: isY(row['VasoPressor Eligible']) +\
                               isY(row['Hypo or Lactate Elig']) +\
                               isY(row['Lactate Remeasure Eligible'])

sep['6HR_Eligible_Scores'] = sep.apply(countSixHrEligible,axis=1)

sep['6HR_Scores_Passed_1'] = sep.filter(items=['B6_1 Vaso Score','B6_4 Lactate Remeasure Score']).sum(axis=1)
sep['6HR_Scores_Passed_CvScBuPlr'] = sep.filter(items=['B6_2 CVP Score','B6_3 Scv0 Achieved Score',
                                                       'Bedside Ultrasound Score',
                                                       'Passive Leg Raise Performed or Fluid Challenge Score'])\
                                                        .sum(axis=1)
sep['6HR_Scores_Passed_2'] = np.where(sep['6HR_Scores_Passed_CvScBuPlr'] > 0,
                                      sep['6HR_Scores_Passed_CvScBuPlr'],
                                      sep['Focused Exam Performed Score'])
sep['6HR_Scores_Passed'] = sep.filter(items=['6HR_Scores_Passed_1','6HR_Scores_Passed_2']).sum(axis=1)
    
sep['B6_All'] = np.where(((sep['6HR_Eligible_Scores'] > 0) &
                          (sep['6HR_Eligible_Scores'] == sep['6HR_Scores_Passed'])),1,0)
sep.loc[(sep['6HR_Eligible_Scores'] == 0), 'B6_All'] = np.nan

#### 3 Hour Eligible
sep['3 Hour Eligible'] = sep['3HR_Eligible_Scores']
sep['3 Hour Success'] = sep['3HR_Scores_Passed']

#### 6 Hour Eligible
sep['6 Hour Eligible'] = sep['6HR_Eligible_Scores']
sep['6 Hour Success'] = sep['6HR_Scores_Passed']

## MAKE UTILITY COLUMNS
# SVR SEP OR SEP SHOCK

sep['Sepsis Presentation DTTM (Any)'] = np.where(sep['Septic Shock Presentation Time'].dt.year > 2017,
                                                 sep['Septic Shock Presentation Time'],
                                                 sep['Severe Sepsis Presentation Time'])

# SVT SEP OR SEP SHOCK TIME
sep['Sepsis Type'] = np.where(sep['Septic Shock Presentation Time'].dt.year > 2017,
                                                 "Septic Shock", "Excluded")
sep['Sepsis Type'] = np.where(((sep['Sepsis Type'] == "Excluded") & (sep['Severe Sepsis Presentation Time'].dt.year > 2017)),
                                                 "Severe Sepsis", sep['Sepsis Type'])
# FUNCTIONAL EXAM REASSESSMENT
sep['6Hr Reassessment Functional Exam Possible'] = np.where(((sep['Focused Exam Performed Score'].isnull() == False)|
                                                    (sep['B6_2 CVP Score'].isnull() == False)|
                                                    (sep['B6_3 Scv0 Achieved Score'].isnull() == False)|
                                                    (sep['Bedside Ultrasound Score'].isnull() == False)|
                                                    (sep['Passive Leg Raise Performed or Fluid Challenge Score'].isnull() == False)),1,0)

sep['6Hr Reassessment Functional Exam Success'] = np.where(((sep['Focused Exam Performed Score'] == 1)|
                                                    (sep['B6_2 CVP Score'] == 1)|
                                                    (sep['B6_3 Scv0 Achieved Score'] == 1)|
                                                    (sep['Bedside Ultrasound Score'] == 1)|
                                                    (sep['Passive Leg Raise Performed or Fluid Challenge Score'] == 1)),1,0)

sep['6Hr Reassessment Functional Exam Score'] = np.where(((sep['6Hr Reassessment Functional Exam Possible'] == 1) &
                                                    (sep['6Hr Reassessment Functional Exam Success'] == 1)),1,\
   np.where(((sep['6Hr Reassessment Functional Exam Possible'] == 1) &
                                                    (sep['6Hr Reassessment Functional Exam Success'] == 0)),0,np.nan)) 
          
#### MAKE FINAL DATAFRAME
sepf = sep[['REGISTRY_DATA_ID','Email Sent','Quarter','MRN','Patient Name',
           'Patient CSN','DOB','Age','Gender','Race','Admit',
           'Sepsis Type','Sepsis Presentation DTTM (Any)',
           'Floor when patient became septic','Destination/Escalation of Care Unit','Admit_date',
           'Critical CareAdmitDate','ICUAdmitTime','Severe Sepsis Presentation Date',
           'Severe Sepsis Presentation Time','Septic Shock Presentation Date',
           'Septic Shock Presentation Time','ICUDCDate','ICUDCTime','DCDate',
           'status','Discharge Disposition','Discharge to: Agency / Service Name',
           'Admit Length of Stay','ICU Length of Stay','Was RRT Called? ', 'RRT Narrator',
           'Record Remarks','Source of Infection','SIRS','Organ Dysfunction',
           'Lactate Done','Lactate','Lactate Actual Result','Lactate Date','Lactate Time','LactateET',
           'ElevatedLactate','RepeatLactate','RepeatLactateDate','RepeatLactateTime',
           'RepeatLactateET','Abx','Antibx Date','Antibx Time','Antibiotic #1','Antibiotic #2',
           'Culture Done','Culture Date','Culture Time','Blood Culture Delay','CultureET',
           'Fluid Resuscitation Required','CrystalloidDate','CrystalloidTime',
           'CrystalloidET','30 mL/kg Administered',
           'Persistent hypotension','CVP8Date','CVP8Time','CVP8TET',
           'ScvO2Date','ScvO2Time','ScvO2ET','Pressors','Vasopressor Date',
           'Vasopressor Time','Focused Exam Performed','Focused Exam Date','Focused Exam Time',
           'Bedside Ultrasound Date','Bedside Ultrasound Time','Passive Leg Raise Date',
           'Passive Leg Raise Time','Fluid Challenge Date','Fluid Challenge Time',
           '3_Hour_Eligible','B3_1 Lactate Time','B3_2 Culture Time','B3_3 Antibiotic Time',
           'Lactate >/=4','Fluid_Resuscitation_SBP_Under_90','Fluid_Resuscitation_MAP_Under_65',
           'Fluid_Resuscitation_SBP_decrease_40mmHg','B3_4 Crystalloid Time','B3_2 Culture before Anti',
           'B3_1 Lactate Score', 'B3_2 Blood Score', 'B3_3 Antibiotic Score',
           'B3_4 Crystalloid Score','B3_All','Mortality',
           'Organ_Dysfunction_SBP_Under_90','Organ_Dysfunction_MAP_Under_65',
           'VasoPressor Eligible','B6_1 Vaso Time','Hypo or Lactate Elig',
           'Focused Exam Performed Time','B6_2 CVP Achieved 6Hr Time',
           'B6_2 Scv0 Achieved 6Hr Time','Bedside Ultrasound Performed Time',
           'Passive Leg Raise Performed or Fluid Challenge Time',
           'Lactate Remeasure Eligible','B6_4 Lactate Remeasure',
           'B6_1 Vaso Score','B6_4 Lactate Remeasure Score','Focused Exam Performed Score',
           'B6_2 CVP Score','B6_3 Scv0 Achieved Score','Bedside Ultrasound Score',
           'Passive Leg Raise Performed or Fluid Challenge Score','B6_All',
           '3 Hour Eligible','3 Hour Success','6 Hour Eligible','6 Hour Success',
           'Provider when patient became septic (time 0)','Service when patient became septic (time 0)',
           'Did the patient have a surgical consult during this admission',
           'Sepsis Pathway utilized (within 7 hours of presentaiton)', 
           '6Hr Reassessment Functional Exam Possible','6Hr Reassessment Functional Exam Success',
           '6Hr Reassessment Functional Exam Score']]

sepf['DOB'] = sepf['DOB'].dt.strftime('%Y-%m-%d')
sepf = sepf.sort_values(by=['Admit_date'], ascending = True)
antibiotic_exp = sepf[['REGISTRY_DATA_ID','MRN','Patient Name',
           'Patient CSN','DOB','Age','Gender','Admit','Admit_date','DCDate',
           'Sepsis Type','Sepsis Presentation DTTM (Any)','Provider when patient became septic (time 0)',
           'Floor when patient became septic','Service when patient became septic (time 0)',
           'Was RRT Called? ','Sepsis Pathway utilized (within 7 hours of presentaiton)',
           'B3_3 Antibiotic Score','Abx','Antibx Date','Antibx Time','Antibiotic #1','Antibiotic #2']]
           
sepf.to_excel('//fs02/Medical-Informatics/justin/SepsisAbs/tableau_data_sources/full_data_source.xlsx', index=False)

antibiotic_exp.to_excel('//fs02/Medical-Informatics/justin/SepsisAbs/tableau_data_sources/antibiotics_data_source.xlsx', index=False)





