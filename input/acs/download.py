#! /usr/bin/python
import pandas as pd
import numpy as np
from lead.model import util
from lead.output.aggregate import aggregate
import sys

def read_acs(table, columns, engine=None, offsets={0:{}}, years=[2009,2010,2011,2012,2013]):
    select = 'select geoid, {fields} from acs{year}_5yr.{table} where geoid ~ \'14000US1703\''
    column_names = ['geoid']
    column_names.extend(columns.keys())

    dfs = {}
    for year in years:
        for i, attrs in offsets.iteritems():
            offset = [c + i for c in columns.values()]
            cols = map( (lambda x: "{0}{1:03d}".format(table, x)), offset)
            s = select.format(fields=str.join(',', cols), year=year, table=table)
            df = pd.read_sql(s, engine)
            df.columns = column_names
            for attr in attrs:
                df[attr] = attrs[attr]
            df['year'] = year
            dfs[year] = df
    df = pd.concat(dfs.values())

    return df
        
# simple sum-aggregation of columns starting with prefix
def get_aggregate_columns(df, prefix):
    return { c: {'numerator':c} for c in df.columns if c.startswith(prefix)}

engine = util.create_engine()
index = ['geoid','year']

race_table='C02003'
race_columns = {
    'race_count_total': 1,
    'race_count_white': 3,
    'race_count_black': 4,
    'race_count_asian': 6
}
race_agg = read_acs(race_table, race_columns, engine)
race_agg.set_index(index, inplace=True)

hispanic_table = 'B03001'
hispanic_columns = {
    'hispanic_count_total': 1,
    'hispanic_count_hispanic': 3
}
hispanic_agg = read_acs(hispanic_table, hispanic_columns, engine)
hispanic_agg.set_index(index, inplace=True)

edu_table = 'B15001'
edu_columns = {
    'edu_count_total':3,
    'edu_count_9th': 4,
    'edu_count_12th': 5,
    'edu_count_hs': 6,
    'edu_count_some_college': 7,
    'edu_count_associates': 8,
    'edu_count_ba': 9,
    'edu_count_advanced': 10,
}
edu_offsets = {
    0: {'sex':'male', 'age':'18-24'},
    8: {'sex':'male', 'age':'25-34'},
    16: {'sex':'male', 'age':'34-44'},
    24: {'sex':'male', 'age':'45-64'},
    32: {'sex':'male', 'age':'65+'},

    41: {'sex':'female', 'age':'18-24'},
    49: {'sex':'female', 'age':'25-34'},
    57: {'sex':'female', 'age':'34-44'},
    65: {'sex':'female', 'age':'45-64'},
    73: {'sex':'female', 'age':'65+'},
}

edu = read_acs(edu_table, edu_columns, engine, edu_offsets)
edu_agg = aggregate(edu, get_aggregate_columns(edu, 'edu'), index=index)

# HEALTH INSURANCE
health_table='C27004'
health_columns={
    'health_count_total': 0,
}
health_offsets = {
    3: {'sex':'male', 'age':'<18'},
    6: {'sex':'male', 'age':'18-64'},
    9: {'sex':'male', 'age':'65+'},
    13: {'sex':'female', 'age':'<18'},
    16: {'sex':'female', 'age':'18-64'},
    19: {'sex':'female', 'age':'65+'},
}

years=[2012,2013]
health = read_acs(health_table, health_columns, engine, health_offsets, years)

insurances = ['employer', 'purchase', 'medicare', 'medicaid', 'military', 'veteran']
for i in range(len(insurances)):
    health_insurance_table = 'C2700' + str(4+i)
    health_insurance_columns={
        'health_count_insured_' + insurances[i]: 2,
    }

    insurance = read_acs(health_insurance_table, health_insurance_columns, engine, health_offsets, years)
    health = health.merge(insurance, on=['geoid', 'year', 'sex', 'age'])

health_agg = aggregate(health, get_aggregate_columns(health, 'health'), index=index)

# TENURE
tenure_table='B11012'
tenure_columns={
    'tenure_count_total': 0,
    'tenure_count_owner': 1,
    'tenure_count_renter': 2
}
tenure_offsets = {
    3: {'family_type': 'married'},
    7: {'family_type': 'male'},
    10: {'family_type': 'female'}
}

tenure = read_acs(tenure_table, tenure_columns, engine, tenure_offsets)
tenure_agg = aggregate(tenure, get_aggregate_columns(tenure, 'tenure'), index=index)

acs = tenure_agg.join((health_agg, edu_agg, race_agg, hispanic_agg))
acs.to_csv(sys.argv[1])