from drain.step import Step, Call
from drain import util, data
from drain.data import FromSQL, Merge
from drain.aggregation import SpacetimeAggregationJoin

from lead.features import aggregations
from lead.features.acs import ACS
from lead.model.left import LeadLeft

from datetime import date
import pandas as pd
import numpy as np
import logging

class LeadData(Step):
    def __init__(self, month, day, year_min, year_max, wic_lag=None, dtype=None):
        if dtype is None:
            dtype = np.float16

        Step.__init__(self, month=month, day=day, 
                year_min=year_min, year_max=year_max,
                wic_lag=wic_lag, dtype=dtype)

        left = LeadLeft(month=month, day=day, year_min=year_min)
        left.target = True

        acs = Call("astype", inputs=[ACS(inputs=[left])], 
                   dtype=dtype)
        acs.target = True

        dates = tuple((date(y, month, day) for y in range(year_min, year_max+1)))
        self.aggregations = aggregations.all_dict(dates, wic_lag)

        self.aggregation_joins = []
        for name, a in self.aggregations.items():
            aj = SpacetimeAggregationJoin(
                    inputs=[left, a], 
                    lag=wic_lag if name.startswith('wic') else None,
                    inputs_mapping=[{'aux':None}, 'aggregation'])
            aj = Call("astype", inputs=[aj], dtype=dtype)
            aj.target = True
            self.aggregation_joins.append(aj)

        self.inputs = [acs, left] + self.aggregation_joins
        self.inputs_mapping=['acs', {}] + [None]*len(self.aggregations)

    def run(self, acs, left, aux):
        # join all aggregations
        logging.info('Joining aggregations')

        index_columns = ['kid_id','address_id','date']
        left_columns = ['ward_id', 'community_area_id', 'address_lat', 'address_lng']

        left = left[index_columns + left_columns]

        X = left.join([a.get_result() for a in self.aggregation_joins] + [acs])
        # delete all aggregation inputs so that memory can be freed
        for a in self.aggregation_joins: del a._result

        logging.info('Dates')
        X['age'] = ((aux.date - aux.date_of_birth)/util.day).astype(self.dtype)
        X['date_of_birth_days'] = util.date_to_days(aux.date_of_birth).astype(self.dtype)
        X['date_of_birth_month'] = aux.date_of_birth.dt.month.astype(self.dtype)
        X['male'] = (aux.sex == 'M').astype(self.dtype)
        X['wic'] = (aux.first_wic_date < aux.date).fillna(False).astype(self.dtype)

        X.set_index(index_columns, inplace=True)
        aux.set_index(index_columns, inplace=True)

        c = data.non_numeric_columns(X)
        if len(c) > 0:
            logging.warning('Non-numeric columns: %s' % c)

        return {'X':X, 'aux':aux}
