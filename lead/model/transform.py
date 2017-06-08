from drain.step import Step
from drain import util, data

import pandas as pd
import numpy as np
import logging


class LeadTransform(Step):
    def __init__(self, outcome_expr, aggregations,
            wic_sample_weight=1, exclude=[], include=[]):
        Step.__init__(self, 
                outcome_expr=outcome_expr,
                aggregations=aggregations,
                wic_sample_weight=wic_sample_weight, 
                exclude=exclude, include=include)

    def run(self, X, aux, train, test):
        y = aux.eval(self.outcome_expr)

        logging.info('Selecting aggregations')
        aggregations = self.inputs[0].aggregations # dictionary of Aggregations
        for a, args in self.aggregations.iteritems():
            X = aggregations[a].select(X, args, inplace=True)

        logging.info('Selecting features')
        X = data.select_features(X, exclude=self.exclude, 
                include=self.include)

        sample_weight = 1 + (revised.wic * self.wic_sample_weight)

        c = data.non_numeric_columns(X)
        if len(c) > 0:
            logging.warning('Non-numeric columns: %s' % c)

        return {'X': X, 'y': y, 
                'train': train, 'test': test, 
                'aux': aux, 'sample_weight': sample_weight}
