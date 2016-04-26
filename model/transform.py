from drain.step import Step
from drain.data import ToHDF
from drain import util, data
from drain.aggregation import SpacetimeAggregation

from lead.output.kids import revise_kid_addresses
from lead.model.data import LeadData

from sklearn import preprocessing
from datetime import date
import pandas as pd
import numpy as np
import logging
from repoze.lru import lru_cache

@lru_cache(maxsize=10)
def lead_data(month, day):
    return LeadData(month=month, day=day, year_min=2003, year_max=2016, target=True)

class LeadTransform(Step):
    EXCLUDE = ['address_id', 'building_id', 'complex_id', 
            'census_block_id', 'census_tract_id', 'ward_id', 
            'community_area_id']

    def __init__(self, month, day, year, outcome_expr, train_years, 
            aggregations,
            train_query=None,
            spacetime_normalize=False,
            wic_sample_weight=1, exclude=[], include=[], **kwargs):
        Step.__init__(self, month=month, day=day, year=year, 
                outcome_expr=outcome_expr,
                train_years=train_years,
                aggregations=aggregations,
                train_query=train_query,
                spacetime_normalize=spacetime_normalize,
                wic_sample_weight=wic_sample_weight, 
                exclude=exclude, include=include, **kwargs)

        year_min = 2003
        year_max = 2016
        if not year_min <= year <= year_max:
            raise ValueError('Invalid year: %s' % year)

        today = date(year, month, day)
        kid_addresses_revised = revise_kid_addresses(date=today)
        self.inputs = [lead_data(month, day), kid_addresses_revised]

    def run(self, revised, X, aux):
        today = util.timestamp(self.year, self.month, self.day)
        min_date = util.timestamp(self.year - self.train_years, self.month, self.day)

        for df in (X, aux):
            date = data.index_as_series(df, 'date')
            df.drop(df.index[(date < min_date) | (date > today)], inplace=True)

        logging.info('Splitting train and test sets')
        # add date column to index

        # don't include future addresses in training
        date = data.index_as_series(aux, 'date')
        train = (date < today) & (aux.address_min_date < today)
        test = date == today

        revised = revise_helper(revised=revised, aux=aux, 
                train=train, test=test, today=today)
        if self.train_query is not None:        
            train &= train.index.isin(
                    revised.query(self.train_query).index)
        # align kid_addresses_revised with the index of X and aux

        #aux = aux[(train | test)]
        aux.drop(aux.index[~(train | test)], inplace=True)
        X,train,test = data.train_test_subset(X, train, test, drop=True)

        logging.info('Selecting aggregations')
        aggregations = self.inputs[0].aggregations # dictionary of Aggregations
        for a, args in self.aggregations.iteritems():
            X = aggregations[a].select(X, args, inplace=True)

        logging.info('Binarizing')
        X = data.binarize(X, ['community_area_id', 'ward_id'], astype=np.float32)
        
        # TODO: include gender, ethnicity, etc.
        y = revised.loc[X.index].eval(self.outcome_expr)
        X = data.select_features(X, exclude=self.EXCLUDE + self.exclude, 
                include=self.include)

        if self.spacetime_normalize:
            prefixes = ['%s_.*' % a.prefix for a in 
                    self.inputs[0].aggregations 
                        if isinstance(a, SpacetimeAggregation)]
            spacetime = data.select_regexes(X.columns, prefixes)
            logging.info('Normalize %s columns' % len(spacetime))
            X.loc[:, spacetime] = X.loc[:,spacetime].groupby(
                    level='date').apply(
                        lambda x: pd.DataFrame(preprocessing.scale(x), 
                        index=x.index, columns=x.columns))

        logging.info('Imputing')
        X = data.impute(X, train=train)

        sample_weight = 1 + (revised.wic * self.wic_sample_weight)

        c = data.non_numeric_columns(X)
        if len(c) > 0:
            logging.warning('Non-numeric columns: %s' % c)

        return {'X': X, 'y': y, 
                'train': train, 'test': test, 
                'aux': aux, 'sample_weight': sample_weight}

def revise_helper(revised, aux, train, test, today):
    """
    given revised and unrevised kid_addresses (aux), merge the unrevised for the test set
    with the revised for training
    """
    revised = aux[[]][train].reset_index().merge(revised, how='left', 
            on=['kid_id', 'address_id'])
    revised.set_index(['kid_id', 'address_id', 'date'], inplace=True)
    revised = pd.concat((revised, aux[test]))

    revised['last_sample_age'] = (revised.last_sample_date - 
             revised.date_of_birth)/util.day
    revised['wic'] = revised.first_wic_date.notnull()
    revised['today_age'] = (today - revised.date_of_birth)/util.day
    revised['address_test_max_age'] = (revised.address_test_max_date - 
             revised.date_of_birth)/util.day

    date = data.index_as_series(revised, 'date')
    revised['age'] = (date - revised.date_of_birth)/util.day

    return revised
