from drain import data, step, model, util
from drain.util import dict_product

from itertools import product
import pandas as pd
import os

import lead.model.data
import lead.model.transform
import lead.model.cv
from lead.features import aggregations


def bll6_forest():
    """
    The basic temporal cross-validation workflow
    """
    return bll6_models(forest())

def bll6_forest_today():
    """
    The workflow used to construct a current model
    Parses the environment variable TODAY using pd.Timestamp to set the date
    """
    today = pd.Timestamp(os.environ['TODAY'])
    p = bll6_models(
            forest(),
            dict(year=today.year, 
                 month=today.month,
                 day=today.day))[0]
    # save the model
    p.get_input('fit').target = True

    # put the predictions into the database
    tosql = data.ToSQL(table_name='predictions', if_exists='replace', 
            inputs=[p], 
            inputs_mapping=[{'y':'df', 'feature_importances':None}, 'db'])
    tosql.target = True
    return tosql

def bll6_forest_quick():
    """
    A fast lead model that only uses 1 year of training data
    """
    today = pd.Timestamp(os.environ['TODAY'])
    p = bll6_models(
            forest(),
            dict(year=today.year, 
                 month=today.month,
                 day=today.day,
                 train_years=1))[0]
    # save the model
    p.get_input('cv').target = True
    p.get_input('fit').target = True
    return p


def bll6_forest_quarterly():
    """
    Quarterly forest models
    """
    return bll6_models(forest(), 
        {'month':[1,4,7,10], 'year':range(2010,2014+1)})

def bll6_forest_monthly():
    """
    Monthly forest models
    """
    return bll6_models(forest(), 
        {'month':range(1,13), 'year':range(2010,2014+1)})

def forest():
    """
    Returns a step constructing a scikit-learn RandomForestClassifier
    """
    return [step.Construct('sklearn.ensemble.RandomForestClassifier',
        n_estimators=2000,
        n_jobs=-1,
        criterion='entropy',
        class_weight='balanced_bootstrap',
        max_features='sqrt',
        random_state=0)]

def bll6_models(estimators, cv_search={}, transform_search={}):
    """
    Provides good defaults for transform_search to models()
    Args:
        estimators: list of estimators as accepted by models()
        transform_search: optional LeadTransform arguments to override the defaults

    """
    cvd = dict(
        year = range(2010, 2015+1),
        month = 1,
        day = 1,
        train_years = [6],
        train_query = [None],
    )
    cvd.update(cv_search)

    transformd = dict(
        wic_sample_weight = [0],
        aggregations = aggregations.args,
        outcome_expr = ['max_bll0 >= 6']
    )
    transformd.update(transform_search)
    return models(estimators, cvd, transformd)

def models(estimators, cv_search, transform_search):
    """
    Grid search prediction workflows. Used by bll6_models, test_models, and product_models.
    Args:
        estimators: collection of steps, each of which constructs an estimator
        cv_search: dictionary of arguments to LeadCrossValidate to search over
        transform_search: dictionary of arguments to LeadTransform to search over

    Returns: a list drain.model.Predict steps constructed by taking the product of
        the estimators with the the result of drain.util.dict_product on each of
        cv_search and transform_search.
        
        Each Predict step contains the following in its inputs graph:
            - lead.model.cv.LeadCrossValidate
            - lead.model.transform.LeadTransform
            - drain.model.Fit
    """
    steps = []
    for cv_args, transform_args, estimator in product(
            dict_product(cv_search), dict_product(transform_search), estimators):
   
        cv = lead.model.cv.LeadCrossValidate(**cv_args)
        cv.name = 'cv'

        transform = lead.model.transform.LeadTransform(inputs=[cv], **transform_args)
        transform.name = 'transform'

        fit = model.Fit(inputs=[estimator, transform], return_estimator=True)
        fit.name = 'fit'

        y = model.Predict(inputs=[fit, transform], 
                return_feature_importances=True)
        y.name = 'predict'
        y.target = True

        steps.append(y)

    return steps
