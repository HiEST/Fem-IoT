from pyspark import SQLContext
from pyspark import SparkContext
from shared.pipefunctions import Meanizer

__author__ = 'agutierrez'


### THIS FILE IS OBSOLETE ###

class Meanizer(sklearn.base.BaseEstimator, sklearn.base.RegressorMixin):
    ## X and y should be pandas dfs
    # TODO: assert that X and y are pandas
    def fit(self, X, y, default_value=np.nan):
        if X.shape[1] == 0: # Normal mean
            # Remove duplicates to do the mean?
            self.mean = pd.DataFrame([y.mean()])
            self.count = len(y)
        else:
            self.default = default_value
            keys = np.apply_along_axis(str, 1, X) # Transform data to keys

            y = y.assign(key=keys)
            df = y.groupby('key').agg('mean')
            self.mean = df.to_dict(orient='index')
            df = y[['key', df.columns[0]]].groupby('key').agg('count')
            self.count = dict(zip(df.index.values, df[df.columns[0]]))
            df = y.groupby('key').agg(np.std)
            self.std = df.to_dict(orient='index')

        return self

    def predict(self, X, y=None):
        if isinstance(self.mean, dict): # Grouped average
            keys = np.apply_along_axis(str, 1, X) # Transform data to keys
            y = [self.mean.get(k, self.default) for k in keys]
        else:
            y = self.mean.loc[np.repeat(0, X.shape[0])] # Repeat mean (index 0) ncol times
        y = pd.DataFrame(y)
        return(y)

def analyze(sc: SparkContext, input_data = 'ships_data.parquet',
        input_metadata = 'ships_metadata.parquet',
        output_file = 'ships_metadata_imputed.parquet',
        type_var = 'typeofship',
        id_var = 'imo',
        ihs_id_var = 'LRIMOShipNO', # TODO: This should be standardized on a previous step...
        target_vars = ['installedPowerME']):

    sqlContext = SQLContext(sc)

    # ## Initial dataset creation
    # - Create AIS['imo', 'typeofshipandcargo'] dataset
    # - Create IHS dataset with AIS typeofshipandcargo
    ais=sqlContext.read.parquet(input_data).select(id_var,'typeofshipandcargo') \
        .dropDuplicates()
    # TODO: This should be in another step!
    ais = ais.withColumn(type_var, floor(ais.typeofshipandcargo/10).cast('int'))

    # Right join so we retain all AIS entries even if they don't have IHS entry.
    ihs = sqlContext.read.parquet(input_metadata)
    ais = ais.join(ihs, ais.imo == ihs.LRIMOShipNO, 'left')

    # ## Split valid and non-valid

    # Remove ships that don't have any data and those that don't have a valid type 
    # (https://help.marinetraffic.com/hc/en-us/articles/205579997-What-is-the-significance-of-the-AIS-SHIPTYPE-number-)

    # FIXME: There are some ships that have IHS entry but are not reporting a correct AIS type!

    # Ships with correct type, i.e. usable by the process
    ais_valid = ais.dropDuplicates(subset=[id_var])\
        .filter((ais[type_var] >= 1) & (ais[type_var] < 10))

    # ## Split training/test
    train = ais_valid.filter(ais_valid[target_var].isNotNull()).toPandas()
    test = ais_valid.filter(ais_valid[target_var].isNull()).toPandas()


    # ## Training
    # Create mean with existing ships using typeofship
    m = Meanizer()
    X = train[[type_var]]
    y = train[[target_var]]
    # This value will be the default value (if no correct type is found, this is set)
    global_mean = y.mean()
    m.fit(X, y, default_value=global_mean)

    X_test = test[[type_var]]
    pred = m.predict(X_test)

    # TODO: Merge with IHS and return.

    # TODO: Add "isEstimated column"
