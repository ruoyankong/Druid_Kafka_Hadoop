from pydruid.client import *
from pydruid.utils.aggregators import doublesum
from datetime import datetime
from pydruid.utils.filters import Dimension
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import time
from sklearn.feature_extraction.text import CountVectorizer

query = PyDruid("http://localhost:8082", 'druid/v2')
datasource = 'iphone_negative'

while (True):
    current_time = datetime.today().strftime('%Y-%m-%d')
    ts = query.select(
        datasource=datasource,
        granularity='minute',
        intervals=current_time+'/p1d',
        paging_spec ={'pagingIdentifies': {}, 'threshold': 1}
    )
    df = query.export_pandas()
    word_vectorizer = CountVectorizer(ngram_range=(1, 2), analyzer='word')
    sparse_matrix = word_vectorizer.fit_transform(df['text'])
    frequencies = sum(sparse_matrix).toarray()[0]
    res = pd.DataFrame(frequencies, index=word_vectorizer.get_feature_names(), columns=['frequency'])[:10].sort_values('frequency', ascending=False)[:4]#.sort_values('frequency', ascending=False)[:10]
    print(res)
    time.sleep(2)