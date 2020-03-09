import matplotlib.pyplot as plt
import numpy as np
import time
from pydruid.client import *
from pydruid.utils.aggregators import doublesum

fig = plt.figure()
ax = fig.add_subplot(111)

# some X and Y data
datasource = 'iphone_positive'
x = None
y = None
li = None


# draw and show it
ax.relim()
ax.autoscale_view(True,True,True)
fig.canvas.draw()
plt.show(block=False)

# loop to update the data
while True:
    try:
        query = PyDruid("http://localhost:8082", 'druid/v2')
        ts = query.timeseries(
            datasource=datasource,
            granularity='minute',
            intervals='2019-10-29/p4w',
            aggregations={'count': doublesum('count')},
        )
        df = query.export_pandas()
        x = df['timestamp'].map(lambda x: x[8:16])

        y = df['count']

        # set the new data
        if not li:
            li, = ax.plot(x, y)
        else:
            li.set_ydata(y)

        fig.canvas.draw()

        time.sleep(0.01)
    except KeyboardInterrupt:
        break