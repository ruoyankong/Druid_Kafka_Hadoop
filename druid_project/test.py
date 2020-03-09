from pydruid.client import *
from pydruid.utils.aggregators import doublesum
import matplotlib.pyplot as plt
import numpy as np
query = PyDruid("http://localhost:8082", 'druid/v2')
datasource = 'iphone_positive'

plt.style.use('ggplot')


def live_plotter(x_vec, y1_data, line1, identifier='', pause_time=2):
    if line1 == []:
        # this is the call to matplotlib that allows dynamic plotting
        plt.ion()
        fig = plt.figure(figsize=(13, 6))
        ax = fig.add_subplot(111)
        # create a variable for the line so we can later update it
        line1, = ax.plot(x_vec, y1_data, '-o', alpha=0.8)
        # update plot label/title
        plt.ylabel('count')
        plt.xlabel('time')
        plt.title('Title: {}'.format(identifier))
        plt.show()

    # after the figure, axis, and line are created, we only need to update the y-data
    line1.set_ydata(y1_data)
    # adjust limits if new data goes beyond bounds
    if np.min(y1_data) <= line1.axes.get_ylim()[0] or np.max(y1_data) >= line1.axes.get_ylim()[1]:
        plt.ylim([np.min(y1_data) - np.std(y1_data), np.max(y1_data) + np.std(y1_data)])
    # this pauses the data so the figure/axis can catch up - the amount of pause can be altered above
    plt.pause(pause_time)

    # return line so we can update it again in the next iteration
    return line1
line = []
while (True):
    ts = query.timeseries(
        datasource=datasource,
        granularity='minute',
        intervals='2019-10-29/p4w',
        aggregations={'count': doublesum('count')},
    )
    df = query.export_pandas()
    df['time'] = df['timestamp'].map(lambda x: x[8:16])
    line = live_plotter(df['time'], df['count'], line, datasource)
    # df.plot(x='time', y='count')#, ylim=(80, 140), rot=20,
    #         #title='Sochi 2014')
    # plt.ylabel('# '+ datasource)
    # plt.show()
    # plt.pause(0.5)

# top = query.topn(
#     datasource='iphone_positive',
#     granularity='minute',
#     intervals='2019-10-29/p1d',  # utc time of 2014 oscars
#     dimension='user_mention_name',
#     aggregations={'count': doublesum('count')},
#     metric='count',
#     threshold=10
# )
#
# df = query.export_pandas()
# print(df)