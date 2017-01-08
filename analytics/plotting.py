import matplotlib
from matplotlib import pyplot
import os.path

import config

if config.pyplot_use_agg:
    matplotlib.use('Agg')    

g_has_plot = False
g_colors = ['b', 'g', 'r', 'c', 'm', 'y', 'k']
g_styles = ['-', '_', '--', ':']

class Plotter(object):
    def __init__(self, title=None, name=None):
        global g_has_plot
        assert not g_has_plot, "only one plot at a time, gbop"
        g_has_plot = True
        self.name = name
        if name is not title:
            pyplot.title(title)
        elif name is not None:
            pyplot.title(name)
        self.color_ix = 0
        self.style_ix = 0

    def Plot(self, rows, color=None, style=None, label=None, x="x", y="y"):
        global g_colors
        global g_styles
        if style is None:
            if color is None and self.color_ix > len(g_colors):
                self.style_ix += 1
                self.color_ix = 0
            style = g_styles[self.style_ix]        
        if color is None:
            color = g_colors[self.color_ix]
            self.color_ix += 1
        xs = [float(r[x]) for r in rows]
        ys = [float(r[y]) for r in rows]
        pyplot.plot(xs, ys, color+style, label=label)
        return self

    def VertLine(self, x, color='k', style='--', label=None):
        pyplot.axvline(x=x, color=color, linestyle=style, label=label)
        return self

    def Done(self):
        global g_has_plot
        assert g_has_plot, "uhh, idk"
        g_has_plot = False
        lgd = pyplot.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)        
        if self.name is None:
            pyplot.show(bbox_extra_artists=(lgd,), bbox_inches='tight')
        else:
            pyplot.savefig(os.path.join("plots", self.name + ".png"), bbox_extra_artists=(lgd,), bbox_inches='tight')
        pyplot.gcf().clear()

    def XLabel(self, label):
        pyplot.xlabel(label)
        return self

    def YLabel(self, label):
        pyplot.ylabel(label)
        return self

class WeekPlotter(Plotter):
    def __init__(self, title=None, name=None):
        super(WeekPlotter, self).__init__(title=title, name=name)
        for i, w in enumerate(["sun","mon","tues","weds","thurs","fri","sat"]):
            self.VertLine(i * 24)
        self.XLabel("hour of week")
    
