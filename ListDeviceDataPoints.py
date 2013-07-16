import Globals
from Products.ZenUtils.ZCmdBase import ZCmdBase
dmd = ZCmdBase().dmd


counts = {}

for poll in dmd.Monitors.Performance.objectValues("PerformanceConf"):
    counts[poll.id] = {}

    for d in poll.devices():

        d = d.primaryAq()

        dc = d.deviceClass().primaryAq().getPrimaryId()[10:]

        if not counts[poll.id].has_key(dc):

          counts[poll.id][dc] = {'devices': 0, 'datapoints': 0}

        comps = d.getMonitoredComponents()

        datapoints = sum([comp.getRRDDataPoints() for comp in comps], []) + d.getRRDDataPoints()

        counts[poll.id][dc]['devices'] += 1

        counts[poll.id][dc]['datapoints'] += len(datapoints)

print(counts)
