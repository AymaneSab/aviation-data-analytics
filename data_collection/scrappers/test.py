import sys 

sys.path.append("/home/hadoop/aviation-data-analytics/opensky-api/python")

from opensky_api import OpenSkyApi

api = OpenSkyApi()
states = api.get_states()
for s in states.states:
    print("(%r, %r, %r, %r)" % (s.longitude, s.latitude, s.baro_altitude, s.velocity))