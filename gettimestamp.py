import sys
import time
from datetime import datetime
timestr = sys.argv[1]
datetime_obj = datetime.strptime(timestr, "%Y-%m-%d %H:%M:%S.%f")
print int(time.mktime(datetime_obj.timetuple()) * 1000000.0 + datetime_obj.microsecond / 1000000.0)