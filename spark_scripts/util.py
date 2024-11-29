"""
created 26-11-2024
project automatic-tvp
author rohitphadtare 
"""
import os, sys
from calendar import monthcalendar
sys.path.append("..")


##
# UDF to get week number of month for given date
# #
def get_week_of_month(year_, month_, day_):
    return next(
        (week_number for week_number, days_of_week in enumerate(monthcalendar(year_, month_), start=1) if
         day_ in days_of_week),
        None,
    )