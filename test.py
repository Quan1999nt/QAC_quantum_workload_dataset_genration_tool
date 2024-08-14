from datetime import datetime, timedelta


# Using current time
ini_time_for_now = datetime.now()

# printing initial_date
print ('initial_date:', str(ini_time_for_now))

# Calculating past dates
# for two years
past_date_before_2yrs = timedelta(seconds = 10)
base_time = datetime.now()
# for two hours
base_time_after_10_seconds= base_time+timedelta(seconds = 10)

print(base_time.timestamp())
print(base_time_after_10_seconds.timestamp())
# printing calculated past_dates
