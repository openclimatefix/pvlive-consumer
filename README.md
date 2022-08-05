# GSPConsumer
Collect GSP solar generation data from PVlive from Sheffield Solar

## Run

To the run the appication install this library and run
```
python gspconsumer/app.py
```

The environmental variables are
DB_URL: The natabase url you want to save the results to
REGIME: Regime of which to pull, either 'in-day' or 'day-after'
N_GSPS: The number of gsps you want to pull
INCLUDE_NATIONAL: Option to loal national data, or not

These options can also be enter like this
```
python gspconsumer/app.py --n-gsps=10
```
