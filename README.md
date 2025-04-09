# PVLiveConsumer
<!-- ALL-CONTRIBUTORS-BADGE:START - Do not remove or modify this section -->
[![All Contributors](https://img.shields.io/badge/all_contributors-4-orange.svg?style=flat-square)](#contributors-)
<!-- ALL-CONTRIBUTORS-BADGE:END -->
Collect GSP solar generation data from PVlive from Sheffield Solar

## Run

To the run the appication install this library and run
```
python pvliveconsumer/app.py
```

The environmental variables are
- DB_URL: The natabase url you want to save the results to
- REGIME: Regime of which to pull, either 'in-day' or 'day-after'
- N_GSPS: The number of gsps you want to pull
- INCLUDE_NATIONAL: Option to load national data, or not
- UK_LONDON_HOUR: Optional to check UK London hour. This means can run this service at the same
   UTC times, independently of the clock change.
- BACKFILL_HOURS: Optional, defaults to 2. The amount of hours of data that is backfilled.
- ELEVATION_LIMIT: Optional, defaults to 5. If no PVLive values are found, and sun elevation is below this, then the values are set to 0
- PVLIVE_DOMAIN_URL: Optional, defaults to 'https://www.pvlive.org.uk'. The domain of the PVLive API. 

These options can also be enter like this:
```
python pvliveconsumer/app.py --n-gsps=10
```

## Tests

To run tests use the following command
```bash
docker stop $(docker ps -a -q)
docker-compose -f test-docker-compose.yml build
docker-compose -f test-docker-compose.yml run pvliveconsumer
```

## Contributors ✨

Thanks goes to these wonderful people ([emoji key](https://allcontributors.org/docs/en/emoji-key)):

<!-- ALL-CONTRIBUTORS-LIST:START - Do not remove or modify this section -->
<!-- prettier-ignore-start -->
<!-- markdownlint-disable -->
<table>
  <tbody>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/rachel-labri-tipton"><img src="https://avatars.githubusercontent.com/u/86949265?v=4?s=100" width="100px;" alt="rachel tipton"/><br /><sub><b>rachel tipton</b></sub></a><br /><a href="https://github.com/openclimatefix/pvlive-consumer/commits?author=rachel-labri-tipton" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/peterdudfield"><img src="https://avatars.githubusercontent.com/u/34686298?v=4?s=100" width="100px;" alt="Peter Dudfield"/><br /><sub><b>Peter Dudfield</b></sub></a><br /><a href="https://github.com/openclimatefix/pvlive-consumer/commits?author=peterdudfield" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/mduffin95"><img src="https://avatars.githubusercontent.com/u/6598483?v=4?s=100" width="100px;" alt="Matthew Duffin"/><br /><sub><b>Matthew Duffin</b></sub></a><br /><a href="https://github.com/openclimatefix/pvlive-consumer/commits?author=mduffin95" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="http://www.wemofy.in"><img src="https://avatars.githubusercontent.com/u/120705327?v=4?s=100" width="100px;" alt="Akshay Waghmare"/><br /><sub><b>Akshay Waghmare</b></sub></a><br /><a href="https://github.com/openclimatefix/pvlive-consumer/commits?author=akshayw1" title="Code">💻</a></td>
    </tr>
  </tbody>
</table>

<!-- markdownlint-restore -->
<!-- prettier-ignore-end -->

<!-- ALL-CONTRIBUTORS-LIST:END -->

This project follows the [all-contributors](https://github.com/all-contributors/all-contributors) specification. Contributions of any kind welcome!
