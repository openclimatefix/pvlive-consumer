# GSPConsumer
<!-- ALL-CONTRIBUTORS-BADGE:START - Do not remove or modify this section -->
[![All Contributors](https://img.shields.io/badge/all_contributors-2-orange.svg?style=flat-square)](#contributors-)
<!-- ALL-CONTRIBUTORS-BADGE:END -->
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
INCLUDE_NATIONAL: Option to load national data, or not
UK_LONDON_HOUR: Optional to check UK London hour. This means can run this service at the same
    UTC times, independently of the clock change.

These options can also be enter like this:
```
python gspconsumer/app.py --n-gsps=10
```

## Contributors ✨

Thanks goes to these wonderful people ([emoji key](https://allcontributors.org/docs/en/emoji-key)):

<!-- ALL-CONTRIBUTORS-LIST:START - Do not remove or modify this section -->
<!-- prettier-ignore-start -->
<!-- markdownlint-disable -->
<table>
  <tr>
    <td align="center"><a href="https://github.com/rachel-labri-tipton"><img src="https://avatars.githubusercontent.com/u/86949265?v=4?s=100" width="100px;" alt=""/><br /><sub><b>rachel tipton</b></sub></a><br /><a href="https://github.com/openclimatefix/GSPConsumer/commits?author=rachel-labri-tipton" title="Code">💻</a></td>
    <td align="center"><a href="https://github.com/peterdudfield"><img src="https://avatars.githubusercontent.com/u/34686298?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Peter Dudfield</b></sub></a><br /><a href="https://github.com/openclimatefix/GSPConsumer/commits?author=peterdudfield" title="Code">💻</a></td>
  </tr>
</table>

<!-- markdownlint-restore -->
<!-- prettier-ignore-end -->

<!-- ALL-CONTRIBUTORS-LIST:END -->

This project follows the [all-contributors](https://github.com/all-contributors/all-contributors) specification. Contributions of any kind welcome!