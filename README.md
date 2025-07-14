# TREND-Junior-Data-Engineer-Technical-Exercise-VB

This repository is organized in the following manner:

```
TREND-Junior-Data-Engineer-Technical-Exercise-VB/
|--- .sql_files/
|--- scripts/
|--- visualizations/
```

Here is what is contained in each subdirectory:
* `.sql_files`: Contains `schema.sql` and the generated SQLite file
* `scripts`: Contains `analysis.py`, where the data will be downloaded and analyzed
* `visualizations`: Contains ER diagram and plots to answer the questions presented in `analysis.py`

I ended up choosing the [311-Service-Requests-from-2010-to-Present](https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9/about_data) dataset to download and analyze. I have selected a subset
of the columns available both for database normalization and relevance to any business questions I believe I could answer.

Issues:
* Socrata API Foundry's directed Python/Pandas code excerpt [here](https://dev.socrata.com/foundry/data.cityofnewyork.us/erm2-nwe9) times out my requests almost instantly, so my download script does not use an API key
* I have run into what I believe are harsh API rate limits with my approach, even when setting a limit of 500 rows to read from the larger dataset, making each run take longer and increasing the difficulty of debugging