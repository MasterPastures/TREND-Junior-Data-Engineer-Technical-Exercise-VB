# TREND-Junior-Data-Engineer-Technical-Exercise-VB

This repository is organized in the following manner:

```
TREND-Junior-Data-Engineer-Technical-Exercise-VB/
|--- .sql_files/
|--- scripts/
|--- visualizations/
```

Here is what is contained in each subdirectory:
* `.sql_files`: Contains `schema.sql`, where the sqlite table is generated from
* `scripts`: Contains `analysis.py`, where the data will be downloaded and analyzed
* `visualizations`: Contains ER diagram as well as text files and a plot to answer the questions in `analysis.py`

I ended up choosing the [311-Service-Requests-from-2010-to-Present](https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9/about_data) dataset to download and analyze. I have selected a subset
of the columns available both for database normalization and relevance to any business questions I believe I could answer.

Issues:
* Socrata API Foundry's directed Python/Pandas code excerpt [here](https://dev.socrata.com/foundry/data.cityofnewyork.us/erm2-nwe9) times out my requests almost instantly, so my download script does not use an API key
* I have run into what I believe are harsh API rate limits with my approach, even when setting a limit of 500 rows to read from the larger dataset, making each run take longer and increasing the difficulty of debugging

## How to run the pipeline:
* Use this command: `python analysis.py -cli_limit <cli_limit> -cli_chunk <cli_chunk>`
    * Replace `<cli_limit>` with the number of rows you want to query from the NYC 311 dataset
    * Replace `<cli_chunk>` with the size of each chunk you want to process
    * Example usage: `python analysis.py -cli_limit 50 -cli_chunk 10` (runs the pipeline on 50 rows from the dataset in sizes of 10)