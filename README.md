# Degiro Screener

## About
Build a CSV list of cherry picked stocks from the DEGIRO broker, based on the API [degiro-connector](https://github.com/Chavithra/degiro-connector).

## Howto
Please export these environment variables:
* `GT_DG_USERNAME`: DEGIRO login
* `GT_DG_PASSWORD`: DEGIRO password
* `GT_DG_TOKEN`: DEGIRO token seed
* `GT_DG_DIRECTORY`: local folder in which cached material will be stored

Launch `.degiro_screener.ipynb` jupyter notebook or execute `python screener.py`. 

For the later, 2 CSV files will be produced:
* `screener4.csv`: all assets from DEGIRO. Roughly 14000 companies are listed
* `extrait.csv`: excerpt of the first list. Contains undervaluated stocks of very profitable companies.

## `extrait.csv` columns meaning
| Column  | Description |
| ------------- | ------------- |
|	**qscore**	|	Score (0-100) as good fundamental ratios	|
|	**qscorePerf**	|	Score (0-100) as good fundamental ratios plus good momentum	|
|	**REVPS5YGR**	|	Annual sales increase percent, 5 year average, CAGR	|
|	**MARGIN5YR**	|	Net margin percent, 5 year average	|
|	**PER**	|	Price earning ratio	|
|	**Rendement**	|	Dividend yield in percent	|
|	**En Solde**	|	How undervalued the stock is. Example: 0.20 -> stock in undervaluated by 20%	|
|	**L%H**	|	Where is the stock price related to lowest (0) and highest (1) price in the last 365 days	|
|	**ChPctPrice5Y**	|	Annual stock price increase in percent, 5 year average, CAGR	|
|	**qMKTCAP.USD**	|	Worldwide percentile rank in capitalization (100 = most valued companies)	|
|	**Vol10D.USD**	|	Daily volume in USD, 10 day average	|


Please note that the produced Dataframe contains 150 columns coming from financial statements, ratios, etc...
