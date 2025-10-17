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
* [screener4.csv](screener4.csv): all assets from DEGIRO. Roughly 14000 companies are listed (file size 16MB)
* [extrait.csv](extrait.csv): excerpt of the first list. Contains 40 undervaluated stocks of very profitable companies.

## `extrait.csv` columns meaning
| Column  | Description |
| ------------- | ------------- |
|	**qscore**	|	Score (0-100) as good fundamental ratios	|
|	**qscorePerf**	|	Score (0-100) as good fundamental ratios plus good momentum |
|	**REVPS5YGR**	|	% Annual sales growth, 5 year average, CAGR |
|	**EPSTRENDGR**	|	% Annual earnings per share growth, 5 year average, CAGR |  
|	**MARGIN5YR**	|	% Net margin, 5 year average	|
|	**PER**	|	Price to earnings ratio	|
|	**En Solde**	|	% Price undervaluation. 20 -> stock in undervaluated by 20%	|
|	**L%H**	|	% Price inside the 365 day lowest and highest range	|
|	**%M200D**	|	% Price relative to 200 day moving average 	|
|	**ChPctPrice5Y**	|	% Annual stock price increase, 5 year average, CAGR	|
|	**Rendement**	|	% Annual dividend yield	|
|	**qMKTCAP.USD**	|	Worldwide percentile rank in capitalization (100 = most valued companies)	|
|	**Vol10D.USD**	|	Daily volume of traded shares, 10 day average	|


Please note that the produced Dataframe contains 150 columns coming from financial statements, ratios, etc...
