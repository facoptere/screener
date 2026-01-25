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
* [extrait.csv](extrait.csv): excerpt of the first list. Contains less than 50 undervaluated stocks of very profitable companies.

## `extrait.csv` columns meaning
| Column  | Description |
| ------------- | ------------- |
|	**qscore**	|	Score (0-100) as good fundamental ratios	|
|	**qscorePerf**	|	Score (0-100) as good fundamental ratios plus good momentum |
|	**EPSTRENDGR**	|	% Annual earnings per share growth, 5 year average, CAGR |  
|	**Focf2Rev_AAvg5**	|	% Annual free operational cash flow to gross revenue ratio, 5 year average |
|	**EnSolde2**	|	% Price undervaluation according to DCF FCFF. 20 -> stock in undervaluated by 20%	|
|	**DCF**	|	DCF Fair price	|
|	**L%H**	|	% Price inside the 365 day lowest and highest range	|
|	**PR13WKPCTR**	|	% Price difference from 3 months ago	|
|	**%M200D**	|	% Price relative to the 200 day moving average 	|
|	**ChPctPrice5Y**	|	% Annual stock price increase, 5 year average, CAGR	|
|	**Rendement**	|	% Annual dividend yield	|
|	**qMKTCAP.USD**	|	Worldwide percentile rank in capitalization (100 = most valued companies)	|
|	**Vol10D**	|	Daily volume of traded shares, 10 day average	|


Please note that the produced Python Dataframe contains 150 columns coming from financial statements, ratios, etc...

## Reports
Special reports are performed using Perplexity AI, based on companies listed on [extrait.csv]. Only companies with the strongest upside are kept here.

