This pulls lists of Australian local government councillors from other scrapers and builds a database of emails.

Put the list of state databases in an environment variable, e.g.:
```
export MORPH_STATE_DATABASES=openaustralia/qld_lg_directory_councillors,openaustralia/nsw_lg_directory_councillors,kaydensigh/sa_lg_councillors
export MORPH_API_KEY=your-api-key
```
The API key lets this scraper download from other scrapers.

It takes many runs to build up the database as it only searches for a limited number of councillors per run.
