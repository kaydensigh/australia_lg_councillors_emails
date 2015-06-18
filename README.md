This pulls lists of Australian local government councillors from other scrapers and builds a database of emails.

Put the list of state databases in an environment variable, e.g.:
```
export MORPH_STATE_DATABASES=openaustralia/qld_lg_directory_councillors,openaustralia/nsw_lg_directory_councillors,kaydensigh/sa_lg_councillors
export MORPH_API_KEY=your-api-key
```
The API key lets this scraper download from other scrapers.

Internally it processes all sqlite rows as json objects. Rows are merged in from the state databases if they have the following fields:
   * `councillor` or `name`
   * `council_name` or `council`
   * `council_website` or `council_url`
   * optionally `email`

The councillor and council names are used to 'uniquely' identify a person. The council website is used to search for their email. Some state databases already contain emails.

It can handle basic error cases like empty fields, but ideally the input data should be as clean as possible.

It takes many runs to build up the database as it only searches for a limited number of councillors per run.
