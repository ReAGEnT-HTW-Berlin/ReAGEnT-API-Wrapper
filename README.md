# ReAGEnT-API-Wrapper

The API Wrapper is responsible for collecting tweets from the Twitter API v2 endpoint using Spark Structured Streaming. The data is sorted, analyzed and saved to Mongo DB according to rules set in main class.

## Start
The application is run through sbt. Following environment variables must be set:

| Variable | Description |
|:---:|:---:|
| TWITTER_BEARER | Your Twitter API v2 Bearer Token |
| dbURL | URL where your Mongo DB resides, for local startup set it to localhost |
| dbName | Name of the database that handels data and authorization |
| user | Mongo DB username |
| pwd | Mongo DB password |

For local startup navigate to the project directory and execute:

### 'sbt run'

ATTENTION! Set Twitter API v2 rules before starting wrapper!


## Extending the wrapper

To extend the application, open the main class 'ReAGEnT_API_Wrapper'. 
You can create worker threads analogous to the nine existing workers. Choose your DataFrame operations and Mongo DB writers.

There are three Mongo DB writers:

| Writer | Description |
|:---:|:---:|
| ForEach| writes tweet as is |
| Update| updates a mongo db entry according to its id |
| Upsert| updates a mongo db entry, provided the id exists - not existing ids are created |

Lastly, create a parser analogous to the ones in the 'parser' folder. There you can configure the Mongo DB id and content you want to save.

Twitter API v2 rules can be edited in 'twitterRules.json'.