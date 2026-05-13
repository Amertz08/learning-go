# Rate limiters

Implemented a few different rate limiter algos.

## Use cases

You'd want a rate limiter anywhere you'd want to limit the rate of ingestion :). The most common would be an API. However,
I think they could come in handy on data ingestion pipelines. For example, if you have a process that downloads a file/makes
API calls to a third party, processes that data, and finally submits that data to an internal database/API, where each stage
communicates over SQS or RabbitMQ, you might want to have a rate limiter on the download stage to match with the 3rd party
API limits as well as on the database insertion/posting stage to not overwhelm the database or the downstream API.

These are all inmemory implementations and thus would only limit per process. You'd likely want to have something reach out
to a cache like redis to store the actual state.
