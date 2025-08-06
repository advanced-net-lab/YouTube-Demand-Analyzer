# YouTube Regional Demand Collector

This Python script collects video demand data from YouTube using the YouTube Data API. It is designed to analyze interest in specific concepts across different countries and languages. The script supports scheduled execution, incremental updates, and Slack notifications for monitoring progress and errors.

## Features

- Collects view counts, like counts, and comment counts for each concept
- Supports multilingual queries to reduce language bias
- Retrieves data from multiple countries (based on ISO region codes)
- Tracks approximate API quota usage during execution
- Logs all execution details to a log file
- Sends notifications to Slack (start, progress, error, completion)

## Files

- `region_demand.py`: Main script for collecting YouTube demand data
- `concepts.txt`: List of concepts to analyze (one per line)
- `query_words.json`: Dictionary that maps each concept to a list of translated query words
- `regions code.txt`: List of target country codes (ISO 3166 format)
- `last_fetch.json`: Tracks the last fetch date for each concept
- `log.txt`: Execution logs and error messages

## Requirements

Install the required libraries using pip:

