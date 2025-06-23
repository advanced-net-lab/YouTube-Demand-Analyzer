# YouTube-Demand-Analyzer
This repository contains a Python script that uses the YouTube Data API v3 to collect regional demand data across multiple countries and search queries.

## Purpose

This tool is used for academic research to analyze cross-cultural demand patterns of online video content. The results are used to create demand vectors for each region, useful in content recommendation and localization studies.

## Features

- Multi-region support (via `regionCode`)
- Multi-query search
- Outputs a CSV file with view/like/comment counts
- API quota-conscious implementation

## Usage

1. Replace `YOUR_API_KEY_HERE` with your own YouTube Data API key
2. Run `youtube_demand_collector.py` with Python 3.x
3. Output file: `youtube_multiquery_demand_data.csv`




