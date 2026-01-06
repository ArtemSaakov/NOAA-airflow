NOAA Airflow Pipeline
====================================

Whenever I want to take on a project for the sake of learning a new tool or piece of software,
I usually look to the weather for all of my data-related needs, as it tends to be public (free). In this case, I wanted to familiarize
myself with [Apache Airflow](https://airflow.apache.org/), a tool for managing data workflows. So I made a workflow that fetches
current and historical weather data for a given day and allows for easy anomaly detection by computing the historical 10th-90th
percentile range of temperatures for that day. If a day's temperature falls outside of that range, it can be considered "unusual."

APIs from the National Oceanic and Atmospheric Administration (NOAA) are utilized to fetch real-time and historical weather data.
Various statistics are pulled, compared, and analyzed.
