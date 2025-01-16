# Polarization
This is the repository for code and data of the paper "Polarized Patterns of Language Toxicity and Sentiment of Debunking Posts on Social Media".

## Reddit and Twitter:
### data：
It only includes Twitter IDs, excluding any intermediate processed data.

`UserDataset+[platform].csv`
- The IDs of the two platforms
### code

- `Reddit` 
  - `Reddit_POTUS2016`
    - `1_filter[topic=POTUS2016].ipynb`
    - `2_social_networks[topic=POTUS2016].ipynb`
    - `3_time_slices[topic=POTUS2016] (1).ipynb`
  - - ... Documents similar to the above.
- `Twitter`
  - `Twitter_2016`
    - `1_filter_potus_election2016.ipynb`
    - `2_retweet_network[topic=POTUS2016].ipynb`
  - ... Documents similar to the above.
- `replydata.ipynb` 
  - the code for figure 9
- `sentiment_2platforms.ipynb` 
  - the code for figure 2
- `entropy_time_process.ipynb` 
  - the code for figure 4, 6, 7, 8
