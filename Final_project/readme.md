
# Description

This project pretend to collect and analyse the sentiments related to the actual pandemic "COVID19". For this purpose, it was decided to use twitter as the main source of information since it is more open compared to other social networks.

So, the first step was to collect and clean the raw data from twitter, related to tens of hashtags (taking into account the limitations such as the standard 7 days search).

But, this project revealed the painful necessity of looking for labelled data, and then teach an algorithm in supervised manner (as most the time we intended to do). So, It was decided to try a different approach: to use unsupervised learning along with transfer learning methodology.

The two implemented algorithms were: Word2Vec and Kmeans clustering.

The following main files are:
- DSF_FINAL_PROJECT.ipynb: Main Twitter collector notebook

- DSF_FINAL_PROJECT_Helper.ipynb: Twitter and db classes

- DSF_FINAL_PROJECT_Helper_General.ipynb: All purposes classes

- DSF_FINAL_PROJECT_Plot.ipynb: Plotting helper classes

- DSF_FINAL_PROJECT_Prep_Embed.ipynb: Preparation and Word2Vec notebook

- DSF_FINAL_PROJECT_kmeans_clustering.ipynb: Kmean notebook

- DSF_FINAL_PROJECT_Test_Prediction.ipynb: Prediction notebook

- TwitterProcess.py: Twitter collector (background process)

- social_network.db: It Stores all the main process generated information. 

Note: It's a working in progress project

## Installation


```bash

```

## Usage

```
```

## Contributing


## License
[MIT](https://choosealicense.com/licenses/mit/)
