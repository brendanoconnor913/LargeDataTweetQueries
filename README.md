# LargeDataTweetQueries
The first query that was implemented used a trends file and spark sql to determine the average length of time a subject trends.
The second query averaged the number of followers for each tweet by the number of mentions used, the number of urls used, and the
number of hashtags used. The final query was a psychographic profile of users and aggregating the hashtags they used by their ascribed 
category. For this the two categories were whether or not the user had a background photo of a person or not. A CV API was used in conjustion
with a list of tags used by the api to indicate people. Once the user was categorized the hashtags were aggregated and sorted based on 
the respective categories.
