# An Empirical Investigation of Personalization Factors on TikTok

In this repository we publish all software resources that were utilized to perform a sock-puppet audit on the web-version of TikTok to mimic a human user. With this audit we focused on analyzing the personalozation factors and their influence on the recommendation algorithm of TikTok. You may find our work here: **LINK**

Within this ReadMe we will provide a short overview on how one can use our code to replicate our results. Please note that eventhough we are confident that our results are trustworthy you may encounter different ones due to different time periods and the continous development of the recommendation algorithm by TikTok.

## Running A Test Scenario

In the following section we will provide a step by step guid how to intialise and run one of the test scenarios we performed in our paper.

First of all, you need to setup the appropriate infrastructure:
1. Create a Webshare account to obtain IP addresses from proxies you can use.
2. Create the test users in the database.
3. Since every test scenario consists of two test users you have to manually create those two users using the previously stored data on TikTok.
4. Once the user accounts exist on TikTok you may initialize the test scenario by executing the ParallelTesting.py file with the corresponding parameters.


We exemplify these steps performing a run of the test scenario 28. This scneario aims on ... consisting of the users ...

- Creating User Accounts
-- Get phone numbers
-- Create user accounts using purchased phone numbers

- explain structure of db_credentials.json
- explain placeholders: Twilio, Heroku DB, Webshare Proxz API, paths within project

## Analyzing Generated Data

In order to obtain the most promising results of the Skip-Gram model we trained the model over 5 epochs with a learning rate of 0.1.
