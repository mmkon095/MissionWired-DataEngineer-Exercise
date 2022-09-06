# %% [markdown]
# MissionWired Data Engineer Exercise
# 
# Question 1.
# Produce a “people” file with the following schema. Save it as a CSV with a header line to the working directory

# %%
#Import the necessary packages
import pandas as pd
import numpy as np

# %%
#Read the 3 csv files from Amazon S3 bucket and stores as variables
information  = pd.read_csv('https://als-hiring.s3.amazonaws.com/fake_data/2020-07-01_17%3A11%3A00/cons.csv')

# %%
emails = pd.read_csv('https://als-hiring.s3.amazonaws.com/fake_data/2020-07-01_17%3A11%3A00/cons_email.csv')

# %%
status = pd.read_csv('https://als-hiring.s3.amazonaws.com/fake_data/2020-07-01_17%3A11%3A00/cons_email_chapter_subscription.csv')

# %%
#Lets get a quick summary and look for common column values between the datasets
information.info()

# %%
information.head(3)

# %%
emails.info()

# %%
emails.head(3)

# %%
status.info()

# %%
status.head(3)

# %%
#First stage of merge:  Information and emails have "cons_id" column in common so we can merge on that and store the merged dataframe in a variable
df = pd.merge(information, emails, how="left", on="cons_id")

# %%

df.head(3)

# %%
#Second stage of merge: Use df dataframe from previous and merge with status dataframe on a common column 'cons_email_id' and store that in a variable
df = pd.merge(df, status, how="left", on='cons_email_id' )

# %%

df.head(3)

# %%
#Now we can trim the dataset to match our criteria only records with chapter_id = 1 and store that as new_df
new_df = df[(df.chapter_id == 1)]

# %%
#This is what our relevant dataset looks like
new_df.head(3)

# %%
#Lets look at the dataset and locate the columns and their data types that we will use for our people file
new_df.info()

# %%
#Create a new dataframe with regards to the 'people' schema
people = pd.DataFrame().assign(email=new_df['email'], code=new_df['source'], is_unsub=new_df['isunsub'], created_dt=new_df['create_dt_x'], updated_dt=new_df['modified_dt_x'])

# %%
#This is what the people dataframe looks like
people

# %%
#Change the data types to match the schema
people.email = people.email.astype('string')

# %%
people.code = people.code.astype('string')

# %%
people.is_unsub = people.is_unsub.astype('bool')

# %%
people.created_dt = pd.to_datetime(people.created_dt)

# %%
people.updated_dt = pd.to_datetime(people.updated_dt)

# %%
#Confirming out data types are now correct
people.info()

# %%
#Final review of people dataframe to make sure it fits the request
people

# %%
#Save the people dataframe to a csv
people.to_csv("people.csv", index=False)

# %% [markdown]
# 

# %% [markdown]
# Question 2:
# 
# Use the output of #1 to produce an “acquisition_facts” file with the following schema that aggregates stats about when people in the dataset were acquired. Save it to the working directory.

# %%
#Create a copy of the people dataframe and store it in a new variable
acquisition_facts = people.copy()

# %%
#Create a new date only column based off of the acquisition_date column
acquisition_facts['acquisition_date'] = acquisition_facts['created_dt'].dt.date

# %%
#Group the dataframe by the new date column and use the email column to count the number of constituents for each day
acquisition_facts = acquisition_facts.groupby('acquisition_date')['email'].count().reset_index()


# %%
#Rename the email column to acquisitions to match our acquisitions_facts schema
acquisition_facts = acquisition_facts.rename(columns = {'email':'acquisitions'})

# %%
#Final check
acquisition_facts.info()

# %%
#Convert acquisition_date column to date type

acquisition_facts['acquisition_date'] = pd.to_datetime(acquisition_facts['acquisition_date'])

# %%
#Check again
acquisition_facts.info()

# %%
#Save the acquisition_facts dataframe to a csv
acquisition_facts.to_csv("acquisition_facts.csv", index=False)

# %%



