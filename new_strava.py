import pandas as pd
import json
import time
import requests
import urllib3
from pandas import json_normalize
import csv
import boto3


# Get the tokens from file to connect to Strava
with open('strava_tokens.json') as json_file:
    strava_tokens = json.load(json_file)
# If access_token has expired then 
# use the refresh_token to get the new access_token
if strava_tokens['expires_at'] < time.time():
# Make Strava auth API call with current refresh token
    response = requests.post(
                        url = 'https://www.strava.com/oauth/token',
                        data = {
                                'client_id': "ENTER YOUR CLIENT ID HERE",
                                'client_secret': 'ENTER YOUR CLIENT SECRET KEY HERE',
                                'grant_type': 'refresh_token',
                                'refresh_token': strava_tokens['refresh_token']
                                }
                    )
    
# Save response as json in new variable
    new_strava_tokens = response.json()
# Save new tokens to file
    with open('strava_tokens.json', 'w') as outfile:
        json.dump(new_strava_tokens, outfile)
# Use new Strava tokens from now
    strava_tokens = new_strava_tokens
# Open the new JSON file and print the file contents 
# to check it's worked properly
with open('strava_tokens.json') as check:
  data = json.load(check)
print(data)

page = 1
url = "https://www.strava.com/api/v3/activities"
access_token = strava_tokens['access_token']
# Create the dataframe ready for the API call to store your activity data
activities = pd.DataFrame(
    columns = [
            "id",
            "name",
            "start_date_local",
            "type",
            "distance",
            "moving_time",
            "elapsed_time",
            "total_elevation_gain",
            "start_latlng",
            "end_latlng",
            "external_id",
            "sport_type",
            #"workout_type",
            "start_date",
            "timezone",
            "utc_offset",
            "location_city",
            "location_state",
            "location_country",
            "achievement_count",
            "kudos_count",
            "comment_count",
            "athlete_count",
            "photo_count",
            "trainer",
            "commute",
            "manual",
            "private",
            "visibility",
            "flagged",
            "gear_id",
            "average_speed",
            "max_speed",
            "has_heartrate",
            "heartrate_opt_out",
            "display_hide_heartrate_option",
            # "elev_high",
            # "elev_low",
            # "upload_id",
            # "upload_id_str",
            # "from_accepted_tag",
            # "pr_count",
            # "total_photo_count",
            # "has_kudoed"
            #"athlete.id",
            #"athlete.resource_state",
            #"map.id",
            #"map.summary_polyline",
            #"map.resource_state"
            #"workout_type"


    ]
)
while True:
    
    # get page of activities from Strava
    r = requests.get(url + '?access_token=' + access_token + '&per_page=200' + '&page=' + str(page))
    r = r.json()
    
    # if no results then exit loop
    if (not r):
        break
    
    # otherwise add new data to dataframe
    for x in range(len(r)):
        activities.loc[x + (page-1)*200,'id'] = r[x]['id']
        activities.loc[x + (page-1)*200,'name'] = r[x]['name']
        activities.loc[x + (page-1)*200,'start_date_local'] = r[x]['start_date_local']
        activities.loc[x + (page-1)*200,'type'] = r[x]['type']
        activities.loc[x + (page-1)*200,'distance'] = r[x]['distance']
        activities.loc[x + (page-1)*200,'moving_time'] = r[x]['moving_time']
        activities.loc[x + (page-1)*200,'elapsed_time'] = r[x]['elapsed_time']
        activities.loc[x + (page-1)*200,'total_elevation_gain'] = r[x]['total_elevation_gain']
        activities.loc[x + (page-1)*200,'start_latlng'] = r[x]['start_latlng']
        activities.loc[x + (page-1)*200,'end_latlng'] = r[x]['end_latlng']
        activities.loc[x + (page-1)*200,'external_id'] = r[x]['external_id']
        activities.loc[x + (page-1)*200,'sport_type'] = r[x]['sport_type']
        #activities.loc[x + (page-1)*200,'workout_type'] = r[x]['workout_type']
        activities.loc[x + (page-1)*200,'start_date'] = r[x]['start_date']
        activities.loc[x + (page-1)*200,'timezone'] = r[x]['timezone']
        activities.loc[x + (page-1)*200,'utc_offset'] = r[x]['utc_offset']
        activities.loc[x + (page-1)*200,'location_city'] = r[x]['location_city']
        activities.loc[x + (page-1)*200,'location_state'] = r[x]['location_state']
        activities.loc[x + (page-1)*200,'location_country'] = r[x]['location_country']
        activities.loc[x + (page-1)*200,'achievement_count'] = r[x]['achievement_count']
        activities.loc[x + (page-1)*200,'kudos_count'] = r[x]['kudos_count']
        activities.loc[x + (page-1)*200,'comment_count'] = r[x]['comment_count']
        activities.loc[x + (page-1)*200,'athlete_count'] = r[x]['athlete_count']
        activities.loc[x + (page-1)*200,'photo_count'] = r[x]['photo_count']
        activities.loc[x + (page-1)*200,'trainer'] = r[x]['trainer']
        activities.loc[x + (page-1)*200,'commute'] = r[x]['commute']
        activities.loc[x + (page-1)*200,'manual'] = r[x]['manual']
        activities.loc[x + (page-1)*200,'private'] = r[x]['private']
        activities.loc[x + (page-1)*200,'visibility'] = r[x]['visibility']
        activities.loc[x + (page-1)*200,'flagged'] = r[x]['flagged']
        activities.loc[x + (page-1)*200,'gear_id'] = r[x]['gear_id']
        activities.loc[x + (page-1)*200,'average_speed'] = r[x]['average_speed']
        activities.loc[x + (page-1)*200,'max_speed'] = r[x]['max_speed']
        activities.loc[x + (page-1)*200,'has_heartrate'] = r[x]['has_heartrate']
        activities.loc[x + (page-1)*200,'heartrate_opt_out'] = r[x]['heartrate_opt_out']
        #activities.loc[x + (page-1)*200,'elev_high'] = r[x]['elev_high']
        # activities.loc[x + (page-1)*200,'elev_low'] = r[x]['elev_low']
        # activities.loc[x + (page-1)*200,'upload_id'] = r[x]['upload_id']
        # activities.loc[x + (page-1)*200,'upload_id_str'] = r[x]['upload_id_str']
        # activities.loc[x + (page-1)*200,'from_accepted_tag'] = r[x]['from_accepted_tag']
        # activities.loc[x + (page-1)*200,'pr_count'] = r[x]['pr_count']
        # activities.loc[x + (page-1)*200,'total_photo_count'] = r[x]['total_photo_count']
        # activities.loc[x + (page-1)*200,'has_kudoed'] = r[x]['has_kudoed']
        #activities.loc[x + (page-1)*200,'athlete.id'] = r[x]['athlete.id']
        #activities.loc[x + (page-1)*200,'athlete.resource_state'] = r[x]['athlete.resource_state']
        #activities.loc[x + (page-1)*200,'map.id'] = r[x]['map.id']
        #activities.loc[x + (page-1)*200,'map.summary_polyline'] = r[x]['map.summary_polyline']
        #activities.loc[x + (page-1)*200,'map.resource_state'] = r[x]['map.resource_state']


    # increment page
    page += 1
# Export your activities file as a csv 
# to the folder you're running this script in
csv_filename='strava_activities_1.csv'
activities.to_csv(csv_filename)

activities.head()



#Create client
s3=boto3.client("s3")


S3_BUCKET_NAME="strava-project-1"
S3_FOLDER_ROUTE="python/strava-data/"

#Upload
with open(csv_filename,"rb") as f:
    s3.upload_fileobj(f,S3_BUCKET_NAME, S3_FOLDER_ROUTE + csv_filename)