from datetime import datetime, timedelta, timezone
from requests_oauthlib import OAuth2Session
import streamlit as st
import pandas as pd
import requests
import argparse
import pytz
import json
import os
st.title("Snapchat API Data Download")

# ---- DOWNLOADING DATA ----#
with open('./snap_credentials.json','r') as file:
    snap_credentials = json.load(file)
    
def day_diff_strptime(start, end):
	"""Calculate the number of days between two dates."""
	s = datetime.strptime(start, '%Y-%m-%d')
	e = datetime.strptime(end, '%Y-%m-%d')
	return (e-s).days

def get_snapchat_access_token(snap_credentials):
		"""Retrieve snapchat access token using credentials."""
		# ---- IMPORTING CREDENTIALS ---- #
		with open('./snapchat_credentials.json', 'r') as f:
			snapchat_credentials = json.load(f)

		st.write("Credentials: ", snapchat_credentials)

		# ---- GENERATING ACCESS TOKEN FROM CLIENT ID AND CLIENT SECRET ---- #
		scope = ['snapchat-marketing-api']
		authorize_url = 'https://accounts.snapchat.com/login/oauth2/authorize'
		access_token_url = 'https://accounts.snapchat.com/login/oauth2/access_token'
		protected_url = 'https://adsapi.snapchat.com/v1/me/organizations'

		oauth = OAuth2Session(
			snapchat_credentials['client_id'],
			redirect_uri=snapchat_credentials['redirect_url'],
			scope=scope
		)

		authorization_url, state = oauth.authorization_url(authorize_url)
		st.write('\nPlease go to %s and authorize access.\n' % authorization_url)

		authorization_response = st.text_input(' \n \n Enter the full callback URL: ')
		button_access = st.button("Done")
		if button_access:
			token = oauth.fetch_token(
				access_token_url,
				authorization_response=authorization_response,
				client_secret=snapchat_credentials['client_secret'],
				scope=scope
			)
			
			snapchat_credentials['access_token'] = oauth.token['access_token']
			snapchat_credentials['refresh_token'] = oauth.token['refresh_token']
			st.write("Done")
			# ---- GENERATE ACCESS FROM REFRESH TOKEN ----#
			access_params = {
				'client_id': snapchat_credentials['client_id'],
				'client_secret': snapchat_credentials['client_secret'],
				'code': snapchat_credentials['refresh_token'], # Get it in first step in redirect URL
				'grant_type': 'refresh_token',
			}

			res = requests.post(
				access_token_url,
				params = access_params
			)

			snapchat_credentials['access_token'] = res.json()['access_token']
			snapchat_credentials['refresh_token'] = res.json()['refresh_token']

			snapTokens = {
				'client_id': str(snapchat_credentials['client_id']),
				'client_secret': str(snapchat_credentials['client_secret']),
				'redirect_url': str(snapchat_credentials['redirect_url']),
				'access_token': str(snapchat_credentials['access_token']),
				'refresh_token': str(snapchat_credentials['refresh_token'])
			}

			with open('./snapchat_credentials.json', 'w') as f:
				json.dump(snapTokens,f)

			st.write("Done")

			return snapTokens['access_token']
		else:
			st.write("Enter the link")

def get_all_campaigns(access_token, ad_accounts_id):
	"""Get all campaigns running on the account in the form of a list."""
      
	# Initialize
	url_campaigns = 'https://adsapi.snapchat.com/v1/adaccounts/%s/campaigns' % (ad_accounts_id)
	headers= {'Authorization': 'Bearer %s' % (access_token)}

	# Get
	res= requests.get(
		url_campaigns,
		headers = headers
	)

	# Store
	campaign_ids = list()
	for c in res.json()['campaigns']:
		campaign_ids += [c['campaign']['id']]

	return campaign_ids

def get_report_from_campaign_id(access_token, campaign_id, start_date, end_date):  
	"""Get stats of a specific campaign_id."""
	# Initialize
	headers= {'Authorization': 'Bearer %s' % (access_token)}
	
	# Get dates
	start_time = (pytz
		.timezone('Europe/Paris')
		.localize(datetime.strptime(start_date, '%Y-%m-%d'))
		.isoformat()
	)
	end_time = (pytz
		.timezone('Europe/Paris')
		.localize(datetime.strptime(end_date, '%Y-%m-%d'))
		.isoformat()
	)
	
	# Prepare
	df = pd.DataFrame()
	url_reporting = 'https://adsapi.snapchat.com/v1/campaigns/%s/stats' % campaign_id
	params = {
		'start_time':start_time,
		'end_time':end_time,
		'granularity': 'DAY',
	}
	
	# Run
	res= requests.get(
		url_reporting,
		params = params,
		headers = headers
	)

	# Format
	for item in res.json()['timeseries_stats'][0]['timeseries_stat']['timeseries']:
		dict_ = {
			'campaign_id': campaign_id,
			'start_time': item['start_time'],
			'end_time': item['end_time'],
			'impressions': item['stats']['impressions'],
			'spend': item['stats']['spend'] / 1000000
		}
		df = df.append(dict_, ignore_index=True)
	
	return df

def main(snap_credentials, start_date, end_date):
	# Initialize
	snap = pd.DataFrame()

	# Get access token from refresh token
	access_token = get_snapchat_access_token(snap_credentials)

	# Get all campaign ids
	st.write('Getting Snapchat API access token...')
	campaign_ids = get_all_campaigns(access_token, snap_credentials['ad_accounts_id'])

	# Get campaign data from snapchat API
	st.write('Getting all campaigns from Snapchat API...')
	for campaign_id in campaign_ids:
		new = get_report_from_campaign_id(
			access_token,
			campaign_id,
			start_date,
			end_date
		)
		snap = pd.concat([snap,new])

	return snap

if __name__ == '__main__':
		# Taking inputs from the user for dates
		st.write("Enter the Start Date: ")
		startDate = st.text_input("Format: YYYY-MM-DD ")
			
		st.write("Enter the End Date: ")
		endDate = st.text_input("Format: YYYY-MM-DD")

		button = st.button("Submit")
		if button:
			# Check that time period is not too large
			if day_diff_strptime(startDate, endDate) >= 30:
				st.error('The difference between start and end date must be less than 31 days')
				exit()

			# Retrieve snapchat data
			snap = main(snap_credentials, startDate, endDate)

		# download CSV
		dwnld_button = st.button("Download File")
		if dwnld_button:
			st.write('Saving data to csv file...')
			snap.to_csv('snap_' + startDate + '_' + endDate + '.csv', index=False)
			st.write('The file has been saved successfully !!')
