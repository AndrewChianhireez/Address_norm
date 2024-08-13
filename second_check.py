import pandas as pd
import json
from openai import OpenAI
import httpx
import asyncio
import nest_asyncio
import numpy as np

file_path = '/content/sample_10k_ll.xlsx'
df = pd.read_excel(file_path)

def normalize_address(row):

    elements = [row['city'], row['state'], row['country']]

    cleaned_elements = []
    for element in elements:
        if pd.isna(element):
            continue
        cleaned_elements.append(str(element))

    normalized_address = ','.join(cleaned_elements)
    return normalized_address

df['is_location'] = df['is_location'].apply(lambda x: True if x == 1 else False if x == 0 else None)
df['is_usa'] = df['is_usa'].apply(lambda x: True if x == 1 else False if x == 0 else None)


subset_usa = df[(df['is_location'] == True) & (df['is_usa'] == True)]
subset_non_usa = df[(df['is_location'] == True) & (df['is_usa'] == False)]
subset_neither = df[(df['is_location'] == False) & (df['is_usa'] == False)]
subset_is_location_empty = df[df['is_location'].isna()]

subset_non_usa.fillna('', inplace=True)

subset_non_usa_no_state = subset_non_usa[
    (subset_non_usa['city'] != '') &
    (subset_non_usa['country'] != '') &
    (subset_non_usa['state'] == '')
]


subset_non_usa_other = subset_non_usa[
    ~((subset_non_usa['city'] != '') &
      (subset_non_usa['country'] != '') &
      (subset_non_usa['state'] == ''))
]

subset_non_usa_no_state=subset_non_usa_no_state.reset_index(drop=True)
subset_neither=subset_neither.reset_index(drop=True)
subset_neither['cleaned_address'] = subset_neither['cleaned_address'].astype(str)

dfs = [subset_usa, subset_non_usa_no_state, subset_non_usa_other, subset_neither, subset_is_location_empty]
combined_df = pd.concat(dfs, ignore_index=True)
shuffled_df = combined_df.sample(frac=1).reset_index(drop=True)
shuffled_df.replace(['', 'None'], np.nan, inplace=True)

shuffled_df['norm_address'] = shuffled_df.apply(normalize_address, axis=1)
shuffled_df['latitude'] = None
shuffled_df['longtitude'] = None
nest_asyncio.apply()

sem = asyncio.Semaphore(30)
async def fetch_address_data(client, address, index, df):
  async with sem:
      url = "https://api.deepseek.com/chat/completions"
      payload = json.dumps({
          "model": "deepseek-coder",
          "messages": [
              {
                  "role": "system",
                  "content": "You are a helpful assistant. Please respond to the provided address with ONLY one line of output, formatted as follows:\
                              1. True or False if the text is a location.\
                              2. True or False if it is a US location.\
                              3. City name, 'None' if not applicable.\
                              4. State name, for non USA address, there should be a similar concept like district or province, 'None' if not applicable.\
                              5. Country name, 'None' if not applicable, make sure to use iso-3166 code for consistency.\
                              End your response after the list and do not repeat.\
                              if multiple address, only get the first result and ignore other address.\
                              Example of output: 'True, True, Los Angeles, California, USA'"
              },
              {
                  "role": "user",
                  "content": address
              }
          ],
          "stream": False,
          "temperature": 1,
          "top_p": 1
      })
      headers = {
          'Authorization': 'Bearer sk-d66430fa97b24ddf908b1c875925b6c9',
          'Content-Type': 'application/json',
          'Accept': 'application/json'
      }

      response = await client.post(url, headers=headers, content=payload)
      print(f"Received response for address at index {index}")
      if response.status_code == 200:
          response_json = response.json()
          if 'choices' in response_json and len(response_json['choices']) > 0:
              response_data = response_json['choices'][0]['message']['content'].strip().split(',')
              if len(response_data) == 5:
                  df.at[index, 'is_location'] = response_data[0].strip() == 'True'
                  df.at[index, 'is_usa'] = response_data[1].strip() == 'True'
                  df.at[index, 'city'] = response_data[2].strip()
                  df.at[index, 'state'] = response_data[3].strip()
                  df.at[index, 'country'] = response_data[4].strip()
              else:
                  print(f"Data format issue at index {index} with address '{address}': {response_data}")
          else:
              print(f"No choices found for address '{address}': {response_json}")
      else:
          print(f"Failed to get result for address '{address}', Status Code: {response.status_code}, Response: {response.text}")

async def process_addresses(df):
    async with httpx.AsyncClient(limits=httpx.Limits(max_connections=100, max_keepalive_connections=20)) as client:
        tasks = [fetch_address_data(client, addr, i, df) for i, addr in enumerate(df['cleaned_address'])]
        await asyncio.gather(*tasks)
task = asyncio.create_task(process_addresses(subset_non_usa_no_state))
await task


sem = asyncio.Semaphore(30)
async def fetch_address_data(client, address, index, df):
  async with sem:
      url = "https://api.deepseek.com/chat/completions"
      payload = json.dumps({
          "model": "deepseek-coder",
          "messages": [
              {
                  "role": "system",
                  "content": "You are a helpful assistant. Please respond to the provided address with ONLY one line of output, formatted as follows:\
                              1. True or False if the text is a location. Terms like street, drive, blvd, apt may indicate it is an address.\
                              2. True or False if it is a US location.\
                              3. City name, 'None' if not applicable.\
                              4. State , for non USA address, there should be a similar concept like district or province, 'None' if not applicable.\
                              5. Country name, 'None' if not applicable, make sure to use iso-3166 code for consistency.\
                              End your response after the list and do not repeat.\
                              if multiple address, only get the first result and ignore other address, do not include 1. 2. 3. in the result. \
                              Example of output: 'True, True, Los Angeles, California, USA'"
              },
              {
                  "role": "user",
                  "content": address
              }
          ],
          "stream": False,
          "temperature": 1,
          "top_p": 1
      })
      headers = {
          'Authorization': 'Bearer sk-d66430fa97b24ddf908b1c875925b6c9',
          'Content-Type': 'application/json',
          'Accept': 'application/json'
      }

      response = await client.post(url, headers=headers, content=payload)
      print(f"Received response for address at index {index}")
      if response.status_code == 200:
          response_json = response.json()
          if 'choices' in response_json and len(response_json['choices']) > 0:
              response_data = response_json['choices'][0]['message']['content'].strip().split(',')
              if len(response_data) == 5:
                  df.at[index, 'is_location'] = response_data[0].strip() == 'True'
                  df.at[index, 'is_usa'] = response_data[1].strip() == 'True'
                  df.at[index, 'city'] = response_data[2].strip()
                  df.at[index, 'state'] = response_data[3].strip()
                  df.at[index, 'country'] = response_data[4].strip()
              else:
                  print(f"Data format issue at index {index} with address '{address}': {response_data}")
          else:
              print(f"No choices found for address '{address}': {response_json}")
      else:
          print(f"Failed to get result for address '{address}', Status Code: {response.status_code}, Response: {response.text}")

async def process_addresses(df):
    async with httpx.AsyncClient(limits=httpx.Limits(max_connections=100, max_keepalive_connections=20)) as client:
        tasks = [fetch_address_data(client, addr, i, df) for i, addr in enumerate(df['cleaned_address'])]
        await asyncio.gather(*tasks)
task = asyncio.create_task(process_addresses(subset_neither))
await task





sem = asyncio.Semaphore(25)
async def fetch_address_data(client, address, index, df):
  async with sem:
      url = "https://api.deepseek.com/chat/completions"
      payload = json.dumps({
          "model": "deepseek-coder",
          "messages": [
              {
                  "role": "system",
                  "content": "You are a helpful assistant. Please respond to the provided address with ONLY one line of output, formatted as follows:\
                            give the longtitude and latitude of the address, you shoule be able to give the result even only country name provided.\
                            if the address is none, just return a list of 2 None .\
                            End your response after the list and do not repeat.\
                            Make sure you read the entire address, especially the country code since city name maybe prersent at many countries.\
                            Example: '38.0000 , -98.0000'"
              },
              {
                  "role": "user",
                  "content": address
              }
          ],
          "stream": False,
          "temperature": 1,
          "top_p": 1
      })
      headers = {
          'Authorization': 'Bearer sk-d66430fa97b24ddf908b1c875925b6c9',
          'Content-Type': 'application/json',
          'Accept': 'application/json'
      }

      response = await client.post(url, headers=headers, content=payload)
      print(f"Received response for address at index {index}")
      if response.status_code == 200:
          response_json = response.json()
          if 'choices' in response_json and len(response_json['choices']) > 0:
              response_data = response_json['choices'][0]['message']['content'].strip().split(',')
              if len(response_data) == 2:
                  df.at[index, 'latitude'] = response_data[0].strip()
                  df.at[index, 'longtitude'] = response_data[1].strip()

              else:
                  print(f"Data format issue at index {index} with address '{address}': {response_data}")
          else:
              print(f"No choices found for address '{address}': {response_json}")
      else:
          print(f"Failed to get result for address '{address}', Status Code: {response.status_code}, Response: {response.text}")

async def process_addresses(df):
    async with httpx.AsyncClient(limits=httpx.Limits(max_connections=100, max_keepalive_connections=20)) as client:
        tasks = [fetch_address_data(client, addr, i, df) for i, addr in enumerate(df['norm_address'])]
        await asyncio.gather(*tasks)
task = asyncio.create_task(process_addresses(shuffled_df))
await task

shuffled_df['latitude'] = shuffled_df['latitude'].str.strip("[]'")
shuffled_df['longtitude'] = shuffled_df['longtitude'].str.strip("[]'")

path = '/content/drive/My Drive/Colab Notebooks/Final_sample_10k.csv'

shuffled_df.to_csv(path, index=False, encoding='utf-8')