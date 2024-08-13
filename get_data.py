import pandas as pd
import json
from openai import OpenAI
import httpx
import asyncio
import nest_asyncio

# !pip install openai
# pip install httpx
# !pip install nest_asyncio

with open('loc_values.json', 'r') as file:
    data = json.load(file)
df = pd.DataFrame(data, columns=['raw_address'])

def clean_address(address):

    cleaned = address.replace('\n', ' ')
    cleaned = ' '.join(cleaned.split())

    return cleaned

sampled_df = df.sample(n=10000, random_state=42)

sampled_df['original_index'] = sampled_df.index
sampled_df = sampled_df.reset_index(drop=True)

sampled_df['cleaned_address'] = sampled_df['raw_address'].apply(clean_address)

sampled_df['is_location'] = None
sampled_df['is_usa'] = None
sampled_df['city'] = None
sampled_df['state'] = None
sampled_df['country'] = None

# i'm using google colab and an interactive environments need this nest_asyncio
# regular python environment just use asyncio.run(process_addresses(df)) should be fine, or i think running this should also be fine. 
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
                              4. State name, 'None' if not applicable.\
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
task = asyncio.create_task(process_addresses(sampled_df))

await task

# run below command is fine I think for a python script
# asyncio.run(process_addresses(df))

# change this to your path
path = 'Final_sample_10k.csv'

sampled_df.to_csv(path, index=False, encoding='utf-8')