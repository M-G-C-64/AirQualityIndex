import os

print("trying to access the keys for aqi_api_key")
print(os.getenv('aqi_api_key'))
print("didint work")
print(os.getenv('AQI_API_KEY'))


