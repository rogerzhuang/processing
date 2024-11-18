import connexion
from connexion import NoContent
import requests
import yaml
import logging
import logging.config
import datetime
import json
from apscheduler.schedulers.background import BackgroundScheduler
import os
from flask_cors import CORS

with open('app_config.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

with open('log_config.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basicLogger')

def get_stats():
    logger.info("Request for statistics received")
    
    if not os.path.exists(app_config['datastore']['filename']):
        logger.error("Statistics do not exist")
        return {"message": "Statistics do not exist"}, 404
    
    with open(app_config['datastore']['filename'], 'r') as f:
        stats = json.load(f)
    
    # Convert stats to match the structure defined in openapi.yaml
    response_data = {
        "num_air_quality_readings": stats["num_air_quality_readings"],
        "num_weather_readings": stats["num_weather_readings"],
        "max_pm25_concentration": stats["max_pm25_concentration"],
        "avg_temperature": stats["avg_temperature"],
        "last_updated": stats["last_updated"]
    }
    
    logger.debug(f"Statistics: {response_data}")
    
    logger.info("Request for statistics completed")
    
    return response_data, 200

def populate_stats():
    logger.info("Start Periodic Processing")
    
    if os.path.exists(app_config['datastore']['filename']):
        with open(app_config['datastore']['filename'], 'r') as f:
            stats = json.load(f)
    else:
        stats = {
            "num_air_quality_readings": 0,
            "num_weather_readings": 0,
            "max_pm25_concentration": 0,
            "avg_temperature": 0,
            "last_updated": "2024-01-01T00:00:00Z"
        }
    
    current_timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
    last_updated = stats['last_updated']
    
    air_quality_response = requests.get(
        f"{app_config['eventstore']['url']}/air-quality?start_timestamp={last_updated}&end_timestamp={current_timestamp}")
    weather_response = requests.get(
        f"{app_config['eventstore']['url']}/weather?start_timestamp={last_updated}&end_timestamp={current_timestamp}")
    
    if air_quality_response.status_code == 200:
        air_quality_events = air_quality_response.json()
        logger.info(f"Received {len(air_quality_events)} air quality events")
        stats['num_air_quality_readings'] += len(air_quality_events)
        for event in air_quality_events:
            if event['pm2_5_concentration'] > stats['max_pm25_concentration']:
                stats['max_pm25_concentration'] = event['pm2_5_concentration']
    else:
        logger.error(f"Failed to get air quality events with status {air_quality_response.status_code}")
    
    if weather_response.status_code == 200:
        weather_events = weather_response.json()
        logger.info(f"Received {len(weather_events)} weather events")
        stats['num_weather_readings'] += len(weather_events)
        if weather_events:
            total_temp = sum(event['temperature'] for event in weather_events)
            avg_temp = total_temp / len(weather_events)
            stats['avg_temperature'] = (stats['avg_temperature'] * (stats['num_weather_readings'] - len(weather_events)) + avg_temp * len(weather_events)) / stats['num_weather_readings']
    else:
        logger.error(f"Failed to get weather events with status {weather_response.status_code}")
    
    stats['last_updated'] = current_timestamp
    
    with open(app_config['datastore']['filename'], 'w') as f:
        json.dump(stats, f)
    
    logger.debug(f"Updated statistics: {stats}")
    logger.info("Periodic Processing Ended")

def init_scheduler():
    sched = BackgroundScheduler(daemon=True)
    sched.add_job(populate_stats, 
                  'interval', 
                  seconds=app_config['scheduler']['period_sec'])
    sched.start()

app = connexion.FlaskApp(__name__, specification_dir='')
CORS(app.app)
app.add_api("openapi.yml", strict_validation=True, validate_responses=True)

if __name__ == "__main__":
    init_scheduler()
    app.run(port=8100, host="0.0.0.0")
