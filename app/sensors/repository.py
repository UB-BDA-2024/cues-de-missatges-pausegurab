from datetime import datetime, timedelta
from fastapi import HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
from app.mongodb_client import MongoDBClient
from app.redis_client import RedisClient
from app.elasticsearch_client import ElasticsearchClient
from app.timescale import Timescale
from app.cassandra_client import CassandraClient
import json


from app.sensors import models, schemas

def get_sensor(db: Session, sensor_id: int) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()

def get_sensor_by_name(db: Session, name: str) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.name == name).first()

def get_sensors(db: Session, skip: int = 0, limit: int = 100) -> List[models.Sensor]:
    return db.query(models.Sensor).offset(skip).limit(limit).all()

def create_sensor(db: Session, sensor: schemas.SensorCreate, mongo_db: MongoDBClient) -> models.Sensor:
    db_sensor = models.Sensor(name=sensor.name)
    db.add(db_sensor)
    db.commit()
    db.refresh(db_sensor)
    add_document(mongo_db=mongo_db, sensor = sensor)
    
    col = connect_collection(mongodb=mongo_db)

    documental_sensor = col.find_one({"name": sensor.name})
    


    es = ElasticsearchClient(host="elasticsearch")
    if not es.client.indices.exists(index='sensors'):
        es.create_index('sensors')
        mapping = {
            'properties': {
                'name': {'type': 'keyword'},
                'description': {'type': 'text'},
                'type': {'type': 'text'}
            }
        }
        es.create_mapping('sensors',mapping)

    es_doc = {
            'name': documental_sensor["name"],
            'description': documental_sensor["description"],
            'type': documental_sensor["type"]
    }
    
    es.index_document('sensors', es_doc)



    sensor_instance = schemas.Sensor(
        id=db_sensor.id,
        name=documental_sensor["name"],
        latitude=documental_sensor["location"]["coordinates"][1],
        longitude=documental_sensor["location"]["coordinates"][0],
        type=documental_sensor["type"],
        mac_address=documental_sensor["mac_address"],
        manufacturer=documental_sensor["manufacturer"],
        model=documental_sensor["model"],
        serie_number=documental_sensor["serie_number"],
        firmware_version=documental_sensor["firmware_version"],
        description=documental_sensor["description"]
    )

    sensor_dict = sensor_instance.dict(exclude_none=True)
    return sensor_dict


def record_data(db: Session, redis: RedisClient, sensor_id: int, data: schemas.SensorData, mongo_db: MongoDBClient, timescale: Timescale, cassandra: CassandraClient) -> schemas.Sensor:
    # Creem les claus compostes per cada un dels atributs
    temp = "sensor" + str(sensor_id) + ":temperatura"
    hum = "sensor" + str(sensor_id) + ":humidity"
    bat = "sensor" + str(sensor_id) + ":battery_level"
    seen = "sensor" + str(sensor_id) + ":last_seen"
    vel = "sensor" + str(sensor_id) + ":velocity"

    # Fem els post de cada un dels atributs amb la seva clau i el seu valor
   
    redis.set(bat, data.battery_level)
    redis.set(seen, data.last_seen)
    if data.velocity is not None:
        redis.set(vel, data.velocity)
    if data.temperature is not None:      
        redis.set(temp, data.temperature)
    if data.humidity is not None:
        redis.set(hum, data.humidity)

    
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()

    sensor_name = db_sensor.name

    col = connect_collection(mongodb=mongo_db)

    cassandra.execute("USE sensor")
    documental_sensor = col.find_one({"name": sensor_name})


    if data.temperature == None:
    
        query = f"""INSERT INTO sensor_data (sensor_id, velocity, battery_level, last_seen)
                VALUES ('{sensor_id}', {data.velocity}, {data.battery_level}, '{data.last_seen}')"""
        timescale.execute(query)

    if data.velocity == None:
    
        query = f"""INSERT INTO sensor_data (sensor_id, temperature, humidity, battery_level, last_seen)
                VALUES ('{sensor_id}', {data.temperature}, {data.humidity},{data.battery_level}, '{data.last_seen}')"""
        timescale.execute(query)
        
    timescale.commit()

    if data.temperature is not None:

        cassandra.get_session().execute("""
            INSERT INTO temperature (sensor_id, temperature)
            VALUES (%s, %s)
            """,
            (sensor_id, data.temperature)
        )
    cassandra.get_session().execute("""
           INSERT INTO types (type, sensor_id)
            VALUES (%s, %s)
            """,
            (documental_sensor["type"], sensor_id)
        )
    
    cassandra.get_session().execute("""
            INSERT INTO battery (sensor_id, battery_level)
            VALUES (%s, %s)
            """,
                (sensor_id, data.battery_level)
        )
    
    last_seen = redis.get(seen)
    battery_level = redis.get(bat)
    temperature = None
    humidity = None
    velocity = None
    if data.temperature is not None:
        temperature = redis.get(temp)
    if data.humidity is not None:
        humidity = redis.get(hum)
    if data.velocity is not None:
        velocity = redis.get(vel)



    return schemas.Sensor(
        id=db_sensor.id,
        name=db_sensor.name,
        latitude=documental_sensor["location"]["coordinates"][1],
        longitude=documental_sensor["location"]["coordinates"][0],
        joined_at=str(db_sensor.joined_at),
        last_seen=last_seen,
        type=documental_sensor["type"],
        mac_address=documental_sensor["mac_address"],
        battery_level=battery_level,
        temperature=temperature,
        humidity=humidity,
        velocity=velocity,
        manufacturer=documental_sensor["manufacturer"],
        model=documental_sensor["model"],
        serie_number=documental_sensor["serie_number"],
        firmware_version=documental_sensor["firmware_version"],
        description=documental_sensor["description"]

        
        
    )






def get_data(db: Session, sensor_id: int, from_date: str, to:str, bucket:str,  timescale: Timescale, mongodb: MongoDBClient, redis: RedisClient):
    # Trobem el sensor corresponent a la id
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()


    
    # Creem les claus compostes per cada un dels atributs
    temp = "sensor" + str(sensor_id) + ":temperatura"
    hum = "sensor" + str(sensor_id) + ":humidity"
    bat = "sensor" + str(sensor_id) + ":battery_level"
    seen = "sensor" + str(sensor_id) + ":last_seen"
    vel = "sensor" + str(sensor_id) + ":velocity"


    sensor_name = db_sensor.name

    col = connect_collection(mongodb=mongodb)

    documental_sensor = col.find_one({"name": sensor_name})


    last_seen = redis.get(seen)
    battery_level = redis.get(bat)
    temperature=redis.get(temp)
    humidity=redis.get(hum)
    velocity=redis.get(vel)

    timescale.commit()
    if bucket is not None:
        query_create = f"""CREATE MATERIALIZED VIEW IF NOT EXISTS conditions_summary_{bucket}
            WITH (timescaledb.continuous) AS
            SELECT sensor_id,
            time_bucket(INTERVAL '1 {bucket}', last_seen) AS bucket,
            AVG(temperature) AS temp_avg
            FROM sensor_data
            GROUP BY sensor_id, bucket;
            """

        timescale.execute(query_create)
        timescale.commit()
    
    if bucket == "week":
        from_ = datetime.fromisoformat(from_date[:-1])
        from_date = from_ - timedelta(days=from_.weekday())
    if bucket is not None:
        query = f"""
            SELECT *
            FROM conditions_summary_{bucket}
            WHERE sensor_id = {sensor_id}
            AND bucket >= '{from_date}'
            AND bucket <= '{to}';
        """

        timescale.execute(query)


        cursor = timescale.getCursor()
        results = cursor.fetchall()
        return results
    else: 
        return schemas.Sensor(
            id=db_sensor.id,
            name=db_sensor.name,
            latitude=documental_sensor["location"]["coordinates"][1],
            longitude=documental_sensor["location"]["coordinates"][0],
            joined_at=str(db_sensor.joined_at),
            last_seen=redis.get(seen),
            type=documental_sensor["type"],
            mac_address=documental_sensor["mac_address"],
            manufacturer=documental_sensor["manufacturer"],
            model=documental_sensor["model"],
            serie_number=documental_sensor["serie_number"],
            firmware_version=documental_sensor["firmware_version"],
            battery_level=redis.get(bat),
            temperature=redis.get(temp),
            humidity=redis.get(hum),
            velocity=redis.get(vel),
            description=documental_sensor["description"]
            
        )
    

def add_document(mongo_db: MongoDBClient, sensor: schemas.SensorCreate):
    
    info = connect_collection(mongodb=mongo_db)
    

    if "location_2dsphere" not in info.index_information():
        info.create_index([("location", "2dsphere")])
    


    coll = {
        "name": sensor.name,
        "location" :{
                'type': 'Point',
                'coordinates': [sensor.longitude, sensor.latitude]
        }, 
        "type": sensor.type,
        "mac_address": sensor.mac_address,
        "manufacturer": sensor.manufacturer,
        "model" : sensor.model,
        "serie_number" : sensor.serie_number,
        "firmware_version" : sensor.firmware_version,
        "description" : sensor.description
    }

    info.insert_one(coll)

def connect_collection(mongodb: MongoDBClient):
    database = mongodb.getDatabase("data")
    col = mongodb.getCollection("sensors")
    return col

def get_sensor_complete_information(db: Session, sensor_id: int, mongo_db=MongoDBClient):
    db_sensor = get_sensor(db,sensor_id)
    sensor_name = db_sensor.name

    col = connect_collection(mongo_db)

    documental_sensor = col.find_one({"name":sensor_name})
    sensor_instance = schemas.Sensor(
        id=db_sensor.id,
        name=documental_sensor["name"],
        latitude=documental_sensor["location"]["coordinates"][1],
        longitude=documental_sensor["location"]["coordinates"][0],
        type=documental_sensor["type"],
        mac_address=documental_sensor["mac_address"],
        manufacturer=documental_sensor["manufacturer"],
        model=documental_sensor["model"],
        serie_number=documental_sensor["serie_number"],
        firmware_version=documental_sensor["firmware_version"],
        description=documental_sensor["description"]
    )

    sensor_dict = sensor_instance.dict(exclude_none=True)
    return sensor_dict

def get_sensors_near(db: Session, mongodb: MongoDBClient, redis: RedisClient, latitude: float, longitude: float, radius: float):
    
    query = create_query(latitude, longitude, radius)

    col = connect_collection(mongodb=mongodb)

    sensors_near = col.find(query)
    sensors_list = []

    for sensors in sensors_near:
        sensor_id_value = sensors["name"]
        sensor_name = get_sensor_by_name(db, sensor_id_value)
        sensor_id = sensor_name.id
        sensor_schema = get_data(db=db, sensor_id=sensor_id, mongo_db=mongodb, redis=redis)
        sensors_list.append(sensor_schema)
        

    return sensors_list

def create_query(latitude, longitude, radius):
    query = {
    'location': {
        '$near': {
            '$geometry': {
                'type': 'Point',
                'coordinates': [longitude, latitude]
            },
            '$maxDistance': radius 
            }
        }
    }
    return query


def search_sensors(db: Session, mongodb: MongoDBClient, query: str, size: int = 10, search_type: str = "match"):

    es = ElasticsearchClient(host="elasticsearch")

    es_index_name = "sensors"

    mongo_db = mongodb.getDatabase("data")
    mongo_collection = mongodb.getCollection("sensors")



    query_dict = json.loads(query)

    clau, valor = next(iter(query_dict.items()))
    

    querySearch = {
        'size' : size,
        'query' : {
            search_type : query_dict
        }

    }
    querySearchSimilar = {
        'size' : size,
        'query' : {
            'fuzzy': {
                clau: {
                    'value' : valor
                }
             
            }
        }
    }
    if search_type == "similar":

        search_result = es.search(es_index_name, querySearchSimilar)
    else:
        search_result = es.search(es_index_name, querySearch)
    sensors_retrived = []
    for hit in search_result['hits']['hits']:
        name = hit['_source']['name']
        sensor = get_sensor_by_name(db, name)
        sensor_id = sensor.id
        dict_sensor = get_sensor_complete_information(db,sensor_id,mongodb)
        sensors_retrived.append(dict_sensor)

    es.close()
    return sensors_retrived

def delete_sensor(db: Session, sensor_id: int):
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    db.delete(db_sensor)
    db.commit()
    return db_sensor

def delete_sensor(db: Session, sensor_id: int):
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    db.delete(db_sensor)
    db.commit()
    return db_sensor

def get_temperature_sensors(db: Session, redis_client: RedisClient, mongo_db: MongoDBClient, cassandra: CassandraClient):
    results = cassandra.execute("""SELECT sensor_id, MIN(temperature) AS min_temperature, MAX(temperature) AS max_temperature,
                                AVG(temperature) AS average_temperature
                                FROM temperature
                                GROUP BY sensor_id;""")
            
    json_retrieved = []

    for row in results:
        sensor_dict = get_sensor_complete_information(db, row.sensor_id, mongo_db)
        sensor_dict["values"] = [{"max_temperature" : row.max_temperature, "min_temperature": row.min_temperature, "average_temperature" : row.average_temperature}]
        json_retrieved.append(sensor_dict)

    data_to_convert_to_json = {"sensors": json_retrieved}
    

    return data_to_convert_to_json
        
def get_quantity_by_type(cassandra: CassandraClient):
    cassandra.execute("USE sensor;")
    results = cassandra.execute("""SELECT type, COUNT(sensor_id) AS quantity FROM types GROUP BY type;""")
    json_retrieved = []
    for row in results:
        type_dict = {"type" : row.type, "quantity": row.quantity}
        json_retrieved.append(type_dict)

    data_to_convert_to_json = {"sensors" : json_retrieved}
    return data_to_convert_to_json

def get_low_battery(db: Session, redis_client: RedisClient, mongo_db: MongoDBClient, cassandra: CassandraClient):
    results = cassandra.execute("""SELECT sensor_id, battery_level
                                FROM battery
                                WHERE battery_level < 0.2
                                ALLOW FILTERING;""")
            
    json_retrieved = []

    for row in results:
        sensor_dict = get_sensor_complete_information(db, row.sensor_id, mongo_db)
        sensor_dict["battery_level"] = row.battery_level
        json_retrieved.append(sensor_dict)

    data_to_convert_to_json = {"sensors": json_retrieved}
    

    return data_to_convert_to_json
        