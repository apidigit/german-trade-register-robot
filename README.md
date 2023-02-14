# German Trade Register Robot

### Get started

- Create virtual environment to isolate dependencies
```
virtualenv venv --python=python3.7
```

- Activate virtual environment 
```
source venv/bin/activate
```

- Upgrade pip (optional)
```
pip install --upgrade pip
```

- Install all dependencies
```
pip install pymysql scrapy selenium pathlib webdriver_manager watchfiles pymupdf
```
- Install Docker Engine

Refer to the [official docker website](https://docs.docker.com/engine/install/)

- Run MySQL Docker image
```
docker run --name handelsregister_mysql \
  -p 3307:3306 \
  -e ALLOW_EMPTY_PASSWORD=yes \
  -e MYSQL_USER=handelsregister_un \
  -e MYSQL_PASSWORD=handelsregister_pw \
  -e MYSQL_DATABASE=handelsregister_db \
  -e MYSQL_AUTHENTICATION_PLUGIN=mysql_native_password \
  bitnami/mysql:latest
```

- Initialize the database schema

Run the script located in the file `resources/db_schema.sql`

- Initialize the street files
```
python streetextractor.py
```

- Start crawling the german trade registry
```
python traderegistry.py
```

- Process downloaded trade registry files
```
python fileprocessor.py
```