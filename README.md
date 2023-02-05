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