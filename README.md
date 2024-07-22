# Introduction
Efficiently process background tasks using Celery and Kafka as message broker with filesystem as backend for results.

## Start services
```
  brew services start zookeeper
  brew services start kafka
  brew services start redis
  brew services info redis
```

## Stop services
```
  brew services stop kafka
  brew services stop zookeeper
  brew services stop redis
```

## Run application
```
  uvicorn main:app --reload
  celery -A main.celery_app worker --loglevel=info  
```