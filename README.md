# Event-Driven Data Warehouse Pipeline

## Overview
Проект — это распределённая аналитическая платформа на основе event-driven архитектуры и DWH (STG → DDS → CDM).
Система состоит из нескольких сервисов, каждый отвечает за свой слой обработки данных и разворачивается отдельно в Kubernetes.
Инфраструктура поднята в Yandex Cloud.

## Infrastructure
- Kafka — поток событий
- PostgreSQL — хранение данных (STG / DDS / CDM)
- Redis (Valkey) — кэш и обогащение данных
- Container Registry — хранение Docker-образов
- Kubernetes (Yandex Cloud) — запуск сервисов
- DataLens — дашборды

## Microservices
Система разделена на 3 сервиса:
- `service_stg` — приём и первичная обработка данных
- `service_dds` — очистка и приведение к модели (Data Vault)
- `service_cdm` — формирование витрин

Каждый сервис:
- собран в Docker образ
- хранится в Container Registry
- запускается в Kubernetes отдельно

## Data Layers
### STG
- сырые данные
- без бизнес-логики
- идемпотентная загрузка
### DDS
- очищенные и нормализованные данные
- модель Data Vault
- основа для дальнейших витрин
### CDM
- витрины для аналитики
- агрегаты и финальные таблицы

## Architecture decisions
- Kafka используется для event-driven архитектуры
- Разделение на STG / DDS / CDM для понятной структуры данных
- Redis используется для кеша и enrichment
- Каждый слой — отдельный сервис
- Kubernetes управляет запуском и масштабированием

## Cloud setup
Все развернуто в Yandex Cloud:
- Kafka cluster
- PostgreSQL
- Redis (Valkey)
- Container Registry
- Kubernetes cluster
CI/CD:
- сборка Docker образов
- загрузка в registry
- деплой в Kubernetes

## What I learned
В этом проекте я:
- собрал DWH архитектуру STG → DDS → CDM
- работал с Kafka и event-driven подходом
- развернул инфраструктуру в облаке
- использовал Kubernetes для микросервисов
- проектировал ETL процессы
- работал с Redis для ускорения обработки данных
