# Event-Driven Data Warehouse Pipeline

## Overview

Проект демонстрирует построение распределённой аналитической платформы
на основе event-driven архитектуры и многослойного DWH (STG → DDS → CDM).

Система реализована как набор микросервисов, каждый из которых отвечает
за отдельный слой обработки данных и разворачивается независимо в Kubernetes.

Инфраструктура полностью поднята в Yandex Cloud.

## Infrastructure
- Kafka — поток событий (ingestion layer)
- PostgreSQL — хранение слоёв STG/DDS/CDM
- Redis (Valkey) — обогащение данных
- Container Registry — хранение Docker-образов
- Kubernetes (Yandex Cloud) — оркестрация сервисов
- DataLens — визуализация витрин

## Microservices Architecture
Каждый слой реализован как отдельный сервис:
- `service_stg` — обогащение и первичная нормализация данных
- `service_dds` — очистка и приведение к доменной модели (Data Vault)
- `service_cdm` — построение аналитических витрин

Каждый сервис:
- упакован в Docker image
- хранится в Container Registry
- разворачивается в Kubernetes как независимый компонент

## Data Layers

### STG (Staging Layer)
- хранит сырые данные без бизнес-логики
- обеспечивает идемпотентность загрузок

### DDS (Data Detail Store)
- нормализованные сущности
- модель в стиле Data Vault
- ключевая задача — консистентность данных

### CDM (Core Data Mart)
- агрегированные витрины
- оптимизированы под BI и аналитические запросы

## Engineering Decisions
- Использована event-driven архитектура через Kafka для слабой связанности сервисов
- Разделение на STG / DDS / CDM для поддержки масштабирования и историчности данных
- Redis (Valkey) применяется для ускорения enrichment и хранения промежуточного состояния
- Каждый слой вынесен в отдельный микросервис для независимого деплоя
- Использован Kubernetes для управления жизненным циклом сервисов

## Cloud Infrastructure
Все компоненты развернуты в **Yandex Cloud**:
- Kafka cluster
- PostgreSQL instances
- Redis (Valkey)
- Container Registry
- Kubernetes cluster

CI/CD процесс:
- сборка Docker образов
- пуш в registry
- деплой в Kubernetes

## Lessons Learned
В рамках проекта я:
- спроектировал и реализовал многослойную DWH архитектуру (STG → DDS → CDM)
- работал с event-driven системами на Kafka
- развернул распределённую инфраструктуру в Yandex Cloud
- использовал Kubernetes для оркестрации микросервисов
- реализовал ETL/ELT пайплайны в микросервисной архитектуре
- применил Redis (Valkey) для ускорения обработки данных

## Key Value
Проект демонстрирует навыки:
- Data Engineering (ETL, DWH, pipeline design)
- Distributed systems thinking
- Cloud-native architecture
- Microservices design
- Event-driven architecture
