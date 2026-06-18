import json
from logging import Logger
from typing import List, Dict
from datetime import datetime

from lib.kafka_connect import KafkaConsumer, KafkaProducer
from lib.redis import RedisClient
from stg_loader.repository.stg_repository import StgRepository


class StgMessageProcessor:
    def __init__(self,

                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 redis_client: RedisClient,
                 stg_repository: StgRepository,
                 batch_size: int,
                 logger: Logger,
                 ) -> None:

        self._consumer = consumer
        self._producer = producer
        self._redis = redis_client
        self._stg_repository = stg_repository
        self._logger = logger
        self._batch_size = batch_size

    # функция, которая будет вызываться по расписанию.
    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START batch_size={self._batch_size}")

        for i in range(self._batch_size):
            self._logger.info("TRY CONSUME")
            msg = self._consumer.consume()
            self._logger.info(f"MSG={msg}")

            if not msg:
                self._logger.info(f"{datetime.utcnow()}: No more messages (msg=None). STOP")
                break

            self._logger.info(f"{datetime.utcnow()}: [{i}] RECEIVED msg object_id={msg.get('object_id')}")

            try:
                order = msg['payload']
                self._logger.info(f"{datetime.utcnow()}: payload extracted object_id={msg['object_id']}")

                self._stg_repository.order_events_insert(
                    msg["object_id"],
                    msg["object_type"],
                    msg["sent_dttm"],
                    json.dumps(order)
                )

                self._logger.info(f"{datetime.utcnow()}: DB INSERT OK object_id={msg['object_id']}")

                user_id = order["user"]["id"]
                self._logger.info(f"{datetime.utcnow()}: fetching user from redis user_id={user_id}")

                user = self._redis.get(user_id)

                if not user:
                    self._logger.error(f"{datetime.utcnow()}: USER NOT FOUND in redis user_id={user_id}")
                    continue

                self._logger.info(f"{datetime.utcnow()}: USER FOUND user_id={user_id}")

                user_name = user["name"]
                user_login = user["login"]

                restaurant_id = order['restaurant']['id']
                self._logger.info(f"{datetime.utcnow()}: fetching restaurant restaurant_id={restaurant_id}")

                restaurant = self._redis.get(restaurant_id)

                if not restaurant:
                    self._logger.error(f"{datetime.utcnow()}: RESTAURANT NOT FOUND id={restaurant_id}")
                    continue

                self._logger.info(f"{datetime.utcnow()}: RESTAURANT FOUND id={restaurant_id}")

                restaurant_name = restaurant["name"]

                menu = restaurant.get("menu")
                if not menu:
                    self._logger.error(f"{datetime.utcnow()}: MENU IS EMPTY restaurant_id={restaurant_id}")
                    continue

                self._logger.info(f"{datetime.utcnow()}: MENU SIZE={len(menu)}")

                self._logger.info(f"{datetime.utcnow()}: building dst_msg object_id={msg['object_id']}")

                dst_msg = {
                    "object_id": msg["object_id"],
                    "object_type": "order",
                    "payload": {
                        "id": msg["object_id"],
                        "date": order["date"],
                        "cost": order["cost"],
                        "payment": order["payment"],
                        "status": order["final_status"],
                        "restaurant": self._format_restaurant(restaurant_id, restaurant_name),
                        "user": self._format_user(user_id, user_name, user_login),
                        "products": self._format_items(order["order_items"], restaurant)
                    }
                }
                self._logger.info(
                    f"PRODUCING TO TOPIC={self._producer.topic}"
                )
                self._logger.info(f"PRODUCING: {json.dumps(dst_msg)}")

                self._producer.produce(dst_msg)

                self._logger.info(f"{datetime.utcnow()}: PRODUCED SUCCESS object_id={msg['object_id']}")

            except Exception as e:
                self._logger.exception(
                    f"{datetime.utcnow()}: ERROR processing message object_id={msg.get('object_id')} error={e}")

        self._logger.info(f"{datetime.utcnow()}: FINISH batch")

    def _format_restaurant(self, id, name) -> Dict[str, str]:
        return {
            "id": id,
            "name": name
        }

    def _format_user(self, id, name, login) -> Dict[str, str]:
        return {
            "id": id,
            "name": name,
            "login": login
        }

    def _format_items(self, order_items, restaurant) -> List[Dict[str, str]]:
        items = []

        menu = restaurant["menu"]
        for it in order_items:
            menu_item = next(x for x in menu if x["_id"] == it["id"])
            dst_it = {
                "id": it["id"],
                "price": it["price"],
                "quantity": it["quantity"],
                "name": menu_item["name"],
                "category": menu_item["category"]
            }
            items.append(dst_it)

        return items
