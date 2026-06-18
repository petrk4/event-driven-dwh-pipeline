import uuid
from datetime import datetime
from typing import Any, Dict, List

from lib.pg import PgConnect
from pydantic import BaseModel


class CdmRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def load_cdm(self, msg):
        msg_type = msg.get("type")
        if not msg_type:
            return
        user_id = uuid.UUID(msg["user_id"])

        if msg_type == 'user_category':
            category_id = uuid.UUID(msg["category_id"])
            self.user_category_counters_insert(user_id, category_id, msg['category_name'], msg['order_cnt'])
        elif msg_type == 'user_product':
            product_id = uuid.UUID(msg["product_id"])
            self.user_product_counters_insert(user_id, product_id, msg['product_name'], msg['order_cnt'])

    def user_category_counters_insert(self,
                            user_id: uuid.UUID,
                            category_id: uuid.UUID,
                            category_name: str,
                            order_cnt: int
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO cdm.user_category_counters (user_id, category_id, category_name, order_cnt) VALUES (%s, %s, %s, %s)
                        ON CONFLICT (user_id, category_id) DO UPDATE SET
                        order_cnt = cdm.user_category_counters.order_cnt + excluded.order_cnt
                    """,
                    (
                        user_id,
                        category_id,
                        category_name,
                        order_cnt
                    )
                )

    def user_product_counters_insert(self,
                            user_id: uuid.UUID,
                            product_id: uuid.UUID,
                            product_name: str,
                            order_cnt: int
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO cdm.user_product_counters (user_id, product_id, product_name, order_cnt) VALUES (%s, %s, %s, %s)
                        ON CONFLICT (user_id, product_id) DO UPDATE SET
                        order_cnt = cdm.user_product_counters.order_cnt + excluded.order_cnt
                    """,
                    (
                        user_id,
                        product_id,
                        product_name,
                        order_cnt
                    )
                )