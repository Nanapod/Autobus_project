import requests
from datetime import datetime
from calendar import monthrange
import logging
import numpy as np


class Cleaner:
    def __init__(self, config: dict):
        self._url = config["url"]
        
    def numpy_to_bytes(self, image: np.ndarray) -> Optional[bytes]:
        try:
            message = base64.b64encode(image.tobytes())
        except Exception as e: # проверить варианты ошибок
            logger.error(f"Error {e} during image convertation")
        return message
    
    def insert_data_at(
        self,
        bus_id: str,
        door_id: str,
        start_time: datetime,
    )-> bool:
        if (datetime.now() - start_time).days < 0:
            logger.error("Incorrect isertion date")
            return False
#         timestamp = datetime(
#             np.random.randint(start_time.year, datetime.now().year), 
#             np.random.randint(start_time.month, datetime.now().month),
#             np.random.randint(start_time.day, datetime.now().day),
#             np.random.randint(start_time.hour, datetime.now().hour),
#             np.random.randint(start_time.minute, datetime.now().minute),
#             np.random.randint(start_time.second, datetime.now().second),
#         ).timestamp()
        
        timestamp = datetime(2022, 
            np.random.randint(1, 12),
            np.random.randint(1, 28),
            np.random.randint(0, 23),
            np.random.randint(0, 59),
            np.random.randint(0, 59),
        ).timestamp()
        
        start_timestamp = datetime(2022, 7, 8, 0, 0, 0).timestamp()
        image = np.random.randint(0, 255, (3, 2))
        bboxes = np.random.randint(0, 255, (2, 4))
        keypoints = np.random.randint(0, 255, (2, 2, 25))
        confidences = np.random.randint(0, 255, (2, 1))


        data = {
            "bus_id": bus_id,
            "door_id": door_id,
            "timestamp": str(timestamp),
            "start_timestamp": str(start_timestamp),
            "image": numpy_to_bytes(image),
            "bboxes": numpy_to_bytes(bboxes),
            "keypoints": numpy_to_bytes(keypoints),
            "confidences": numpy_to_bytes(confidences),
        }

        response = requests.post( 
            self._url + "/db/insert_image_data",
            data=data,
        )
        if response.status_code != 200:
            logger.error(
                "Couldn't insert data"
                f"Response: {response.status_code}, {response.reason}"
            )
        return True
    
    def get_sequence_starting_at(
        self,
        bus_id: str,
        door_id: str,
        start_timestamp: float,
    ) :
        
        body = {
            "bus_id": bus_id,
            "door_id": door_id,
            "start_timestamp": start_timestamp,
        }

        response = requests.get(
            self._url + "/db/get_sequence_at",
            params=body,
        )
        if response.status_code != 200:
            logger.error(
                f"Couldn't get sequence for {bus_id}.{door_id}"
                f"at {start_timestamp}"
                f"Response: {response.status_code}, {response.reason}"
            )

            return None
        else:
            logger.debug(
                f"Successfully retrieved sequence at {start_timestamp}"
            )
            
        return response.json()["data"]["result"]
    
    def get_data_entries(
        self,
    ) -> dict:
        response = requests.get(
            self._url + "/db/get_data_entries",
        )
        
        if response.status_code != 200:
            logger.error(
                "Couldn't get data entries"
                f"Response: {response.status_code}, {response.reason}"
            )

            return None
        else:
            logger.debug(
                "Successfully retrieved data entries"
            )
            
        return response.json()["data"]
    
    def calculate_end_date(self,
                          safe_period = 3) -> float:
        if datetime.now().day < safe_period:
            if datetime.now().month == 1:
                return datetime(datetime.now().year - 1, 12, 31 - (datetime.now().day-safe_period+1), 0, 0, 0).timestamp()
            return datetime(datetime.now().year, datetime.now().month-1, monthrange(datetime.now().year, datetime.now().month-1)[1] - (datetime.now().day-safe_period+1), 0, 0, 0).timestamp()
        return datetime(datetime.now().year, datetime.now().month, datetime.now().day-safe_period+1, 0, 0, 0).timestamp()

    def delete_data(self,
                   start: datetime,
                   safe_period = 3) -> bool:
        finish = self.calculate_end_date(safe_period)

        logger.debug("Initialize daily data cleanup"
                    f"From {start} to {finish}")

        params = {
            "start": str(start.timestamp()),
            "finish": str(finish),
        }

        response = requests.get(
            self._url + "/db/remove_image_data",
            params=params,
        )
        
        if response.status_code != 200:
            logger.error(
                "Couldn't delete data"
                f"Response: {response.status_code}, {response.reason}"
            )

            return False
        else:
            res = response.json()["data"]["result"]
            logger.debug(f"Removed {res} entries")
        return True
