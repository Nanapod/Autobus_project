import yaml
import schedule
import time
from datetime import datetime

from src import Cleaner 

def Clean_db():
    logger.debug("Initializing cleaning")
#     api_config: dict = {'url': 'http://127.0.0.1:1337'}
    with open("../config/api.yaml", "r") as f:
        api_config = yaml.load(f, yaml.Loader)

    test_cleaner = Cleaner(api_config)
    test_cleaner.delete_data(datetime(2021, 1, 1, 0, 0, 0))  # удаляем все, что не последние три дня

# schedule.every(10).minutes.do(job)
# schedule.every().hour.do(job)
schedule.every().day.at("23:50").do(Clean_db)

while 1:
    schedule.run_pending()
    time.sleep(1)