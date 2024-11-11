import asyncio
import uuid
import json
import logging
from configparser import ConfigParser, NoOptionError, NoSectionError

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


class ServiceA:
    def __init__(self, config: ConfigParser):
        self.event_generation_interval = self._get_config_float(config, "ServiceA", "event_generation_interval")
        self.max_concurrent_requests = self._get_config_int(config, "ServiceA", "max_concurrent_requests")
        self.host = self._get_config_str(config, "Network", "host")
        self.port = self._get_config_int(config, "Network", "port")

        self.event_queue = asyncio.Queue()
        self.semaphore = asyncio.Semaphore(self.max_concurrent_requests)

    def _get_config_float(self, config: ConfigParser, section: str, option: str) -> float:
        """Получает значение float из конфигурационного файла с обработкой ошибок."""
        try:
            return config.getfloat(section, option)
        except (NoSectionError, NoOptionError, ValueError) as e:
            logging.error(f"Ошибка конфигурации для [{section}] {option}: {e}")
            raise

    def _get_config_int(self, config: ConfigParser, section: str, option: str) -> int:
        """Получает значение int из конфигурационного файла с обработкой ошибок."""
        try:
            return config.getint(section, option)
        except (NoSectionError, NoOptionError, ValueError) as e:
            logging.error(f"Ошибка конфигурации для [{section}] {option}: {e}")
            raise

    def _get_config_str(self, config: ConfigParser, section: str, option: str) -> str:
        """Получает значение str из конфигурационного файла с обработкой ошибок."""
        try:
            return config.get(section, option)
        except (NoSectionError, NoOptionError) as e:
            logging.error(f"Ошибка конфигурации для [{section}] {option}: {e}")
            raise

    async def generate_event(self) -> None:
        """Генерация событий с уникальными UUID и добавление их в очередь."""
        while True:
            event_id = str(uuid.uuid4())
            await self.event_queue.put(event_id)
            logging.info(f"Создано событие {event_id}, всего событий в очереди: {self.event_queue.qsize()}")
            await asyncio.sleep(self.event_generation_interval)

    async def send_event(self) -> None:
        """Отправляет событие в Сервис B по сети и ожидает ответа."""
        async with self.semaphore:
            event_id = await self.event_queue.get()
            try:
                reader, writer = await asyncio.open_connection(self.host, self.port)
                message = json.dumps({"uuid": event_id})
                writer.write(message.encode())
                await writer.drain()
                logging.info(f"Событие {event_id} отправлено в Сервис B")

                try:
                    data = await asyncio.wait_for(reader.read(100), timeout=10.0)
                    response = data.decode()
                    logging.info(f"Получен ответ на событие {event_id}: {response}")
                except asyncio.TimeoutError:
                    logging.warning(f"Таймаут при ожидании ответа для события {event_id}")

                writer.close()
                await writer.wait_closed()
            except (ConnectionRefusedError, OSError) as e:
                logging.error(f"Ошибка при отправке события {event_id}: {e}")
            finally:
                self.event_queue.task_done()

    async def process_events(self) -> None:
        """Обрабатывает события из очереди, отправляя их в целевой сервис."""
        while True:
            if not self.event_queue.empty():
                asyncio.create_task(self.send_event())
            await asyncio.sleep(0.05)

    async def start(self) -> None:
        """Запускает задачи генерации и обработки событий."""
        await asyncio.gather(self.generate_event(), self.process_events())


if __name__ == "__main__":
    config = ConfigParser()
    config.read("config.ini")
    service_a = ServiceA(config)
    try:
        asyncio.run(service_a.start())
    except Exception as e:
        logging.error(f"Сервис завершен из-за ошибки: {e}")
