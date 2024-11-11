import asyncio
import json
import random
import logging
from configparser import ConfigParser, NoOptionError, NoSectionError
from typing import Tuple

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


class ServiceB:
    def __init__(self, config: ConfigParser):
        self.host, self.port = self._get_network_config(config)
        self.processing_time_min, self.processing_time_max = self._get_processing_times(config)

    def _get_network_config(self, config: ConfigParser) -> Tuple[str, int]:
        """Получает настройки сети, включая хост и порт, с обработкой ошибок конфигурации."""
        try:
            host = config.get("Network", "host")
            port = config.getint("Network", "port")
            return host, port
        except (NoSectionError, NoOptionError, ValueError) as e:
            logging.error(f"Ошибка конфигурации сети: {e}")
            raise

    def _get_processing_times(self, config: ConfigParser) -> Tuple[int, int]:
        """Получает минимальное и максимальное время обработки с проверкой конфигурации."""
        try:
            processing_time_min = config.getint("ServiceB", "processing_time_min")
            processing_time_max = config.getint("ServiceB", "processing_time_max")
            if processing_time_min < 0 or processing_time_max < processing_time_min:
                raise ValueError("Некорректные значения времени обработки")
            return processing_time_min, processing_time_max
        except (NoSectionError, NoOptionError, ValueError) as e:
            logging.error(f"Ошибка конфигурации времени обработки: {e}")
            raise

    async def handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """Обрабатывает входящее событие от сервиса A и отправляет ответ после обработки."""
        try:
            data = await reader.read(100)
            message = data.decode()
            event = json.loads(message)
            event_id = event.get("uuid", "unknown")
            logging.info(f"Принято событие {event_id}")

            # Имитация обработки события с рандомным временем задержки
            processing_time = random.uniform(self.processing_time_min, self.processing_time_max)
            await asyncio.sleep(processing_time)

            response = json.dumps({"status": "processed", "uuid": event_id})
            writer.write(response.encode())
            await writer.drain()
            logging.info(f"Ответ отправлен на событие {event_id}")

        except json.JSONDecodeError:
            logging.error("Ошибка декодирования JSON, получены некорректные данные.")
        except Exception as e:
            logging.error(f"Ошибка при обработке события: {e}")
        finally:
            writer.close()
            await writer.wait_closed()

    async def start_server(self) -> None:
        """Запускает сервер и обрабатывает входящие подключения."""
        try:
            server = await asyncio.start_server(self.handle_connection, self.host, self.port)
            logging.info(f"Сервис B запущен на {self.host}:{self.port}")
            async with server:
                await server.serve_forever()
        except Exception as e:
            logging.error(f"Ошибка при запуске сервера: {e}")


if __name__ == "__main__":
    config = ConfigParser()
    config.read("config.ini")

    try:
        service_b = ServiceB(config)
        asyncio.run(service_b.start_server())
    except Exception as e:
        logging.error(f"Сервис завершен с ошибкой: {e}")
